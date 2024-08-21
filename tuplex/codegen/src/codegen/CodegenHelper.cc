//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <codegen/CodegenHelper.h>
#include <Logger.h>
#include <Base.h>

#include <llvm/Target/TargetIntrinsicInfo.h>
#include <llvm/Target/TargetOptions.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Support/TargetSelect.h>
#if LLVM_VERSION_MAJOR < 14
#include <llvm/Support/TargetRegistry.h>
#else
#include <llvm/MC/TargetRegistry.h>
#endif
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/IRReader/IRReader.h>
#if LLVM_VERSION_MAJOR < 17
#include <llvm/MC/SubtargetFeature.h>
#endif
#include <llvm/IR/CFG.h> // to iterate over predecessors/successors easily
#include <codegen/LLVMEnvironment.h>
#include <codegen/LambdaFunction.h>
#include <codegen/FunctionRegistry.h>
#include <codegen/InstructionCountPass.h>
#include <llvm/Analysis/ValueTracking.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#if LLVM_VERSION_MAJOR >= 9
#include <llvm/Bitstream/BitCodes.h>
#else
#include <llvm/Bitcode/BitCodes.h>
#endif
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/IR/Constant.h>
#include <llvm/Support/CodeGen.h>

// llvm 10 refactored sys into Host
#if LLVM_VERSION_MAJOR > 9
#include <llvm/Support/Host.h>
#endif

#include <llvm/Support/ManagedStatic.h>

#ifdef USE_YYJSON_INSTEAD
#include <yyjson.h>
#endif

namespace tuplex {
    namespace codegen {
        // global var because often only references are passed around.
        // CompilePolicy DEFAULT_COMPILE_POLICY = CompilePolicy();

        static bool llvmInitialized = false;
        void initLLVM() {
            if(!llvmInitialized) {
                // LLVM Initialization is required because else
                llvm::InitializeNativeTarget();
                llvm::InitializeNativeTargetAsmPrinter();
                llvm::InitializeNativeTargetAsmParser();

//                llvm::InitializeAllTargetInfos();
//                llvm::InitializeAllTargets();
//                llvm::InitializeAllTargetMCs();
//                llvm::InitializeAllAsmParsers();
//                llvm::InitializeAllAsmPrinters();

                llvmInitialized = true;
            }
        }

        bool is_llvm_initialized() {
            return llvmInitialized;
        }

        void shutdownLLVM() {
            llvm::llvm_shutdown();
            llvmInitialized = false;
        }

        // IRBuilder definitions
        IRBuilder::IRBuilder(llvm::BasicBlock *bb) {
            _llvm_builder = std::make_unique<llvm::IRBuilder<>>(bb);
        }

        IRBuilder::IRBuilder(llvm::IRBuilder<> &llvm_builder) {
            _llvm_builder = std::make_unique<llvm::IRBuilder<>>(llvm_builder.getContext());
            _llvm_builder->SetInsertPoint(llvm_builder.GetInsertBlock(), llvm_builder.GetInsertPoint());
        }

        IRBuilder::IRBuilder(const IRBuilder &other) : _llvm_builder(nullptr) {
            if(other._llvm_builder) {
                // cf. https://reviews.llvm.org/D74693
                auto& ctx = other._llvm_builder->getContext();
                const llvm::DILocation *DL = nullptr;
                _llvm_builder.reset(new llvm::IRBuilder<>(ctx));
                llvm::Instruction* InsertBefore = nullptr;
                auto InsertBB = other._llvm_builder->GetInsertBlock();
                if(InsertBB && !InsertBB->empty()) {
                    auto& inst = *InsertBB->getFirstInsertionPt();
                    InsertBefore = &inst;
                }
                if(InsertBefore)
                    _llvm_builder->SetInsertPoint(InsertBefore);
                else if(InsertBB)
                    _llvm_builder->SetInsertPoint(InsertBB);
                _llvm_builder->SetCurrentDebugLocation(DL);
            }
        }

        IRBuilder::IRBuilder(llvm::LLVMContext& ctx) {
            _llvm_builder = std::make_unique<llvm::IRBuilder<>>(ctx);
        }

        IRBuilder::~IRBuilder() {
            if(_llvm_builder)
                _llvm_builder->ClearInsertionPoint();
        }

        IRBuilder IRBuilder::firstBlockBuilder(bool insertAtEnd) const {
            // create new IRBuilder for first block

            // empty builder? I.e., no basicblock?
            if(!_llvm_builder)
                return IRBuilder();

            assert(_llvm_builder->GetInsertBlock());
            assert(_llvm_builder->GetInsertBlock()->getParent());

            // function shouldn't be empty when this function here is called!
            assert(!_llvm_builder->GetInsertBlock()->getParent()->empty());

            // create new builder to avoid memory issues
            auto b = std::make_unique<llvm::IRBuilder<>>(_llvm_builder->GetInsertBlock());

            // special case: no instructions yet present?
            auto func = b->GetInsertBlock()->getParent();
            auto is_empty = b->GetInsertBlock()->getParent()->empty();
            //auto num_blocks = func->getBasicBlockList().size();
            auto firstBlock = &func->getEntryBlock();

            if(firstBlock->empty())
                return IRBuilder(firstBlock);

            if(!insertAtEnd) {
                auto it = firstBlock->getFirstInsertionPt();
                auto inst_name = it->getName().str();
                return IRBuilder(it);
            } else {
                // create inserter unless it's a branch instruction
                auto it = firstBlock->getFirstInsertionPt();
                auto lastit = it;
                while(it != firstBlock->end() && !llvm::isa<llvm::BranchInst>(*it)) {
                    lastit = it;
                    ++it;
                }
                return IRBuilder(lastit);
            }
        }

        void IRBuilder::initFromIterator(llvm::BasicBlock::iterator it) {
            if(it->getParent()->empty())
                _llvm_builder = std::make_unique<llvm::IRBuilder<>>(it->getParent());
            else {
                auto& ctx = it->getParent()->getContext();
                _llvm_builder = std::make_unique<llvm::IRBuilder<>>(ctx);

                // instruction & basic block
                auto bb = it->getParent();

                auto pt = llvm::IRBuilderBase::InsertPoint(bb, it);
                _llvm_builder->restoreIP(pt);
            }
        }

        IRBuilder::IRBuilder(const llvm::IRBuilder<> &llvm_builder) : IRBuilder(llvm_builder.GetInsertPoint()) {}

        IRBuilder::IRBuilder(llvm::BasicBlock::iterator it) {
            initFromIterator(it);
        }

        // Clang doesn't work well with ASAN, disable here container overflow.
        ATTRIBUTE_NO_SANITIZE_ADDRESS std::string getLLVMFeatureStr() {
            using namespace llvm;
            SubtargetFeatures Features;

            // If user asked for the 'native' CPU, we need to autodetect feat3ures.
            // This is necessary for x86 where the CPU might not support all the
            // features the autodetected CPU name lists in the target. For example,
            // not all Sandybridge processors support AVX.
            StringMap<bool> HostFeatures;
            if (sys::getHostCPUFeatures(HostFeatures))
                for (auto &F : HostFeatures)
                    Features.AddFeature(F.first(), F.second);

            return Features.getString();
        }

        llvm::TargetMachine* getOrCreateTargetMachine() {
            using namespace llvm;

            initLLVM();

            // we need SSE4.2 features. So create target machine that has these
            auto& logger = Logger::instance().logger("JITCompiler");

            auto triple = sys::getProcessTriple();//sys::getDefaultTargetTriple();
            std::string error;
            auto theTarget = llvm::TargetRegistry::lookupTarget(triple, error);
            std::string CPUStr = sys::getHostCPUName().str();

            //logger.info("using LLVM for target triple: " + triple + " target: " + theTarget->getName() + " CPU: " + CPUStr);

            // use default target options
            TargetOptions to;


            // need to tune this, so compilation gets fast enough

//            return theTarget->createTargetMachine(triple,
//                                                   CPUStr,
//                                                   getLLVMFeatureStr(),
//                                                   to,
//                                                   Reloc::PIC_,
//                                                   CodeModel::Large,
//                                                   CodeGenOpt::None);

            // confer https://subscription.packtpub.com/book/application_development/9781785285981/1/ch01lvl1sec14/converting-llvm-bitcode-to-target-machine-assembly
            // on how to tune this...

            return theTarget->createTargetMachine(triple,
                                                  CPUStr,
                                                  getLLVMFeatureStr(),
                                                  to,
                                                  Reloc::PIC_,
                                                  CodeModel::Large,
                                                  CodeGenOpt::Aggressive);
        }

        std::string moduleToAssembly(std::shared_ptr<llvm::Module> module) {
            llvm::SmallString<2048> asm_string;
            llvm::raw_svector_ostream asm_sstream{asm_string};

            llvm::legacy::PassManager pass_manager;
            auto target_machine = tuplex::codegen::getOrCreateTargetMachine();

            target_machine->Options.MCOptions.AsmVerbose = true;
#if LLVM_VERSION_MAJOR == 9
            target_machine->addPassesToEmitFile(pass_manager, asm_sstream, nullptr,
                                                llvm::TargetMachine::CGFT_AssemblyFile);
#elif LLVM_VERSION_MAJOR < 9
            target_machine->addPassesToEmitFile(pass_manager, asm_sstream,
                                                llvm::TargetMachine::CGFT_AssemblyFile);
#else
            target_machine->addPassesToEmitFile(pass_manager, asm_sstream, nullptr,
                                                llvm::CodeGenFileType::CGFT_AssemblyFile);
#endif

            pass_manager.run(*module);
            target_machine->Options.MCOptions.AsmVerbose = false;

            return asm_sstream.str().str();
        }

        std::unique_ptr<llvm::Module> stringToModule(llvm::LLVMContext& context, const std::string& llvmIR) {
            using namespace llvm;
            // first parse IR. It would be also an alternative to directly the LLVM Module from the ModuleBuilder class,
            // however if something went wrong there, memory errors would occur. Better is to first transform to a string
            // and then parse it because LLVM will validate the IR on the way.

            SMDiagnostic err; // create an SMDiagnostic instance

            std::unique_ptr<MemoryBuffer> buff = MemoryBuffer::getMemBuffer(llvmIR);
            std::unique_ptr<llvm::Module> mod = parseIR(buff->getMemBufferRef(), err, context); // use err directly

            // check if any errors occurred during module parsing
            if (nullptr == mod.get()) {
                // print errors
                Logger::instance().logger("LLVM Backend").error("could not compile module:\n>>>>>>>>>>>>>>>>>\n"
                                                                + core::withLineNumbers(llvmIR)
                                                                + "\n<<<<<<<<<<<<<<<<<");
                Logger::instance().logger("LLVM Backend").error(
                        "line " + std::to_string(err.getLineNo()) + ": " + err.getMessage().str());
                return nullptr;
            }


            // run verify pass on module and print out any errors, before attempting to compile it
            std::string moduleErrors;
            llvm::raw_string_ostream os(moduleErrors);
            if (verifyModule(*mod, &os)) {
                os.flush();
                Logger::instance().logger("LLVM Backend").error("could not verify module:\n>>>>>>>>>>>>>>>>>\n"
                                                                + core::withLineNumbers(llvmIR)
                                                                + "\n<<<<<<<<<<<<<<<<<");
                Logger::instance().logger("LLVM Backend").error(moduleErrors);
                return nullptr;
            }

            return std::move(mod);
        }

        std::unique_ptr<llvm::Module> bitCodeToModule(llvm::LLVMContext& context, void* buf, size_t bufSize) {
            using namespace llvm;

            // Note: check llvm11 for parallel codegen https://llvm.org/doxygen/ParallelCG_8cpp_source.html
            auto res = parseBitcodeFile(llvm::MemoryBufferRef(llvm::StringRef((char*)buf, bufSize), "<module>"), context);

            // check if any errors occurred during module parsing
            if (!res) {
                // print errors
                auto err = res.takeError();
                std::string err_msg;
                raw_string_ostream os(err_msg);
#if LLVM_VERSION_MAJOR >= 9
		os<<err;
#else
		err_msg = toString(std::move(err));
#endif

		os.flush();
                Logger::instance().logger("LLVM Backend").error("could not parse module from bitcode");
                Logger::instance().logger("LLVM Backend").error(err_msg);
                return nullptr;
            }

            std::unique_ptr<llvm::Module> mod = std::move(res.get()); // use err directly

#ifndef NDEBUG
            // run verify pass on module and print out any errors, before attempting to compile it
            std::string moduleErrors;
            llvm::raw_string_ostream os(moduleErrors);
            if (verifyModule(*mod, &os)) {
                os.flush();
                Logger::instance().logger("LLVM Backend").error("could not verify module from bitcode");
                Logger::instance().logger("LLVM Backend").error(moduleErrors);
                Logger::instance().logger("LLVM Backend").error(core::withLineNumbers(moduleToString(*mod)));
                return nullptr;
            }
#endif

            return mod;
        }

        llvm::Value* upCast(const codegen::IRBuilder& builder, llvm::Value *val, llvm::Type *destType) {
            // check if types are the same, then just return val
            if (val->getType() == destType)
                return val;
            else {
                // check if dest type is integer
                if(destType->isIntegerTy()) {
                    // check that dest type is larger than val's type
                    if(val->getType()->getIntegerBitWidth() > destType->getIntegerBitWidth())
                        throw std::runtime_error("destination types bitwidth is smaller than the current value ones, can't upcast");
                    return builder.CreateZExt(val, destType);
                } else if(destType->isFloatTy() || destType->isDoubleTy()) {
                    // check if current val is integer or float
                    if(val->getType()->isIntegerTy()) {
                        return builder.CreateSIToFP(val, destType);
                    } else {
                        return builder.CreateFPExt(val, destType);
                    }
                } else {
                    throw std::runtime_error("can't upcast llvm type " + llvmTypeToStr(destType));
                }
            }
        }

        llvm::Value *
        dictionaryKey(llvm::LLVMContext &ctx, llvm::Module *mod, const codegen::IRBuilder &builder, llvm::Value *key_value,
                      python::Type keyType, python::Type valType) {

            // optimized types? deoptimize!
            if(keyType.isOptimizedType() || valType.isOptimizedType()) {
                // special case: optimized value type
                if(valType.isConstantValued()) {
                    return dictionaryKey(ctx, mod, builder, constantValuedTypeToLLVM(builder, keyType).val, deoptimizedType(keyType),
                                         deoptimizedType(valType));
                }
                return dictionaryKey(ctx, mod, builder, key_value, deoptimizedType(keyType), deoptimizedType(valType));
            }


            // get key to string
            auto strFormat_func = strFormat_prototype(ctx, mod);
            std::vector<llvm::Value *> valargs;

            // format for key
            std::string typesstr;
            std::string replacestr;
            if(keyType == python::Type::STRING) {
                typesstr = "s";
                replacestr = "s_{}";
            }
            else if (python::Type::BOOLEAN == keyType) {
                typesstr = "b";
                replacestr = "b_{}";
                key_value = builder.CreateSExt(key_value, llvm::Type::getInt64Ty(ctx)); // extend to 64 bit integer
            } else if (python::Type::I64 == keyType) {
                typesstr = "d";
                replacestr = "i_{}";
            } else if (python::Type::F64 == keyType) {
                typesstr = "f";
                replacestr = "f_{}";
            } else {
                throw std::runtime_error("objects of type " + keyType.desc() + " are not supported as dictionary keys");
            }

            // value type encoding
            if(valType == python::Type::STRING) replacestr[1] = 's';
            else if(valType == python::Type::BOOLEAN) replacestr[1] = 'b';
            else if(valType == python::Type::I64) replacestr[1] = 'i';
            else if(valType == python::Type::F64) replacestr[1] = 'f';
            else throw std::runtime_error("objects of type " + valType.desc() + " are not supported as dictionary values");

            auto replaceptr = builder.CreatePointerCast(builder.CreateGlobalStringPtr(replacestr),
                                                        llvm::Type::getInt8PtrTy(ctx, 0));
            auto sizeVar = builder.CreateAlloca(llvm::Type::getInt64Ty(ctx), 0, nullptr);
            auto typesptr = builder.CreatePointerCast(builder.CreateGlobalStringPtr(typesstr),
                                                      llvm::Type::getInt8PtrTy(ctx, 0));
            valargs.push_back(replaceptr);
            valargs.push_back(sizeVar);
            valargs.push_back(typesptr);
            valargs.push_back(key_value);

            return builder.CreateCall(strFormat_func, valargs);
        }

        // TODO: Do we need to use lfb to add checks?
        SerializableValue
        dictionaryKeyCast(llvm::LLVMContext &ctx, llvm::Module* mod,
                          const codegen::IRBuilder& builder, llvm::Value *val, python::Type keyType) {
            // type chars
            auto s_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 's'));
            auto b_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 'b'));
            auto i_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 'i'));
            auto f_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 'f'));

            auto typechar = builder.CreateLoad(builder.getInt8Ty(), val);
            auto keystr = builder.MovePtrByBytes(val, llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 2)));
            auto keylen = builder.CreateCall(strlen_prototype(ctx, mod), {keystr});
            if(keyType == python::Type::STRING) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, s_char));
                return SerializableValue(keystr, builder.CreateAdd(keylen, llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 1))));
            } else if (keyType == python::Type::BOOLEAN) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, b_char));
                auto value = builder.CreateAlloca(llvm::Type::getInt8Ty(ctx), 0, nullptr);
                auto strBegin = keystr;
                auto strEnd = builder.MovePtrByBytes(strBegin, keylen);
                auto resCode = builder.CreateCall(fastatob_prototype(ctx, mod), {strBegin, strEnd, value});
                auto cond = builder.CreateICmpNE(resCode, llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(ctx),
                                                                                          llvm::APInt(32,
                                                                                                      ecToI32(ExceptionCode::SUCCESS))));
//                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);
                return SerializableValue(builder.CreateZExtOrTrunc(builder.CreateLoad(llvm::Type::getInt8Ty(ctx), value), builder.getInt64Ty()),
                        llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                llvm::APInt(64, sizeof(int64_t))));
            } else if (keyType == python::Type::I64) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, i_char));
                auto value = builder.CreateAlloca(llvm::Type::getInt64Ty(ctx), 0, nullptr);
                auto strBegin = keystr;
                auto strEnd = builder.MovePtrByBytes(strBegin, keylen);
                auto resCode = builder.CreateCall(fastatoi_prototype(ctx, mod), {strBegin, strEnd, value});
                auto cond = builder.CreateICmpNE(resCode, llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(ctx),
                                                                                          llvm::APInt(32,
                                                                                                      ecToI32(ExceptionCode::SUCCESS))));
//                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);
                return SerializableValue(builder.CreateLoad(llvm::Type::getInt64Ty(ctx), value),
                                         llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                                                         llvm::APInt(64, sizeof(int64_t))));
            } else if (keyType == python::Type::F64) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, f_char));
                auto value = builder.CreateAlloca(llvm::Type::getDoubleTy(ctx), 0, nullptr);
                auto strBegin = keystr;
                auto strEnd = builder.MovePtrByBytes(strBegin, keylen);
                auto resCode = builder.CreateCall(fastatod_prototype(ctx, mod), {strBegin, strEnd, value});
                auto cond = builder.CreateICmpNE(resCode, llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(ctx),
                                                                                          llvm::APInt(32,
                                                                                                      ecToI32(ExceptionCode::SUCCESS))));
//                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);
                return SerializableValue(builder.CreateLoad(llvm::Type::getDoubleTy(ctx), value),
                                         llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                                                         llvm::APInt(64, sizeof(double))));
            } else {
                throw std::runtime_error("objects of type " + keyType.desc() + " are not supported as dictionary keys");
            }
        }


        bool verifyFunction(llvm::Function* func, std::string* out) {
            std::string funcErrors = "";
            llvm::raw_string_ostream os(funcErrors);

            if(llvm::verifyFunction(*func, &os)) {
                os.flush(); // important, else funcErrors may be an empty string

                if(out)
                    *out = funcErrors;
                return false;
            }
            return true;
        }

        bool verifyModule(llvm::Module& mod, std::string* out) {
            std::string modErrors = "";
            llvm::raw_string_ostream os(modErrors);

            if(llvm::verifyModule(mod, &os)) {
                os.flush();

                if(out)
                    *out = modErrors;
                return false;
            }

            for(auto& f : mod.functions()) {
                if(!verifyFunction(&f, out))
                    return false;
            }

            return true;
        }

        size_t successorBlockCount(llvm::BasicBlock* block) {
            if(!block)
                return 0;
            auto term = block->getTerminator();
            if(!term)
                return 0;

            return term->getNumSuccessors();
        }

        std::string moduleStats(const std::string& llvmIR, bool include_detailed_counts) {
            llvm::LLVMContext context;
            auto mod = stringToModule(context, llvmIR);

            char name[] = "inst count";
            InstructionCounts inst_count(*name);
            inst_count.runOnModule(*mod);
            return inst_count.formattedStats(include_detailed_counts);
        }


        /// If generating a bc file on darwin, we have to emit a
        /// header and trailer to make it compatible with the system archiver.  To do
        /// this we emit the following header, and then emit a trailer that pads the
        /// file out to be a multiple of 16 bytes.
        ///
        /// struct bc_header {
        ///   uint32_t Magic;         // 0x0B17C0DE
        ///   uint32_t Version;       // Version, currently always 0.
        ///   uint32_t BitcodeOffset; // Offset to traditional bitcode file.
        ///   uint32_t BitcodeSize;   // Size of traditional bitcode file.
        ///   uint32_t CPUType;       // CPU specifier.
        ///   ... potentially more later ...
        /// };

        static void writeInt32ToBuffer(uint32_t Value, llvm::SmallVectorImpl<char> &Buffer,
                                       uint32_t &Position) {
            llvm::support::endian::write32le(&Buffer[Position], Value);
            Position += 4;
        }

        static void emitDarwinBCHeaderAndTrailer(llvm::SmallVectorImpl<char> &Buffer,
                                                 const llvm::Triple &TT) {
            using namespace llvm;
            unsigned CPUType = ~0U;

            // Match x86_64-*, i[3-9]86-*, powerpc-*, powerpc64-*, arm-*, thumb-*,
            // armv[0-9]-*, thumbv[0-9]-*, armv5te-*, or armv6t2-*. The CPUType is a magic
            // number from /usr/include/mach/machine.h.  It is ok to reproduce the
            // specific constants here because they are implicitly part of the Darwin ABI.
            enum {
                DARWIN_CPU_ARCH_ABI64      = 0x01000000,
                DARWIN_CPU_TYPE_X86        = 7,
                DARWIN_CPU_TYPE_ARM        = 12,
                DARWIN_CPU_TYPE_POWERPC    = 18
            };

            Triple::ArchType Arch = TT.getArch();
            if (Arch == Triple::x86_64)
                CPUType = DARWIN_CPU_TYPE_X86 | DARWIN_CPU_ARCH_ABI64;
            else if (Arch == Triple::x86)
                CPUType = DARWIN_CPU_TYPE_X86;
            else if (Arch == Triple::ppc)
                CPUType = DARWIN_CPU_TYPE_POWERPC;
            else if (Arch == Triple::ppc64)
                CPUType = DARWIN_CPU_TYPE_POWERPC | DARWIN_CPU_ARCH_ABI64;
            else if (Arch == Triple::arm || Arch == Triple::thumb)
                CPUType = DARWIN_CPU_TYPE_ARM;

            // Traditional Bitcode starts after header.
            assert(Buffer.size() >= BWH_HeaderSize &&
                   "Expected header size to be reserved");
            unsigned BCOffset = BWH_HeaderSize;
            unsigned BCSize = Buffer.size() - BWH_HeaderSize;

            // Write the magic and version.
            unsigned Position = 0;
            writeInt32ToBuffer(0x0B17C0DE, Buffer, Position);
            writeInt32ToBuffer(0, Buffer, Position); // Version.
            writeInt32ToBuffer(BCOffset, Buffer, Position);
            writeInt32ToBuffer(BCSize, Buffer, Position);
            writeInt32ToBuffer(CPUType, Buffer, Position);

            // If the file is not a multiple of 16 bytes, insert dummy padding.
            while (Buffer.size() & 15ul)
                Buffer.push_back(0);
        }

        bool validateModule(const llvm::Module& mod) {
            // check if module is ok, if not print out issues & throw exception

            // run verify pass on module and print out any errors, before attempting to compile it
            std::string moduleErrors = "";
            llvm::raw_string_ostream os(moduleErrors);
            if(llvm::verifyModule(mod, &os)) {
                std::stringstream errStream;
                os.flush();
                auto llvmIR = moduleToString(mod);

                errStream<<"could not verify module:\n>>>>>>>>>>>>>>>>>\n"<<core::withLineNumbers(llvmIR)<<"\n<<<<<<<<<<<<<<<<<\n";
                errStream<<moduleErrors;

                throw std::runtime_error("failed to verify module (code: " + std::to_string(llvm::inconvertibleErrorCode().value()) + "), details: " + errStream.str());
            }
            return true;
        }

        uint8_t* moduleToBitCode(const llvm::Module& module, size_t* bufSize) {
            using namespace llvm;

            // in debug mode validate module first before writing it out
#ifndef NDEBUG
            validateModule(module);
#endif

            SmallVector<char, 0> Buffer;
            Buffer.reserve(256 * 1014); // 256K
            auto ShouldPreserveUseListOrder = false;
            const ModuleSummaryIndex *Index=nullptr;
            bool GenerateHash=false;
            ModuleHash *ModHash=nullptr;

            Triple TT(module.getTargetTriple());
            if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
                Buffer.insert(Buffer.begin(), BWH_HeaderSize, 0);

            BitcodeWriter Writer(Buffer);
#if LLVM_VERSION_MAJOR < 9
            Writer.writeModule(&module, ShouldPreserveUseListOrder, Index,
                    GenerateHash,
                               ModHash);
#else
            Writer.writeModule(module, ShouldPreserveUseListOrder, Index,
                    GenerateHash,
                               ModHash);
#endif
            Writer.writeSymtab();
            Writer.writeStrtab();

            if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
                emitDarwinBCHeaderAndTrailer(Buffer, TT);

            // alloc buffer & memcpy module
            auto bc_size = Buffer.size();
            auto buf = new uint8_t[bc_size];
            memcpy(buf, (char*)&Buffer.front(), bc_size);
            if(bufSize)
                *bufSize = bc_size;

            return buf;
        }

        std::string moduleToBitCodeString(const llvm::Module& module) {
            using namespace llvm;

            // in debug mode validate module first before writing it out
#ifndef NDEBUG
            validateModule(module);
#endif

            // iterate over functions
            {
                std::stringstream ss;
                for(auto& func : module) {
                    ss<<"function: "<<func.getName().str()<<std::endl;

                    // type
                    auto type = func.getType();
                    ss<<"type: "<<type<<std::endl;
                }

                Logger::instance().logger("LLVM Backend").debug(ss.str());
            }


            // cf. https://github.com/llvm-mirror/llvm/blob/master/tools/verify-uselistorder/verify-uselistorder.cpp#L179
            // to check that everything is mappable?

            // simple conversion using LLVM builtins...
            std::string out_str;
            llvm::raw_string_ostream os(out_str);
#if LLVM_VERSION_MAJOR < 9
            WriteBitcodeToFile(&module, os);
#else
            WriteBitcodeToFile(module, os);
#endif
            os.flush();
            return out_str;

            // could also use direct code & tune buffer sizes better...
            // SmallVector<char, 0> Buffer;
            // Buffer.reserve(256 * 1014); // 256K
            // auto ShouldPreserveUseListOrder = false;
            // const ModuleSummaryIndex *Index=nullptr;
            // bool GenerateHash=false;
            // ModuleHash *ModHash=nullptr;

            // Triple TT(module.getTargetTriple());
            // if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
            //     Buffer.insert(Buffer.begin(), BWH_HeaderSize, 0);

            // BitcodeWriter Writer(Buffer);
            // Writer.writeModule(module, ShouldPreserveUseListOrder, Index,
            //                    GenerateHash,
            //                    ModHash);
            // Writer.writeSymtab();
            // Writer.writeStrtab();

            // if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
            //     emitDarwinBCHeaderAndTrailer(Buffer, TT);

            // // copy buffer to module
            // auto bc_size = Buffer.size();
            // std::string bc_str;
            // bc_str.reserve(bc_size);
            // bc_str.assign((char*)&Buffer.front(), bc_size);
            // assert(bc_str.length() == bc_size);
            // return bc_str;
        }

        inline llvm::Type* i8ptrType(llvm::LLVMContext& ctx) {
            return llvm::Type::getInt8PtrTy(ctx, 0);
        }
        inline llvm::Type* i32ptrType(llvm::LLVMContext& ctx) {
            return llvm::Type::getInt32PtrTy(ctx, 0);
        }
        inline llvm::Type* i64ptrType(llvm::LLVMContext& ctx) {
            return llvm::Type::getInt64PtrTy(ctx, 0);
        }
        inline llvm::Value* i1Const(llvm::LLVMContext& ctx, bool value) {
            return llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(1, value));
        }
        inline llvm::Value* i8Const(llvm::LLVMContext& ctx, int8_t value) {
            return llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(8, value));
        }
        inline llvm::Value* i32Const(llvm::LLVMContext& ctx, int32_t value) {
            return llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(32, value));
        }
        inline llvm::Value* i64Const(llvm::LLVMContext& ctx, int64_t value) {
            return llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(64, value));
        }

        llvm::Value* stringCompare(const IRBuilder& builder, llvm::Value *ptr, const std::string &str,
                                   bool include_zero=false) {

            auto& ctx = builder.getContext();

            // how many bytes to compare?
            int numBytes = include_zero ? str.length() + 1 : str.length();

            assert(ptr->getType() == i8ptrType(ctx));

            // compare in 64bit (8 bytes) blocks if possible, else smaller blocks.
            llvm::Value *cond = i1Const(ctx, true);
            int pos = 0;
            while (numBytes >= 8) {

                uint64_t str_const = 0;

                // create str const by extracting string data
                str_const = *((int64_t *) (str.c_str() + pos));

                auto val = builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.MovePtrByBytes(ptr,  pos), i64ptrType(ctx)));

                auto comp = builder.CreateICmpEQ(val, i64Const(ctx, str_const));
                cond = builder.CreateAnd(cond, comp);
                numBytes -= 8;
                pos += 8;
            }

            // 32 bit compare?
            if(numBytes >= 4) {
                uint32_t str_const = 0;

                // create str const by extracting string data
                str_const = *((uint32_t *) (str.c_str() + pos));
                auto val = builder.CreateLoad(builder.getInt32Ty(), builder.CreatePointerCast(builder.MovePtrByBytes(ptr,  pos), i32ptrType(ctx)));
                auto comp = builder.CreateICmpEQ(val, i32Const(ctx, str_const));
                cond = builder.CreateAnd(cond, comp);

                numBytes -= 4;
                pos += 4;
            }

            // only 0, 1, 2, 3 bytes left.
            // do 8 bit compares
            for (int i = 0; i < numBytes; ++i) {
                auto val = builder.CreateLoad(builder.getInt8Ty(), builder.MovePtrByBytes(ptr,  pos));
                auto comp = builder.CreateICmpEQ(val, i8Const(ctx, str.c_str()[pos]));
                cond = builder.CreateAnd(cond, comp);
                pos++;
            }

            return cond;
        }

        llvm::Value *
        NormalCaseCheck::codegenForCell(llvm::IRBuilder<> &builder, llvm::Value *cell_value, llvm::Value *cell_size) {
            auto& ctx = builder.getContext();
            auto false_const = llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(1, false));
            auto& logger = Logger::instance().logger("codegen");
            switch(type) {
                case CheckType::CHECK_CONSTANT: {
                    assert(_constantType.isConstantValued());
                    auto value = _constantType.constant();
                    auto reduced_type = simplifyConstantOption(_constantType);
                    auto elementType = reduced_type.isConstantValued() ? reduced_type.underlying() : reduced_type;

                    // compare stored string value directly
                    if(elementType == python::Type::STRING) {
                        // zero terminated cell!
                        auto size_match = builder.CreateICmpEQ(cell_size, i64Const(ctx, value.length() + 1));
                        auto content_match = stringCompare(builder, cell_value, value);
                        //     assert(Cond2->getType()->isIntOrIntVectorTy(1));
                        //     return CreateSelect(Cond1, Cond2,
                        //                         ConstantInt::getNullValue(Cond2->getType()), Name);
                        // return builder.CreateLogicalAnd(size_match, content_match); LLVM11+
                        return builder.CreateSelect(size_match, content_match, llvm::ConstantInt::getNullValue(content_match->getType()));
                    } else if(elementType == python::Type::I64) {
                        // create string match against var...

                        // else, parse check and compare then!

                    } else if(elementType == python::Type::F64) {
                        // create string match against var...

                        // else, parse check and compare then!

                    } else {
                        // always tell check is not passed, because not supported.
                        logger.warn("unsupported constant type " + _constantType.desc() + "/--> " + elementType.desc() + " for normal-case check, always failing check.");
                        return false_const;
                    }

                    // then perform parse & compare if necessary

                }
                default:
                    throw std::runtime_error("unsupported check encountered, don't know how to generate code for it");
            }
            return nullptr;
        }

        void annotateModuleWithInstructionPrint(llvm::Module& mod, bool print_values) {

            auto printf_func = codegen::printf_prototype(mod.getContext(), &mod);

            // lookup table for names (before modifying module!)
            std::unordered_map<llvm::Instruction*, std::string> names;
            for(auto& func : mod) {
                for(auto& bb : func) {

                    for(auto& inst : bb) {
                        std::string inst_name;
                        llvm::raw_string_ostream os(inst_name);
                        inst.print(os);
                        os.flush();

                        // save instruction name in map
                        auto inst_ptr = &inst;
                        names[inst_ptr] = inst_name;

                    }
                }
            }

            // go over all functions in mod
            for(auto& func : mod) {
                // go over blocks
                size_t num_blocks = 0;
                size_t num_instructions = 0;
                for(auto& bb : func) {

                    auto printed_enter = false;

                    for(auto& inst : bb) {
                        // only call printf IFF not a branching instruction and not a ret instruction
                        auto inst_ptr = &inst;

                        // inst not found in names? -> skip!
                        if(names.end() == names.find(inst_ptr))
                            continue;

                        auto inst_name = names.at(inst_ptr);
                        if(!llvm::isa<llvm::BranchInst>(inst_ptr) && !llvm::isa<llvm::ReturnInst>(inst_ptr) && !llvm::isa<llvm::PHINode>(inst_ptr)) {
                            llvm::IRBuilder<> builder(inst_ptr);
                            llvm::Value *sConst = builder.CreateGlobalStringPtr(inst_name);

                            // print enter instruction
                            if(!printed_enter) {
                                llvm::Value* str = builder.CreateGlobalStringPtr("enter basic block " + bb.getName().str() + " ::\n");
                                builder.CreateCall(printf_func, {str});
                                printed_enter = true;
                            }

                            // value trace format
                            // bb= : %19 = load i64, i64* %exceptionCode : %19 = 42

                            if(print_values) {

                                llvm::Value* value_to_print = nullptr;
                                std::string format = "bb=" + bb.getName().str() + " : " + inst_name;

                                if(!inst_ptr->getNextNode()) {
                                    // nothing to do, else print value as well.
                                } else {
                                    builder.SetInsertPoint(inst_ptr->getNextNode());

                                    auto inst_number = splitToArray(inst_name, '=').front();
                                    trim(inst_number);

                                    if(inst_ptr->hasValueHandle()) {
                                        // check what type of value it is and adjust printing accordingly
                                        if(inst.getType() == builder.getInt8Ty()) {
                                            static_assert(sizeof(int32_t) == 4);
                                            value_to_print = builder.CreateZExtOrTrunc(inst_ptr, builder.getInt32Ty());
                                            format += " : [i8] " + inst_number + " = %d";
                                        } else if(inst.getType() == builder.getInt16Ty()) {
                                            static_assert(sizeof(int32_t) == 4);
                                            value_to_print = builder.CreateZExtOrTrunc(inst_ptr, builder.getInt32Ty());
                                            format += " : [i16] " + inst_number + " = %d";
                                        } else if(inst.getType() == builder.getInt32Ty()) {
                                            value_to_print = inst_ptr;
                                            format += " : [i32] " + inst_number + " = %d";
                                        } else if(inst.getType() == builder.getInt64Ty()) {
                                            value_to_print = inst_ptr;
                                            format += " : [i64] " + inst_number + " = %" PRId64;
                                        } else if(inst.getType()->isPointerTy()) {
                                            value_to_print = inst_ptr;
                                            format += " : [ptr] " + inst_number + " = %p";
                                        }
                                    }
                                }

                                // call func
                                llvm::Value *sFormat = builder.CreateGlobalStringPtr(format + "\n");
                                std::vector<llvm::Value*> llvm_args{sFormat};
                                if(value_to_print)
                                    llvm_args.push_back(value_to_print);
                                builder.CreateCall(printf_func, llvm_args);
                            } else {
                                // Trace format:
                                llvm::Value *sFormat = builder.CreateGlobalStringPtr("  %s\n");
                                builder.CreateCall(printf_func, {sFormat, sConst});
                            }

                            num_instructions++;
                        }
                    }

                    num_blocks++;
                }
            }
        }


        std::vector<uint8_t> compileToObjectFile(llvm::Module& mod,
                                                 const std::string& target_triple,
                                                 const std::string& cpu) {
            std::string error;
            auto target = llvm::TargetRegistry::lookupTarget(target_triple, error);

            // lookup target and throw exception else
            if (!target) {
                throw std::runtime_error("could not find target " + target_triple + ", details: " + error);
            }

            std::string CPU = "generic";
            std::string Features = "";

            if(Features.empty()) {
                // lookup features?
            }

            llvm::TargetOptions opt;
#if LLVM_VERSION_MAJOR == 9
            auto RM = llvm::Optional<llvm::Reloc::Model>();
#else
            auto RM = std::optional<llvm::Reloc::Model>(llvm::Reloc::Model());
#endif
            // use position independent code
            RM = llvm::Reloc::PIC_;
            auto TargetMachine = target->createTargetMachine(target_triple, CPU, Features, opt, RM);

            if(!TargetMachine)
                throw std::runtime_error("failed to create target machine for CPU=" + CPU + ", features="=Features);

            // check: https://llvm.org/docs/tutorial/MyFirstLanguageFrontend/LangImpl08.html
            mod.setDataLayout(TargetMachine->createDataLayout());
            mod.setTargetTriple(target_triple);

            llvm::legacy::PassManager pass;
#if LLVM_VERSION_MAJOR == 9
            auto FileType = llvm::LLVMTargetMachine::CGFT_ObjectFile;
#else
            auto FileType = llvm::CGFT_ObjectFile;
#endif
            llvm::SmallVector<char, 0> buffer;
            llvm::raw_svector_ostream dest(buffer);
            if (TargetMachine->addPassesToEmitFile(pass, dest, nullptr, FileType)) {
                throw std::runtime_error("TargetMachine can't emit a file of this type");
            }

            pass.run(mod);

            return std::vector<uint8_t>(buffer.begin(), buffer.end());
        }

        std::string compileEnvironmentAsJsonString() {
            return compileEnvironmentAsJson().dump();
        }

        nlohmann::json compileEnvironmentAsJson() {
            // target triple, CPU arch, CPU features

            // make sure to init llvm first, else target detect won't work.
            initLLVM();

            nlohmann::json j;

            std::string target_triple = llvm::sys::getProcessTriple();

            // unknown in target triple? => replace with generic
            if(target_triple.find("unknown") != std::string::npos)
                target_triple = llvm::sys::getDefaultTargetTriple();

            std::string cpu = llvm::sys::getHostCPUName().str();
            std::string features = getLLVMFeatureStr();

            j["llvmVersion"] = LLVM_VERSION_STRING;
            j["targetTriple"] = target_triple;
            j["cpu"] = cpu;
#if LLVM_VERSION_MAJOR == 9
            j["cpuCores"] = llvm::sys::getHostNumPhysicalCores();
#else
            j["cpuCores"] = llvm::get_physical_cores();
#endif
            j["cpuFeatures"] = features;

            // get data layout
            llvm::TargetOptions opt;
#if LLVM_VERSION_MAJOR == 9
            auto RM = llvm::Optional<llvm::Reloc::Model>();
#else
            auto RM = std::optional<llvm::Reloc::Model>(llvm::Reloc::Model());
#endif
            // use position independent code
            RM = llvm::Reloc::PIC_;
            std::string error;
            auto target = llvm::TargetRegistry::lookupTarget(target_triple, error);

            // lookup target and throw exception else
            if (!target) {
                throw std::runtime_error("could not find target " + target_triple + ", details: " + error);
            }
            auto TargetMachine = target->createTargetMachine(target_triple, cpu, features, opt, RM);

            if(!TargetMachine)
                throw std::runtime_error("failed to create target machine for CPU=" + cpu + ", features="=features);

            // check: https://llvm.org/docs/tutorial/MyFirstLanguageFrontend/LangImpl08.html
            j["dataLayout"] = TargetMachine->createDataLayout().getStringRepresentation();

            return j;
        }

        void codegen_debug_printf(const IRBuilder& builder, const std::string& message) {
#ifndef NDEBUG
            auto printf_func = printf_prototype(builder.getContext(), builder.GetInsertBlock()->getModule());
            llvm::Value *sConst = builder.CreateGlobalStringPtr(message);
            llvm::Value *sFormat = builder.CreateGlobalStringPtr("%s\n");
            builder.CreateCall(printf_func, {sFormat, sConst});
#endif
        }

#ifdef USE_YYJSON_INSTEAD
        // yyjson uses a doc to store a couple values.
        // These structures are detailed in https://github.com/ibireme/yyjson/blob/master/doc/DataStructure.md.
        // Practically this means, that when replacing cjson_obj with yyjson, an indirection struct holding a pointer to
        // both the doc, and the current object needs to be used.
        // LLVM may optimize parts of this away.
        // In addition, yyjson memory management needs to be intercepted when calling init/reset stage.
        llvm::Type* get_or_create_yyjson_shim_type(llvm::LLVMContext& ctx) {
            using namespace llvm;
            std::vector<llvm::Type*> member_types(2, i8ptrType(ctx)); // first is doc, second current obj
            auto stype = llvm::StructType::get(ctx, member_types);
            stype->setName("yyjson");
            return stype;
        }

        llvm::Type* get_or_create_yyjson_shim_type(const IRBuilder& builder) {
            return get_or_create_yyjson_shim_type(builder.getContext());
        }

        llvm::Value* get_yyjson_doc(const IRBuilder& builder, llvm::Value* cjson_obj) {
            return builder.CreateStructLoadOrExtract(get_or_create_yyjson_shim_type(builder), cjson_obj, 0);
        }

        llvm::Value* get_yyjson_mut_obj(const IRBuilder& builder, llvm::Value* cjson_obj) {
            return builder.CreateStructLoadOrExtract(get_or_create_yyjson_shim_type(builder), cjson_obj, 1);
        }

        void set_yyjson_mut_obj(const IRBuilder& builder, llvm::Value* cjson_obj, llvm::Value* yyjson_obj) {
            builder.CreateStructStore(get_or_create_yyjson_shim_type(builder), cjson_obj, 1, yyjson_obj);
        }

        void set_yyjson_mut_doc(const IRBuilder& builder, llvm::Value* cjson_obj, llvm::Value* yyjson_doc) {
            builder.CreateStructStore(get_or_create_yyjson_shim_type(builder), cjson_obj, 0, yyjson_doc);
        }

        llvm::Value* yy_key_string(const IRBuilder& builder, llvm::Value* yy_doc, llvm::Value* str) {
            // can safely use yyjson_mut_str here, because pointers backing strings will be read-only in Tuplex.
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();
            auto func = getOrInsertFunction(mod, "yyjson_mut_str", i8ptrType(ctx), i8ptrType(ctx), i8ptrType(ctx));

            return builder.CreateCall(func, {yy_doc, str});
        }
#endif


        llvm::Value* call_cjson_is_null_object(const IRBuilder& builder, llvm::Value* cjson_obj) {
            auto& ctx = builder.getContext();
#ifdef USE_YYJSON_INSTEAD
            auto null_pointer = llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(get_or_create_yyjson_shim_type(ctx)->getPointerTo()));
            return builder.CreateICmpEQ(cjson_obj, null_pointer);
#else
          return builder.CreateICmpEQ(cjson_obj, llvm::ConstantPointerNull::get(
                  static_cast<llvm::PointerType *>(i8ptrType(ctx))));
#endif
        }

        llvm::Value* call_cjson_getitem(const IRBuilder& builder, llvm::Value* cjson_obj, llvm::Value* key, llvm::Value** out_item_found) {
            assert(cjson_obj);
            assert(key);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();
#ifdef USE_YYJSON_INSTEAD
            auto func = getOrInsertFunction(mod, "yyjson_mut_obj_get", i8ptrType(ctx), i8ptrType(ctx), i8ptrType(ctx));

            auto yyjson_obj = get_yyjson_mut_obj(builder, cjson_obj);

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_getitem");

            auto yy_doc = get_yyjson_doc(builder, cjson_obj);
            auto yy_ret_item = builder.CreateCall(func, {yyjson_obj, key});

            // alloc and then return object
            auto ctor_builder = builder.firstBlockBuilder(false); // insert at beginning.
            auto llvm_type = get_or_create_yyjson_shim_type(builder);
            auto yy_ret_val = ctor_builder.CreateAlloca(llvm_type, 0, nullptr, "yy_retval");

            // output whether item is found.
            auto null_i8ptr = llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(i8ptrType(ctx)));
            if(out_item_found) {
                *out_item_found = builder.CreateICmpNE(yyjson_obj, null_i8ptr);
            }

            set_yyjson_mut_doc(builder, yy_ret_val, yy_doc);
            set_yyjson_mut_obj(builder, yy_ret_val, yy_ret_item);
            return builder.CreateLoad(llvm_type, yy_ret_val);
#else
            auto func = getOrInsertFunction(mod, "cJSON_GetObjectItemCaseSensitive", llvm::Type::getInt8PtrTy(ctx, 0),
                                            llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt8PtrTy(ctx, 0));

            auto ans = builder.CreateCall(func, {cjson_obj, key});

            auto null_i8ptr = llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(i8ptrType(ctx)));
            if(out_item_found) {
                *out_item_found = builder.CreateICmpNE(ans, null_i8ptr);
            }

            return ans;
#endif
        }

        llvm::Value* call_cjson_object_set_item(const IRBuilder& builder, llvm::Value* cjson_obj, llvm::Value* key, llvm::Value* cjson_item) {
            // basically cjson_obj[key] = cjson_item
            // return cjson_obj.

            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);

            auto& ctx = mod->getContext();
            auto ptr_type = i8ptrType(ctx);

#ifdef USE_YYJSON_INSTEAD
            // note: argument for key is not char* but instead a key, which must be created via yyjson_mut_str

            // also note that str is not copied for yyjson_mut_str -> we can steal string b.c. in tuplex lifetime is per row
            auto yy_doc = get_yyjson_doc(builder, cjson_obj);
            auto yy_key = yy_key_string(builder, yy_doc, key);

            auto func = getOrInsertFunction(mod, "yyjson_mut_obj_put", ctypeToLLVM<bool>(ctx), i8ptrType(ctx),
                                            i8ptrType(ctx), i8ptrType(ctx));
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);
            auto yy_item = get_yyjson_mut_obj(builder, cjson_item);

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_setitem");

            builder.CreateCall(func, {yy_obj, yy_key, yy_item});
            return cjson_obj;
#else
            auto func = getOrInsertFunction(mod, "cJSON_AddItemToObject", ptr_type, ptr_type, ptr_type, ptr_type);
            return builder.CreateCall(func, {cjson_obj, key, cjson_item});
#endif
        }


        llvm::Value* call_cjson_isnumber(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_is_num", ctypeToLLVM<bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_isnumber");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt1Ty(ctx));
#else

            auto func = getOrInsertFunction(mod, "cJSON_IsNumber", ctypeToLLVM<cJSON_AS4CPP_bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {cjson_obj}), llvm::Type::getInt1Ty(ctx));
#endif
        }

        llvm::Value* call_cjson_isnull(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_is_null", ctypeToLLVM<bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_isnull");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt1Ty(ctx));
#else
            auto func = getOrInsertFunction(mod, "cJSON_IsNull", ctypeToLLVM<cJSON_AS4CPP_bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {cjson_obj}), llvm::Type::getInt1Ty(ctx));
#endif
        }

        llvm::Value* call_cjson_isobject(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_is_obj", ctypeToLLVM<bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_isobject");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt1Ty(ctx));
#else
            auto func = getOrInsertFunction(mod, "cJSON_IsObject", ctypeToLLVM<cJSON_AS4CPP_bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {cjson_obj}), llvm::Type::getInt1Ty(ctx));
#endif
        }

        llvm::Value* call_cjson_isarray(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_is_arr", ctypeToLLVM<bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_isarray");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt1Ty(ctx));
#else
            auto func = getOrInsertFunction(mod, "cJSON_IsArray", ctypeToLLVM<cJSON_AS4CPP_bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {cjson_obj}), llvm::Type::getInt1Ty(ctx));
#endif
        }

        llvm::Value* call_cjson_getarraysize(const IRBuilder& builder, llvm::Value* cjson_array) {
            assert(cjson_array);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_array);

            auto func = getOrInsertFunction(mod, "yyjson_mut_arr_size", ctypeToLLVM<size_t>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_getarraysize");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt64Ty(ctx));
#else
            auto func = getOrInsertFunction(mod, "cJSON_GetArraySize", ctypeToLLVM<int>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {cjson_array}), llvm::Type::getInt64Ty(ctx));
#endif
        }

        SerializableValue get_cjson_array_item(const IRBuilder& builder, llvm::Value* cjson_array, llvm::Value* idx) {
            assert(cjson_array);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_doc = get_yyjson_doc(builder, cjson_array);
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_array);

            auto func = getOrInsertFunction(mod, "yyjson_mut_arr_get", i8ptrType(ctx), i8ptrType(ctx), ctypeToLLVM<size_t>(ctx));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_get_array_item");

            auto yy_ret_item = builder.CreateCall(func, {yy_obj, idx});

            // alloc new obj struct and fill with doc & returned object

            auto ctor_builder = builder.firstBlockBuilder(false); // insert at beginning.
            auto llvm_type = get_or_create_yyjson_shim_type(builder);
            auto yy_ret_val = ctor_builder.CreateAlloca(llvm_type, 0, nullptr, "yy_retval");

            set_yyjson_mut_doc(builder, yy_ret_val, yy_doc); // <-- this may lead to modificaitons if subdict is returned, this should be correct. dict.copy() creates deep copy of elements.
            set_yyjson_mut_obj(builder, yy_ret_val, yy_ret_item);
            return {builder.CreateLoad(llvm_type, yy_ret_val), nullptr, nullptr};
#else
            auto func = getOrInsertFunction(mod, "cJSON_GetArrayItem", llvm::Type::getInt8PtrTy(ctx, 0),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0), ctypeToLLVM<int>(ctx));

            auto item = builder.CreateCall(func, {cjson_array, builder.CreateZExtOrTrunc(idx,
                                                                                                              ctypeToLLVM<int>(ctx))});
            return {item, nullptr, nullptr};
#endif
        }

        llvm::Value* call_cjson_isstring(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_is_str", ctypeToLLVM<bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_isstring");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt1Ty(ctx));
#else
            auto func = getOrInsertFunction(mod, "cJSON_IsString", ctypeToLLVM<cJSON_AS4CPP_bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {cjson_obj}), llvm::Type::getInt1Ty(ctx));
#endif
        }

        [[maybe_unused]] llvm::Value* call_cjson_is_list_of_generic_dicts(const IRBuilder& builder, llvm::Value* cjson_obj) {
            // this calls a runtime function (linked in JITCompiler)
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_is_array_of_objects", ctypeToLLVM<bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_is_list_of_generic_dicts");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt1Ty(ctx));
#else
            auto func = getOrInsertFunction(mod, "cJSON_IsArrayOfObjects", ctypeToLLVM<cJSON_AS4CPP_bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {cjson_obj}), llvm::Type::getInt1Ty(ctx));
#endif
        }

        llvm::Value* get_cjson_as_integer(const IRBuilder& builder, llvm::Value* cjson_obj) {
            // in yyjson, can directly return integer
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_get_sint", ctypeToLLVM<int64_t>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_get_integer");

            return builder.CreateCall(func, {yy_obj});
#else
            auto float_val = get_cjson_as_float(builder, cjson_obj);
            return builder.CreateFPToSI(float_val, llvm::Type::getInt64Ty(builder.getContext()));
#endif
        }

        llvm::Value* get_cjson_as_boolean(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_get_bool", ctypeToLLVM<bool>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_get_boolean");

            return builder.CreateZExtOrTrunc(builder.CreateCall(func, {yy_obj}), llvm::Type::getInt64Ty(ctx));
#else
            auto func = getOrInsertFunction(mod, "cJSON_IsTrue", llvm::Type::getInt64Ty(ctx), llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateCall(func, {cjson_obj});
#endif
        }

        llvm::Value* get_cjson_as_float(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_get_real", ctypeToLLVM<double>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_get_float");

            return builder.CreateCall(func, {yy_obj});
#else
            auto func = getOrInsertFunction(mod, "cJSON_GetNumberValue", llvm::Type::getDoubleTy(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateCall(func, {cjson_obj});
#endif
        }

        SerializableValue get_cjson_as_string_value(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto func = getOrInsertFunction(mod, "yyjson_mut_get_str", ctypeToLLVM<const char*>(ctx),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_get_string");

            auto str_pointer = builder.CreateCall(func, {yy_obj});
#else
            auto func = getOrInsertFunction(mod, "cJSON_GetStringValue", llvm::Type::getInt8PtrTy(ctx, 0),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            auto str_pointer = builder.CreateCall(func, {cjson_obj});
#endif
            auto str_len = builder.CreateCall(strlen_prototype(ctx, mod), {str_pointer});
            auto str_size = builder.CreateAdd(str_len, llvm::ConstantInt::get(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 1)));
            return {str_pointer, str_size};
        }

        [[maybe_unused]] SerializableValue serialize_cjson_as_runtime_str(const IRBuilder& builder, llvm::Value* cjson_obj) {
            assert(cjson_obj);
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();
#ifdef USE_YYJSON_INSTEAD

            auto func = getOrInsertFunction(mod, "yyjson_print_to_runtime_str", llvm::Type::getInt8PtrTy(ctx, 0),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt64PtrTy(ctx, 0));

            auto first_builder = builder.firstBlockBuilder(false);
            auto str_size_var = first_builder.CreateAlloca(llvm::Type::getInt64Ty(ctx), 0, nullptr);

            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_as_runtime_str");

            auto str = builder.CreateCall(func, {yy_obj, str_size_var});
            auto str_size = builder.CreateLoad(llvm::Type::getInt64Ty(ctx), str_size_var);
            return {str, str_size};
#else

            auto func = getOrInsertFunction(mod, "cJSON_PrintUnformatted", llvm::Type::getInt8PtrTy(ctx, 0),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            auto str_pointer = builder.CreateCall(func, {cjson_obj});

            auto str_len = builder.CreateCall(strlen_prototype(ctx, mod), {str_pointer});
            auto str_size = builder.CreateAdd(str_len, llvm::ConstantInt::get(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 1)));
            return {str_pointer, str_size};
#endif
        }

        llvm::Value* call_cjson_create_empty(const IRBuilder& builder) {
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);

            auto& ctx = mod->getContext();
#ifdef USE_YYJSON_INSTEAD

            // this is a custom function, which sets up also the runtime allocator to be used within yyjson.
            auto func_doc_init = getOrInsertFunction(mod, "yyjson_init_doc", i8ptrType(ctx));
            auto func_doc_get_root = getOrInsertFunction(mod, "yyjson_mut_doc_get_root", i8ptrType(ctx), i8ptrType(ctx));
            auto yy_doc = builder.CreateCall(func_doc_init);
            auto yy_root_item = builder.CreateCall(func_doc_get_root, {yy_doc});

            auto llvm_type = get_or_create_yyjson_shim_type(builder);
            auto ctor_builder = builder.firstBlockBuilder(false); // insert at beginning.
            auto yy_ret_val = ctor_builder.CreateAlloca(llvm_type, 0, nullptr, "yy_retval");

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_create_empty");

            set_yyjson_mut_doc(builder, yy_ret_val, yy_doc); // <-- this may lead to modificaitons if subdict is returned, this should be correct. dict.copy() creates deep copy of elements.
            set_yyjson_mut_obj(builder, yy_ret_val, yy_root_item);
            return builder.CreateLoad(llvm_type, yy_ret_val);
#else
            auto func = getOrInsertFunction(mod, "cJSON_CreateObject", llvm::Type::getInt8PtrTy(ctx, 0));
            return builder.CreateCall(func, {});
#endif
        }

        extern llvm::Value* call_simdjson_to_cjson_object(const IRBuilder& builder, llvm::Value* json_item) {
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);

            auto& ctx = mod->getContext();

#ifdef USE_YYJSON_INSTEAD
            auto func = getOrInsertFunction(mod, "JsonItem_to_yyjson_mut_doc", llvm::Type::getInt8PtrTy(ctx, 0),
                                           (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            auto func_doc_get_root = getOrInsertFunction(mod, "yyjson_mut_doc_get_root", i8ptrType(ctx), i8ptrType(ctx));
            auto yy_doc = builder.CreateCall(func, {json_item});
            auto yy_root_item = builder.CreateCall(func_doc_get_root, {yy_doc});

            auto ctor_builder = builder.firstBlockBuilder(false); // insert at beginning.
            auto llvm_type = get_or_create_yyjson_shim_type(builder);
            auto yy_ret_val = ctor_builder.CreateAlloca(llvm_type, 0, nullptr, "yy_retval");

            // codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_simdjson_to_yyjson");

            set_yyjson_mut_doc(builder, yy_ret_val, yy_doc); // <-- this may lead to modificaitons if subdict is returned, this should be correct. dict.copy() creates deep copy of elements.
            set_yyjson_mut_obj(builder, yy_ret_val, yy_root_item);
            return builder.CreateLoad(llvm_type, yy_ret_val);
#else

            auto func = getOrInsertFunction(mod, "JsonItem_to_cJSON", llvm::Type::getInt8PtrTy(ctx, 0),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateCall(func, {json_item});
#endif
        }

        extern llvm::Value* call_cjson_type_as_str(const IRBuilder& builder, llvm::Value* cjson_obj) {
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);

            auto& ctx = mod->getContext();
#ifdef USE_YYJSON_INSTEAD

            auto func = getOrInsertFunction(mod, "yyjson_type_as_runtime_str", llvm::Type::getInt8PtrTy(ctx, 0),
                                            (llvm::Type*)llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt64PtrTy(ctx, 0));

            auto first_builder = builder.firstBlockBuilder(false);
            auto str_size_var = first_builder.CreateAlloca(llvm::Type::getInt64Ty(ctx), 0, nullptr);

            auto yy_obj = get_yyjson_mut_obj(builder, cjson_obj);

            auto str = builder.CreateCall(func, {yy_obj, str_size_var});
            // auto str_size = builder.CreateLoad(llvm::Type::getInt64Ty(ctx), str_size_var);
            return str;
#else
            auto func = getOrInsertFunction(mod, "cJSON_TypeAsString", llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt8PtrTy(ctx, 0));

            return builder.CreateCall(func, {cjson_obj});
#endif
        }

        extern llvm::Value* call_cjson_parse(const IRBuilder& builder, llvm::Value* str_ptr) {
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();
#ifdef USE_YYJSON_INSTEAD

            auto ctor_builder = builder.firstBlockBuilder(false); // insert at beginning.
            auto llvm_type = get_or_create_yyjson_shim_type(builder);
            auto yy_ret_val = ctor_builder.CreateAlloca(llvm_type, 0, nullptr, "yy_retval");

            auto func_doc_get_root = getOrInsertFunction(mod, "yyjson_mut_doc_get_root", i8ptrType(ctx), i8ptrType(ctx));

            auto func_parse = getOrInsertFunction(mod, "yyjson_mut_parse", i8ptrType(ctx), i8ptrType(ctx), llvm::Type::getInt64Ty(ctx));

            auto func_strlen = strlen_prototype(ctx, mod);
            auto str_size = builder.CreateAdd(builder.CreateCall(func_strlen, {str_ptr}), llvm::ConstantInt::get(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 1)));

            codegen_debug_printf(builder, std::string(__FILE__) + ":" + std::to_string(__LINE__) + " call_cjson_yyjson_parse");

            auto yy_doc = builder.CreateCall(func_parse, {str_ptr, str_size});
            auto yy_root_object = builder.CreateCall(func_doc_get_root, {yy_doc});

            set_yyjson_mut_doc(builder, yy_ret_val, yy_doc); // <-- this may lead to modificaitons if subdict is returned, this should be correct. dict.copy() creates deep copy of elements.
            set_yyjson_mut_obj(builder, yy_ret_val, yy_root_object);
            return builder.CreateLoad(llvm_type, yy_ret_val);
#else

            auto func = getOrInsertFunction(mod, "cJSON_Parse", llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt8PtrTy(ctx, 0));
            return builder.CreateCall(func, {str_ptr});
#endif
        }

        SerializableValue call_cjson_to_string(const IRBuilder& builder, llvm::Value* cjson_obj) {
#ifdef USE_YYJSON_INSTEAD
            return serialize_cjson_as_runtime_str(builder, cjson_obj);
            //throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " not yet implemented in yyjson mode.");
#endif
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();
            auto f_print = getOrInsertFunction(mod, "cJSON_PrintUnformatted", llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt8PtrTy(ctx, 0));
            auto f_strlen = strlen_prototype(ctx, mod);

            SerializableValue v;
            v.val = builder.CreateCall(f_print, {cjson_obj});
            v.size = builder.CreateAdd(llvm::ConstantInt::get(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 1)), builder.CreateCall(f_strlen, {v.val}));
            return v;
        }

        llvm::Value* calc_bitmap_size_in_64bit_blocks(const IRBuilder& builder, llvm::Value* num_elements) {
            // implement here the same logic as
            // calc_bitmap_size_in_64bit_blocks in Serializer.h provides

            assert(num_elements && num_elements->getType()->isIntegerTy() && num_elements->getType()->getIntegerBitWidth() == 64);

            auto& ctx = builder.getContext();

            auto base = llvm::ConstantInt::get(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 64));
            // T k = x / base;
            auto k = builder.CreateSDiv(num_elements, base);

            //  if(k * base >= x)
            //            return k * base;
            //        else
            //            return (k + 1) * base;
            auto k_base = builder.CreateMul(k, base);
            auto lgtx = builder.CreateICmpSGE(k_base, num_elements);

            auto k_base_plus_one = builder.CreateAdd(k_base, base);
            auto ceil_to_multiple_base = builder.CreateSelect(lgtx, k_base, k_base_plus_one);
            llvm::Value* num_bitmap_fields = builder.CreateSDiv(ceil_to_multiple_base, base);

            return builder.CreateMul(num_bitmap_fields, llvm::ConstantInt::get(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, sizeof(uint64_t))));

        }

        llvm::Value* call_cjson_from_value(const IRBuilder& builder, const SerializableValue& value, const python::Type& type) {
#ifdef USE_YYJSON_INSTEAD
            throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " not yet implemented in yyjson mode.");
#endif
            // converts value to cJSON value.
            using namespace llvm;

            assert(builder.GetInsertBlock());
            auto mod = builder.GetInsertBlock()->getParent()->getParent();
            assert(mod);
            auto& ctx = mod->getContext();
            auto i8ptrtype = llvm::Type::getInt8PtrTy(ctx, 0);

            if(type.isOptionType()) {
                // nested -> if null, convert to Json null
                // else, recursively call.
                auto func = builder.GetInsertBlock()->getParent();

                auto bbIsNull = BasicBlock::Create(ctx, "cjson_encode_is_null", func);
                auto bbIsNotNull = BasicBlock::Create(ctx, "cjson_encode_is_not_null", func);
                auto bbDone = BasicBlock::Create(ctx, "cjson_encode_done", func);

                builder.CreateCondBr(value.is_null, bbIsNull, bbIsNotNull);
                builder.SetInsertPoint(bbIsNull);
                auto null_value = call_cjson_from_value(builder, {}, python::Type::NULLVALUE);
                builder.CreateBr(bbDone);

                builder.SetInsertPoint(bbIsNotNull);
                auto non_null_value = call_cjson_from_value(builder, value, type.withoutOption());
                builder.CreateBr(bbDone);

                auto phi = builder.CreatePHI(i8ptrtype, 2);
                phi->addIncoming(null_value, bbIsNull);
                phi->addIncoming(non_null_value, bbIsNotNull);
                return phi;
            }


            // regular, primitive types.
            if(python::Type::NULLVALUE == type) {
                auto func = getOrInsertFunction(mod, "cJSON_CreateNull", i8ptrtype);
                return builder.CreateCall(func);
            }

            if(python::Type::BOOLEAN == type) {
                auto func = getOrInsertFunction(mod, "cJSON_CreateBool", i8ptrtype, ctypeToLLVM<cJSON_bool>(ctx));
                auto v = builder.CreateZExtOrTrunc(value.val, ctypeToLLVM<cJSON_bool>(ctx));
                return builder.CreateCall(func, {v});
            }

            if(python::Type::I64 == type) {
                // this is bad in cJSON. bad API mixing double and integer.
                auto func = getOrInsertFunction(mod, "cJSON_CreateNumber", i8ptrtype, ctypeToLLVM<double>(ctx));
                auto v = builder.CreateSIToFP(value.val, ctypeToLLVM<double>(ctx));
                return builder.CreateCall(func, {v});
            }

            if(python::Type::F64 == type) {
                // this is bad in cJSON. bad API mixing double and integer.
                auto func = getOrInsertFunction(mod, "cJSON_CreateNumber", i8ptrtype, ctypeToLLVM<double>(ctx));
                return builder.CreateCall(func, {value.val});
            }

            if(python::Type::STRING == type) {
                auto func = getOrInsertFunction(mod, "cJSON_CreateString", i8ptrtype, i8ptrtype);
                return builder.CreateCall(func, {value.val});
            }

            std::stringstream ss;
            ss<<__FILE__<<":"<<__LINE__<<" Unsupported value of type "<<type.desc()<<" can't get converted to cJSON value.";
            throw std::runtime_error(ss.str());
        }

        SerializableValue get_value_from_cjson(const IRBuilder& builder, llvm::Value* cjson_obj, const python::Type& type) {
            // use primitive conversion functions directly.

            auto& ctx = builder.getContext();

            // option: check if is null or not

            auto i64_8bytes = llvm::ConstantInt::get(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 8));

            if(type == python::Type::NULLVALUE) {
                return SerializableValue(nullptr, nullptr, llvm::ConstantInt::get(llvm::Type::getInt1Ty(ctx), llvm::APInt(1, 1)));
            }

            if(type == python::Type::BOOLEAN) {
                return SerializableValue(get_cjson_as_boolean(builder, cjson_obj), i64_8bytes);
            }

            if(type == python::Type::I64) {
                return SerializableValue(get_cjson_as_integer(builder, cjson_obj), i64_8bytes);
            }

            if(type == python::Type::STRING) {
                return get_cjson_as_string_value(builder, cjson_obj);
            }

            std::stringstream ss;
            ss<<__FILE__<<":"<<__LINE__<<" Unsupported value of type "<<type.desc()<<" can't retrieve value from cJSON value.";
            throw std::runtime_error(ss.str());
        }
    }
}