//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <CodegenHelper.h>
#include <Logger.h>
#include <Base.h>

#include <llvm/Target/TargetIntrinsicInfo.h>
#include <llvm/Target/TargetOptions.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Support/TargetSelect.h>
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
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/IR/CFG.h> // to iterate over predecessors/successors easily
#include <LLVMEnvironment.h>
#include <LambdaFunction.h>
#include <FunctionRegistry.h>
#include <InstructionCountPass.h>
#include <llvm/Analysis/ValueTracking.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/Bitstream/BitCodes.h>
#include <llvm/Bitcode/BitcodeReader.h>

// llvm 10 refactored sys into Host
#if LLVM_VERSION_MAJOR > 9
#include <llvm/Support/Host.h>
#endif

#include <llvm/Support/ManagedStatic.h>

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
                llvmInitialized = true;
            }
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
                os<<err; os.flush();
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
        dictionaryKey(llvm::LLVMContext &ctx, llvm::Module *mod, const codegen::IRBuilder &builder, llvm::Value *val,
                      python::Type keyType, python::Type valType) {
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
                val = builder.CreateSExt(val, llvm::Type::getInt64Ty(ctx)); // extend to 64 bit integer
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
            valargs.push_back(val);
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
            Writer.writeModule(module, ShouldPreserveUseListOrder, Index,
                    GenerateHash,
                               ModHash);
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
            WriteBitcodeToFile(module, os);
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
    }
}