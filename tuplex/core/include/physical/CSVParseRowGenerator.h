//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CSVPARSEROWGENERATOR_H
#define TUPLEX_CSVPARSEROWGENERATOR_H


#include <LLVMEnvironment.h>
#include <TypeSystem.h>
#include <CodegenHelper.h>
#include <ExceptionCodes.h>
#include <Base.h>
#include <Utils.h>

// define SSE42 only for x86_64. Tuplex requires at least cpu with sse42 features.
#ifdef __x86_64
#define SSE42_MODE
#endif

namespace tuplex {

    namespace codegen {
        struct CSVCellDesc {
            python::Type type;
            bool willBeSerialized;
        };

        inline llvm::Type* v16qi_type(llvm::LLVMContext& ctx) {
#if LLVM_VERSION_MAJOR < 10
            return llvm::VectorType::get(llvm::Type::getInt8Ty(ctx), 16u);
#else
            return llvm::VectorType::get(llvm::Type::getInt8Ty(ctx), 16u, false);
#endif
        }


        /*!
         * this class is a helper class for the CSVParserGenerator class. In detail it generates the code to parse a single row.
         * this function returns the status, linestart, lineend as well as all values that could be deserialized.
         */
        class CSVParseRowGenerator {
        private:
            char _quotechar;
            char _delimiter;
            char _escapechar;
            std::vector<std::string> _null_values; // strings to be interpreted as null values
            std::vector<CSVCellDesc> _cellDescs;

            // LLVM variables/internals for code generation
            LLVMEnvironment *_env;

            llvm::Function *_func;
            llvm::Type *_resultType;

            // generated internals:
            llvm::Value *_inputPtr;
            llvm::Value *_endPtr;
            llvm::Value *_resultPtr; //! holds the result to be obtained


            void storeParseInfo(IRBuilder &builder, llvm::Value *lineStart, llvm::Value *lineEnd,
                                llvm::Value *numParsedBytes);

            void storeValue(IRBuilder &builder, int column, llvm::Value *val, llvm::Value *size,
                            llvm::Value *isnull);


            llvm::Value *_currentPtrVar; // this is a ptr
            llvm::Value *_currentLookAheadVar; // this is a char

            llvm::Value *_cellBeginVar; // this is a ptr
            llvm::Value *_cellEndVar; // this is a ptr

            llvm::Value *_lineBeginVar; // this is a ptr
            llvm::Value *_lineEndVar; // this is a ptr


            // serialization + co vars
            llvm::Value *_cellNoVar; // i32*
            llvm::Value *_ecVar; // i32*

            llvm::Value *_storeIndexVar; //i32
            llvm::Value *_storedCellBeginsVar; // i8* array
            llvm::Value *_storedCellEndsVar; // i8* array

            // in SSE4.2 mode this a vector mask, else it's the fallback function
            llvm::Value *_quotedSpanner;
            llvm::Value *_unquotedSpanner;

            size_t numCells() const { return _cellDescs.size(); }

            void createFunction(bool internalOnly);

            /*!
             * sets currentLookAheadVar based on currentPtr and endPtr.
             * @param builder
             */
            void updateLookAhead(IRBuilder &builder);

            inline llvm::Value *lookahead(IRBuilder &builder) {
                return builder.CreateLoad(builder.getInt8Ty(), _currentLookAheadVar);
            }

            /*!
             * safely get current char. If over endptr, return 0
             * @param builder
             * @return
             */
            inline llvm::Value *currentChar(IRBuilder &builder) {
                auto ptr = currentPtr(builder);
                auto i8ptr_type = llvm::Type::getInt8PtrTy(_env->getContext(), 0);
                // assert(ptr->getType() == i8ptr_type);
                // assert(_endPtr->getType() == i8ptr_type);
                assert(ptr->getType()->isPointerTy());
                auto ans =  builder.CreateSelect(builder.CreateICmpUGE(ptr, _endPtr), _env->i8Const(_escapechar),
                                            builder.CreateLoad(builder.getInt8Ty(), ptr));
                // _env->printValue(builder, ans, "cur char is=");
                return ans;
            }

            llvm::Value *clampWithStartPtr(IRBuilder &builder, llvm::Value *ptr) {
                assert(_inputPtr);
                assert(_inputPtr->getType() == llvm::Type::getInt8PtrTy(_env->getContext(), 0));
                assert(ptr->getType() == llvm::Type::getInt8PtrTy(_env->getContext(), 0));
                auto cond = builder.CreateICmpULT(builder.CreatePtrToInt(ptr, _env->i64Type()),
                                                  builder.CreatePtrToInt(_inputPtr, _env->i64Type()));
                auto endval = builder.CreateSelect(cond, _inputPtr, ptr);
                return endval;
            }

            inline llvm::Value *clampWithEndPtr(IRBuilder &builder, llvm::Value *ptr) {
                assert(_endPtr);
                assert(_endPtr->getType() == llvm::Type::getInt8PtrTy(_env->getContext(), 0));
                assert(ptr->getType() == llvm::Type::getInt8PtrTy(_env->getContext(), 0));
                auto cond = builder.CreateICmpULT(builder.CreatePtrToInt(ptr, _env->i64Type()),
                                                  builder.CreatePtrToInt(_endPtr, _env->i64Type()));
                auto endval = builder.CreateSelect(cond, ptr, _endPtr);
                return endval;
            }

            inline void consume(IRBuilder &builder, llvm::Value *howManyChars) {

                assert(howManyChars->getType() == _env->i32Type());

                // change ptr
                auto ptr = builder.CreateLoad(_env->i8ptrType(), _currentPtrVar);


                // clamp with endptr
                auto clamped_ptr = clampWithEndPtr(builder, builder.MovePtrByBytes(ptr, howManyChars));

                // _env->printValue(builder, howManyChars, "consuming num bytes=");
                // _env->printValue(builder, ptr, "current ptr=");
                // _env->printValue(builder, clamped_ptr, "new ptr=");

                builder.CreateStore(clamped_ptr, _currentPtrVar);

                // important also to update look ahead!
                updateLookAhead(builder);
            }

            inline void consume(IRBuilder &builder, int32_t howMany) {
                consume(builder, _env->i32Const(howMany));
            }

            void saveCurrentCell(IRBuilder &builder);


            inline void saveCellBegin(IRBuilder &builder, int32_t offset = 0) {
                builder.CreateStore(builder.MovePtrByBytes(builder.CreateLoad(_env->i8ptrType(), _currentPtrVar), offset),
                                    _cellBeginVar);
            }

            inline void saveCellEnd(IRBuilder &builder, int32_t offset = 0) {
                auto ptr = builder.MovePtrByBytes(builder.CreateLoad(_env->i8ptrType(), _currentPtrVar),
                                                  offset);
                auto clamped_ptr = clampWithEndPtr(builder, clampWithStartPtr(builder, ptr));

                // also clamp with cell begin
                auto cb = builder.CreateLoad(_env->i8ptrType(), _cellBeginVar);
                auto final_ptr = builder.CreateSelect(builder.CreateICmpULT(clamped_ptr, cb), cb, clamped_ptr);

                builder.CreateStore(final_ptr, _cellEndVar);
            }

            inline void saveLineBegin(IRBuilder &builder) {
                builder.CreateStore(builder.CreateLoad(_env->i8ptrType(), _currentPtrVar), _lineBeginVar);
            }

            inline void saveLineEnd(IRBuilder &builder) {
                builder.CreateStore(clampWithEndPtr(builder,
                                                    builder.CreateLoad(_env->i8ptrType(), _currentPtrVar)),
                                    _lineEndVar);
            }

            inline llvm::Value *currentPtr(IRBuilder &builder) {
                return builder.CreateLoad(_env->i8ptrType(), _currentPtrVar);
            }

            inline llvm::Value *numParsedBytes(IRBuilder &builder) {
                auto ptr = currentPtr(builder);
                return builder.CreateSub(builder.CreatePtrToInt(ptr, _env->i64Type()),
                                         builder.CreatePtrToInt(_inputPtr, _env->i64Type()));
            }


            inline llvm::Value *storageCondition(IRBuilder &builder, llvm::Value *cellNo) {
                // returns condition on whether cell with cellNo (starts with 0)
                // shall be stored or not according to descs
                assert(cellNo->getType() == _env->i32Type());

                llvm::Value *cond = nullptr; // true
                for (int i = 0; i < _cellDescs.size(); ++i) {
                    if (_cellDescs[i].willBeSerialized) {
                        if (!cond) {
                            // lazy init
                            cond = builder.CreateICmpEQ(cellNo, _env->i32Const(i));
                        } else {
                            // or with new condition
                            cond = builder.CreateOr(cond, builder.CreateICmpEQ(cellNo, _env->i32Const(i)));
                        }
                    }
                }

                // none will be stored?
                if (!cond) {
                    // return false
                    return _env->i1Const(false);
                } else {
                    return cond;
                }
            }

            inline size_t numCellsToSerialize() const {
                return std::max((size_t) 1, serializedType().parameters().size());
            }

            void fillResultCode(IRBuilder &builder, bool errorOccurred);

            /*!
             * generates i1 to check whether curChar is '\n' or '\r'
             * @param builder
             * @param curChar
             * @return
             */
            llvm::Value *newlineCondition(IRBuilder &builder, llvm::Value *curChar);

            llvm::Value *
            generateCellSpannerCode(IRBuilder &builder, const std::string& name, char c1 = 0, char c2 = 0, char c3 = 0, char c4 = 0);

            llvm::Value *executeSpanner(IRBuilder &builder, llvm::Value *spanner, llvm::Value *ptr);

            // NEW: code-gen null value check (incl. quoting!)
            llvm::Value *isCellNullValue(IRBuilder &builder, llvm::Value *cellBegin, llvm::Value *cellEndIncl) {

                // @TODO: generate more complicated check logic!

                auto cell_size =  builder.CreateSub(builder.CreatePtrToInt(cellEndIncl, _env->i64Type()),
                                                    builder.CreatePtrToInt(cellBegin, _env->i64Type()));

                // if cell_size == 0, then it's an empty cell!
                auto isEmpty = builder.CreateICmpEQ(cell_size, _env->i64Const(0));

                // empty quoted cell?
                auto isEmptyQuoted = builder.CreateAnd(builder.CreateICmpEQ(cell_size, _env->i64Const(2)),
                                                       _env->fixedSizeStringCompare(builder, cellBegin, char2str(_quotechar) + char2str(_quotechar)));

                return builder.CreateOr(isEmpty, isEmptyQuoted);

                // NEW: fails...
                // return _env->compareToNullValues(builder, cellBegin, _null_values);
            }

            llvm::Value *isCellQuoted(IRBuilder &builder, llvm::Value *cellBegin, llvm::Value *cellEnd) {
                auto i8ptr_type = llvm::Type::getInt8PtrTy(_env->getContext(), 0);
                assert(cellBegin->getType() == i8ptr_type);
                assert(cellBegin->getType() == i8ptr_type);

                // special case: cellbegin == startPtr OR cellend == endPtr
                // ==> cell can't be quoted
                auto cellAtBoundaries = builder.CreateOr(builder.CreateICmpEQ(cellBegin, _inputPtr),
                                                         builder.CreateICmpEQ(cellEnd, _endPtr));


                auto beforeCellBegin = clampWithStartPtr(builder, builder.MovePtrByBytes(cellBegin, -1));
                // note that cellEnd is excl. Hence at cellEnd there is the character after the cell end
                auto afterCellEnd = clampWithEndPtr(builder, builder.MovePtrByBytes(cellEnd, (int64_t)0));

                auto beforeIsQuote = builder.CreateICmpEQ(builder.CreateLoad(builder.getInt8Ty(), beforeCellBegin),
                                                          _env->i8Const(_quotechar));
                auto afterIsQuote = builder.CreateICmpEQ(builder.CreateLoad(builder.getInt8Ty(), afterCellEnd), _env->i8Const(_quotechar));
                auto beforeAndAfterAreQuotes = builder.CreateAnd(beforeIsQuote, afterIsQuote);

                return builder.CreateSelect(cellAtBoundaries, _env->i1Const(false), beforeAndAfterAreQuotes);
            }

            void createParseDoneBlocks(llvm::BasicBlock *bParseDone);


            void buildQuotedCellBlocks(llvm::BasicBlock *bQuotedCellBegin,
                                       llvm::BasicBlock *bCellDone);

            void buildUnquotedCellBlocks(llvm::BasicBlock *bUnquotedCellBegin,
                                         llvm::BasicBlock *bCellDone);

            size_t bitmapBitCount() const {
                // ==> multiple of 64, larger than columns

                return core::ceilToMultiple(_cellDescs.size(), 64ul);
            }


            // store in result ptr bad parse result
            void storeBadParseInfo(const IRBuilder& builder);


            llvm::Function* getCSVNormalizeFunc();

        public:
            CSVParseRowGenerator(LLVMEnvironment *env,
                                 const std::vector<std::string> &null_values = std::vector<std::string>{""},
                                 char quotechar = '"', char delimiter = ',',
                                 char escapechar = '\0') : _env(env), _null_values(null_values) {
                _quotechar = quotechar;
                _delimiter = delimiter;
                _escapechar = escapechar;
                _func = nullptr;
                _resultType = nullptr;

                // clear all
                _inputPtr = _endPtr = _resultPtr = nullptr;
            }

            /*!
             * adds code to parse a cell. If serialize if specified to be true, cell contents will be automatically serialized/converted.
             * @param type
             * @param serialize
             * @return
             */
            CSVParseRowGenerator &addCell(const python::Type &type, bool serialize);

            void build(bool internalOnly = true);


            /*!
             * returns the row type of the data that will be serialized through this parser in memory. (i.e. allows to skip certain cells)
             * @return
             */
            python::Type serializedType() const;


            std::string functionName() const {
                return getFunction()->getName().str();
            }

            /*!
             * the return type of this function. It is a struct type with the following fields:
             * {lineStart: i8*, lineEnd: i8*, ...} with ... being the converted values (ready to be serialized later)
             * @return
             */
            llvm::Type *resultType() const;

            llvm::Function *getFunction() const {
                assert(_func);
                return _func;
            }


            /*!
             * helper function to generate code to fetch from result var a serializable value
             * @param builder
             * @param column
             * @param result
             * @return serializable value. If column type is option, then isnull won't be a nullptr.
             */
            SerializableValue getColumnResult(IRBuilder &builder, int column, llvm::Value *result) const;

            /*!
             * returns pointer to cell info & Co
             * @param builder
             * @param result
             * @return
             */
            SerializableValue getCellInfo(IRBuilder& builder, llvm::Value* result) const;

        };

        /*!
         * helper to generate spanner code function in LLVM IR
         * @param env
         * @param name
         * @param c1
         * @param c2
         * @param c3
         * @param c4
         * @return
         */
        extern llvm::Function* generateFallbackSpannerFunction(LLVMEnvironment& env, const std::string& name="fallback_spanner", char c1 = 0, char c2 = 0, char c3 = 0, char c4 = 0);
    }
}


#endif //TUPLEX_CSVPARSEROWGENERATOR_H