//
// Created by leonhards on 3/23/22.
//

#include <physical/codegen/CodeGenerationContext.h>

namespace tuplex {
    namespace codegen {
        std::string CodeGenerationContext::toJSON() const {
            using namespace nlohmann;
            auto &logger = Logger::instance().logger("codegen");


            try {
                json root;

                // global settings, encode them!
                root["sharedObjectPropagation"] = sharedObjectPropagation;
                root["retypeUsingOptimizedInputSchema"] = nullValueOptimization;
                root["constantFoldingOptimization"] = constantFoldingOptimization;
                root["isRootStage"] = isRootStage;
                root["generateParser"] = generateParser;
                root["normalCaseThreshold"] = normalCaseThreshold;
                root["outputMode"] = outputMode;
                root["outputFileFormat"] = outputFileFormat;
                root["outputNodeID"] = outputNodeID;
                root["outputSchema"] = outputSchema.getRowType().desc();
                root["fileOutputParameters"] = fileOutputParameters;
                root["outputLimit"] = outputLimit;
                root["hashColKeys"] = hashColKeys;
//                if(python::Type::UNKNOWN != hashKeyType && hashKeyType.hash() > 0) // HACK
//                    root["hashKeyType"] = hashKeyType.desc();
//                if(python::Type::UNKNOWN != hashBucketType && hashBucketType.hash() > 0) // HACK
//                    root["hashBucketType"] = hashBucketType.desc();
                root["hashSaveOthers"] = hashSaveOthers;
                root["hashAggregate"] = hashAggregate;
                root["inputMode"] = inputMode;
                root["inputFileFormat"] = inputFileFormat;
                root["inputNodeID"] = inputNodeID;
                root["fileInputParameters"] = fileInputParameters;

                // encode codepath contexts as well!
                if(fastPathContext.valid())
                    root["fastPathContext"] = fastPathContext.to_json();
                if(slowPathContext.valid())
                    root["slowPathContext"] = slowPathContext.to_json();

                return root.dump();
            } catch(nlohmann::json::type_error& e) {
                logger.error(std::string("exception constructing json: ") + e.what());
                return "{}";
            }
        }

        nlohmann::json encodeOperator(LogicalOperator* op) {
            nlohmann::json obj;

            if(op->type() == LogicalOperatorType::FILEINPUT)
                return dynamic_cast<FileInputOperator*>(op)->to_json();
            else if(op->type() == LogicalOperatorType::MAP)
                return dynamic_cast<MapOperator*>(op)->to_json();
            else if(op->type() == LogicalOperatorType::WITHCOLUMN)
                return dynamic_cast<WithColumnOperator*>(op)->to_json();
            else if(op->type() == LogicalOperatorType::FILTER)
                return dynamic_cast<FilterOperator*>(op)->to_json();
            else if(op->type() == LogicalOperatorType::FILEOUTPUT) {
                return dynamic_cast<FileOutputOperator*>(op)->to_json();
            } else {
                throw std::runtime_error("unsupported operator " + op->name() + " seen for encoding... HACK");
            }

            return obj;
        }

        nlohmann::json CodeGenerationContext::CodePathContext::to_json() const {
            nlohmann::json obj;
            obj["read"] = readSchema.getRowType().desc();
            obj["input"] = inputSchema.getRowType().desc();
            obj["output"] = outputSchema.getRowType().desc();

            obj["colsToRead"] = columnsToRead;

            // now the most annoying thing, encoding the operators
            auto ops = nlohmann::json::array();
            ops.push_back(encodeOperator(inputNode.get()));
            for(auto op : operators)
                ops.push_back(encodeOperator(op.get()));

            obj["operators"] = ops;

            // serialize checks
            auto arr = nlohmann::json::array();
            for(auto check : checks)
                arr.push_back(check.to_json());
            obj["checks"] = arr;

            return std::move(obj);
        }

        // decode now everything...
        CodeGenerationContext CodeGenerationContext::fromJSON(const std::string &json_str) {
            CodeGenerationContext ctx;
            using namespace nlohmann;
            auto &logger = Logger::instance().logger("codegen");

            try {
                // try to extract json
                auto root = json::parse(json_str);

                // global settings, encode them!
                ctx.sharedObjectPropagation = root["sharedObjectPropagation"].get<bool>();
                ctx.nullValueOptimization = root["retypeUsingOptimizedInputSchema"].get<bool>();// = retypeUsingOptimizedInputSchema;
                ctx.constantFoldingOptimization = root["constantFoldingOptimization"].get<bool>();// = retypeUsingOptimizedInputSchema;
                ctx.isRootStage = root["isRootStage"].get<bool>();// = isRootStage;
                ctx.generateParser = root["generateParser"].get<bool>();// = generateParser;
                ctx.normalCaseThreshold = root["normalCaseThreshold"].get<double>();// = normalCaseThreshold;
                ctx.outputMode = static_cast<EndPointMode>(root["outputMode"].get<int>());// = outputMode;
                ctx.outputFileFormat = static_cast<FileFormat>(root["outputFileFormat"].get<int>());// = outputFileFormat;
                ctx.outputNodeID = root["outputNodeID"].get<int>();// = outputNodeID;
                ctx.outputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(root["outputSchema"].get<std::string>()));// = outputSchema.getRowType().desc();
                ctx.fileOutputParameters = root["fileOutputParameters"].get<std::unordered_map<std::string, std::string>>();// = fileOutputParameters;
                ctx.outputLimit = root["outputLimit"].get<size_t>();// = outputLimit;
                ctx.hashColKeys = root["hashColKeys"].get<std::vector<size_t>>();// = hashColKeys;

                ctx.hashSaveOthers = root["hashSaveOthers"].get<bool>();// = hashSaveOthers;
                ctx.hashAggregate = root["hashAggregate"].get<bool>();// = hashAggregate;
                ctx.inputMode = static_cast<EndPointMode>(root["inputMode"].get<int>());// = inputMode;
                ctx.inputFileFormat = static_cast<FileFormat>(root["inputFileFormat"].get<int>());
                ctx.inputNodeID = root["inputNodeID"].get<int>();// = inputNodeID;
                ctx.fileInputParameters = root["fileInputParameters"].get<std::unordered_map<std::string, std::string>>();// = fileInputParameters;

                // TODO:
                // if(python::Type::UNKNOWN != hashKeyType && hashKeyType.hash() > 0) // HACK
                //     root["hashKeyType"] = hashKeyType.desc();
                // if(python::Type::UNKNOWN != hashBucketType && hashBucketType.hash() > 0) // HACK
                //     root["hashBucketType"] = hashBucketType.desc();

                if(root.find("fastPathContext") != root.end()) {
                    // won't be true, skip
                    ctx.fastPathContext = CodeGenerationContext::CodePathContext::from_json(root["fastPathContext"]);
                }
                if(root.find("slowPathContext") != root.end()) {
                    ctx.slowPathContext = CodeGenerationContext::CodePathContext::from_json(root["slowPathContext"]);
                }
            } catch(const json::parse_error& jse) {
                logger.error(std::string("failed to decode: ") + jse.what());
            }

            return ctx;
        }

        CodeGenerationContext::CodePathContext CodeGenerationContext::CodePathContext::from_json(nlohmann::json obj) {
            CodeGenerationContext::CodePathContext ctx;

            // decode schemas
            ctx.readSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["read"].get<std::string>()));
            ctx.inputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["input"].get<std::string>()));
            ctx.outputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["output"].get<std::string>()));
            ctx.columnsToRead = obj["colsToRead"].get<std::vector<bool>>();

            // now the worst, decoding the operators...
            std::vector<std::shared_ptr<LogicalOperator>> operators;
            for(auto json_op : obj["operators"]) {
                // only map and csv supported
                auto name = json_op["name"].get<std::string>();
                if(strStartsWith(name, "input_")) {
                    auto fop = FileInputOperator::from_json(json_op);
                    // !! remove input files so there is no bad typing happening.
                    fop->setInputFiles({}, {});

                    operators.push_back(std::shared_ptr<LogicalOperator>(fop));
                } else if(strStartsWith(name, "output_")) {
                    operators.push_back(std::shared_ptr<LogicalOperator>(FileOutputOperator::from_json(operators.back(), json_op)));
                } else if(name == "withColumn") {
                    auto columnNames = json_op["columnNames"].get<std::vector<std::string>>();
                    auto columnName = json_op["columnName"].get<std::string>();
                    auto id = json_op["id"].get<int>();
                    auto code = json_op["udf"]["code"].get<std::string>();
                    // @TODO: avoid typing call?
                    // i.e. this will draw a sample too?
                    // or ok, because sample anyways need to get drawn??
                    UDF udf(code);

                    auto wop = new WithColumnOperator(operators.back(), columnNames, columnName, udf);
                    wop->setID(id);
                    operators.push_back(std::shared_ptr<LogicalOperator>(wop));
                } else if(name == "filter") {
                    auto columnNames = json_op["columnNames"].get<std::vector<std::string>>();
                    auto id = json_op["id"].get<int>();
                    auto code = json_op["udf"]["code"].get<std::string>();
                    // @TODO: avoid typing call?
                    // i.e. this will draw a sample too?
                    // or ok, because sample anyways need to get drawn??
                    UDF udf(code);

                    auto fop = new FilterOperator(operators.back(), udf, columnNames);
                    fop->setID(id);
                    operators.push_back(std::shared_ptr<LogicalOperator>(fop));
                } else if(name == "map" || name == "select" || name == "rename") {
                    // map is easy, simply decode UDF and hook up with parent operator!
                    assert(!operators.empty());
                    auto columnNames = json_op["columnNames"].get<std::vector<std::string>>();
                    auto id = json_op["id"].get<int>();
                    auto code = json_op["udf"]["code"].get<std::string>();
                    // @TODO: avoid typing call?
                    // i.e. this will draw a sample too?
                    // or ok, because sample anyways need to get drawn??
                    UDF udf(code);

                    auto mop = new MapOperator(operators.back(), udf, columnNames);
                    mop->setID(id);
                    mop->setName(json_op["name"].get<std::string>());

                    // set other important fields
                    mop->setOutputColumns(json_op["outputColumns"].get<std::vector<std::string>>());

                    operators.push_back(std::shared_ptr<LogicalOperator>(mop));
                } else {
                    throw std::runtime_error("attempting to decode unknown op " + name);
                }
            }
            ctx.inputNode = operators.front();
            ctx.operators = std::vector<std::shared_ptr<LogicalOperator>>(operators.begin() + 1, operators.end());

            // decode checks
            std::vector<NormalCaseCheck> checks;
            for(auto json_check : obj["checks"]) {
                checks.push_back(NormalCaseCheck::from_json(json_check));
            }
            ctx.checks = checks;


//            obj["read"] = readSchema.getRowType().desc();
//            obj["input"] = inputSchema.getRowType().desc();
//            obj["output"] = outputSchema.getRowType().desc();
//
//            obj["colsToRead"] = columnsToRead;
//
//            // now the most annoying thing, encoding the operators
//            auto ops = nlohmann::json::array();
//            ops.push_back(encodeOperator(inputNode));
//            for(auto op : operators)
//                ops.push_back(encodeOperator(op));
//
//            obj["operators"] = ops;
//            return obj;

            return ctx;
        }

        std::string serialize_codegen_context(const CodeGenerationContext& ctx) {
            auto& logger = Logger::instance().logger("io");

#ifdef BUILD_WITH_CEREAL
            // use test-wise cereal to encode the context (i.e., the stage) to send
            // over to individual executors for specialization.
            std::ostringstream oss(std::stringstream::binary);
            std::string serialized_type_closure;
            {
                // cereal::BinaryOutputArchive ar(oss);
                BinaryOutputArchive ar(oss);
                ar(ctx);
                // ar going out of scope flushes everything

                serialized_type_closure = serialize_type_closure(ar.typeClosure());
            }
            auto bytes_str = oss.str();
            // prepend type closure.
            bytes_str = string_mem_cat(serialized_type_closure, bytes_str);
#else
            std::string bytes_str;

                // use custom written JSON serialization routine
                bytes_str = ctx.toJSON();
#endif
            logger.info("Serialized CodeGeneration Context to " + sizeToMemString(bytes_str.size()));
            // compress this now using zip or so...
            // https://gist.github.com/gomons/9d446024fbb7ccb6536ab984e29e154a
            auto compressed_cg_str = compress_string(bytes_str);
            logger.info("ZLIB compressed CodeGeneration Context is: " + sizeToMemString(compressed_cg_str.size()));
            // @TODO: remove the hacky stuff!

#ifndef NDEBUG
            // validate result
            auto decompressed_str = decompress_string(compressed_cg_str);
            if(decompressed_str != bytes_str)
                logger.error("decompressed string doesn't match compressed one.");
#endif
            return compressed_cg_str;
        }

        CodeGenerationContext deserialize_codegen_context(const std::string& data) {
            auto& logger = Logger::instance().logger("io");

            CodeGenerationContext ctx;

            Timer timer;
#ifdef BUILD_WITH_CEREAL
            {
                auto decompressed_str = decompress_string(data);
                logger.info("Decompressed Code context from " + sizeToMemString(data.size()) + " to " + sizeToMemString(decompressed_str.size()));
                Timer deserializeTimer;

                // extract type map first. and then set up archive.
                size_t bytes_read_for_type_map = 0;
                auto type_map = deserialize_type_closure(reinterpret_cast<const uint8_t *>(decompressed_str.data()), &bytes_read_for_type_map);

                std::istringstream iss(decompressed_str.substr(bytes_read_for_type_map));
                // cereal::BinaryInputArchive ar(iss);
                BinaryInputArchive ar(iss, type_map);
                ar(ctx);
                logger.info("Deserialization of Code context took " + std::to_string(deserializeTimer.time()) + "s");
            }
            logger.info("Total Stage Decode took " + std::to_string(timer.time()) + "s");
#else
            // use custom JSON encoding
        auto decompressed_str = decompress_string(data);
        logger.info("Decompressed Code context from " + sizeToMemString(compressed_str.size()) + " to " + sizeToMemString(decompressed_str.size()));
        Timer deserializeTimer;
        ctx = codegen::CodeGenerationContext::fromJSON(decompressed_str);
        logger.info("Deserialization of Code context took " + std::to_string(deserializeTimer.time()) + "s");
        logger.info("Total Stage Decode took " + std::to_string(timer.time()) + "s");
#endif
            return ctx;
        }
    }
}