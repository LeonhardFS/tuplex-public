//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/FileInputOperator.h>
#include <VirtualFileSystem.h>
#include <orc/VirtualInputStream.h>
#include <CSVUtils.h>
#include <CSVStatistic.h>
#include <algorithm>
#include <orc/OrcTypes.h>

#include <JSONUtils.h>
#include <JsonStatistic.h>
#include <physical/experimental/JsonHelper.h>

#include <Base.h>
#include <StringUtils.h>

namespace tuplex {

    python::Type generalize_struct_type(const python::Type& struct_type) {
        if(!struct_type.isStructuredDictionaryType())
            return struct_type;

        // go over pairs and make them all optional
        auto kv_pairs = struct_type.get_struct_pairs();
        for(auto& p : kv_pairs) {
            // p.alwaysPresent = false;
            p.presence = python::MAYBE_PRESENT;

            // update value types as well
            p.valueType = generalize_struct_type(p.valueType);
        }
        return python::Type::makeStructuredDictType(kv_pairs);
    }

    // wip: more generalizing function
    python::Type generalize_type(const python::Type& type) {
        // is it a tuple?
        if(type.isTupleType()) {
            // make everything optional...
            auto col_types = type.parameters();
            for(auto& col_type : col_types) {

                if(col_type.withoutOption().isStructuredDictionaryType())
                    col_type = generalize_struct_type(col_type);

                if(!col_type.isOptionType() && col_type != python::Type::NULLVALUE) {
                    col_type = python::Type::makeOptionType(col_type);
                }
            }
            return python::Type::makeTupleType(col_types);
        }

        return type;
    }


    FileInputOperator::FileInputOperator() : _fmt(FileFormat::OUTFMT_UNKNOWN),
                                             _json_unwrap_first_level(false),
                                             _json_treat_heterogeneous_lists_as_tuples(true),
                                             _json_use_generic_dicts(false),
                                             _header(false),
                                             _sampling_time_s(0.0), _quotechar('\0'),
                                             _delimiter('\0'), _samplingMode(SamplingMode::UNKNOWN),
                                             _isRowSampleProjected(false),
                                             _cachePopulated(false),
                                             _estimatedRowCount(0),
                                             _samplingSize(0) {}

    void FileInputOperator::detectFiles(const std::string& pattern) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        // walk files
        size_t totalInputSize = 0;
        size_t printFactor = 1000;
        VirtualFileSystem::walkPattern(URI(pattern), [&](void *userData, const tuplex::URI &uri, size_t size) {
            _fileURIs.push_back(uri);
            _sizes.push_back(size);
            totalInputSize += size;
            // print every printFactor files out, how many were found
            if (_fileURIs.size() % printFactor == 0) {
            logger.info("found " + pluralize(_fileURIs.size(), "file") + " (" + sizeToMemString(totalInputSize) +
            ") so far...");
            }
            return true;
        });

        logger.info("found " + pluralize(_fileURIs.size(), "file") + " (" + sizeToMemString(totalInputSize) +
        ") to process.");
    }

    aligned_string FileInputOperator::loadSample(size_t sampleSize, const URI& uri, size_t file_size, const SamplingMode& mode, bool use_cache, size_t* file_offset) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        if(0 == file_size || uri == URI::INVALID)
            return "";

        // is the URI range based?
        size_t range_start = 0, range_end = 0;
        size_t range_size = 0;
        URI target_uri;
        decodeRangeURI(uri.toString(), target_uri, range_start, range_end);

        // both 0? no restriction
        if(range_start == 0 && range_end == 0) {
            range_end = file_size;
            range_size = file_size;
        } else {
            // restrict!
            range_size = range_end - range_start;
        }

        if(0 == range_size)
            return "";

        assert(range_size > 0);

        // correct sample size for small files
        sampleSize = std::min(sampleSize, range_size);

        auto key = std::make_tuple(uri, perFileMode(mode));
        if(use_cache) {
            std::lock_guard<std::mutex> lock(_sampleCacheMutex);

            // check if in sample cache already
            auto it = _sampleCache.find(key);
            if(it != _sampleCache.end())
                return it->second;
        }

        auto vfs = VirtualFileSystem::fromURI(uri);
        auto read_mode = VirtualFileMode::VFS_READ;
        auto vf = vfs.open_file(target_uri, read_mode);
        if (!vf) {
            logger.error("could not open file " + uri.toString());
            return "";
        }

        // if range_start != 0 -> seek
        if(0 != range_start)
            vf->seek(range_start);

        if(file_offset)
            *file_offset = range_start;

        // make sample size multiple
        sampleSize = core::ceilToMultiple(std::min(sampleSize, range_size), 16ul);

        if(sampleSize > range_size)
            sampleSize = range_size;

        if(0 == sampleSize)
            return "";

        assert(range_size >= sampleSize);

        // depending on sampling mode seek in file!
        switch(mode) {
            case SamplingMode::LAST_ROWS: {
                vf->seek(range_size - sampleSize);
                if(file_offset)
                    *file_offset = range_start + range_size - sampleSize;
                break;
            }
            case SamplingMode::RANDOM_ROWS: {
                // random seek between 0 and file_size - sampleSize
                auto randomSeekOffset = randi(0ul, range_size - sampleSize);
                vf->seek(randomSeekOffset);
                if(file_offset)
                    *file_offset = range_start + randomSeekOffset;
                break;
            }
            default:
                // nothing todo (i.e. first rows mode!)
                break;
        }

        // memory must be at least 16
        auto nbytes_sample = core::ceilToMultiple(sampleSize + 16 + 1, 16ul);
        aligned_string sample(nbytes_sample, '\0');
        assert(sample.capacity() > 16 && sample.size() > 16);

        auto ptr = &sample[0];
        // memset last 16 bytes to 0
        assert(ptr + nbytes_sample - 16ul >= ptr);
        assert(nbytes_sample >= 16ul);
        std::memset(ptr + nbytes_sample - 16ul, 0, 16ul);
        // read contents
        size_t bytesRead = 0;
        vf->readOnly(ptr, sampleSize, &bytesRead); // use read-only here to speed up sampling
        auto end = ptr + bytesRead;

        // memset to 0 after bytesRead for 16 bytes
        assert(bytesRead + 16 < nbytes_sample);
        std::memset(ptr + bytesRead, 0, 16ul);

        Logger::instance().defaultLogger().info(
                "sampled " + uri.toString() + " on " + sizeToMemString(sampleSize));

        if(use_cache) {
            std::lock_guard<std::mutex> lock(_sampleCacheMutex);

            // put into cache
            _sampleCache[key] = sample;
        }

        return sample;
    }

    FileInputOperator *FileInputOperator::fromText(const std::string &pattern, const ContextOptions &co,
                                                    const std::vector<std::string> &null_values,
                                                    const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, null_values, sampling_mode);
    }


    size_t FileInputOperator::estimateTextFileRowCount(size_t sample_size, const SamplingMode& mode) {
        if(_fileURIs.empty())
            return 0;

        // take a single sample and then scale over all available text files...
        auto sample = loadSample(sample_size, _fileURIs.front(), _sizes.front(), SamplingMode::FIRST_ROWS);

        // estimate row count
        // split into lines, compute average length & scale row estimate up
        auto lines = splitToLines(sample.c_str());
        size_t accLineLength = 0;
        for(auto line : lines)
            accLineLength += line.length();
        double avgLineLength = lines.empty() ? 1.0 : (double)accLineLength / (double)lines.size();
        double estimatedRowCount = 0.0;
        for(auto s : _sizes)
            estimatedRowCount += (double)s / (double)avgLineLength;
        return static_cast<size_t>(std::ceil(estimatedRowCount));
    }

    FileInputOperator::FileInputOperator(const std::string& pattern,
            const ContextOptions& co,
            const std::vector<std::string>& null_values,
            const SamplingMode& sampling_mode) : _null_values(null_values), _estimatedRowCount(0), _sampling_time_s(0.0), _samplingMode(normalizeSamplingMode(sampling_mode)), _samplingSize(co.SAMPLE_SIZE()), _cachePopulated(false) {

        auto &logger = Logger::instance().logger("fileinputoperator");
        _fmt = FileFormat::OUTFMT_TEXT;
        _quotechar = '\0';
        _delimiter = '\0';
        _header = false;
        _columnNames.reserve(1);

        Timer timer;
        detectFiles(pattern);

        // fill sample cache
        if(!_fileURIs.empty()) {
            // fill sampling cache
            fillFileCache(sampling_mode);

            // run estimation
            _estimatedRowCount = estimateTextFileRowCount(co.SAMPLE_MAX_DETECTION_MEMORY(), sampling_mode);
        } else {
            _estimatedRowCount = 0;
        }

        // when no null-values are given, simply set to string always
        // else, it's an option type...
        auto rowType = python::Type::makeTupleType({python::Type::STRING});
        _normalCaseRowType = rowType; // NVO speculation?
        if(!null_values.empty())
            rowType = python::Type::makeTupleType({python::Type::makeOptionType(python::Type::STRING)});
        _generalCaseRowType = rowType;

        // get type & assign schema
        setOutputSchema(Schema(Schema::MemoryLayout::ROW, rowType));
        setProjectionDefaults();
#ifndef NDEBUG
        logger.info("Created file input operator for text file");
#endif
        _sampling_time_s = timer.time();
        logger.info("Sampling took " + std::to_string(_sampling_time_s));
    }

    FileInputOperator *FileInputOperator::fromCsv(const std::string &pattern, const ContextOptions &co,
                                                   option<bool> hasHeader,
                                                   option<char> delimiter,
                                                   option<char> quotechar,
                                                   const std::vector<std::string> &null_values,
                                                   const std::vector<std::string>& column_name_hints,
                                                   const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                                                   const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                                                   const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, hasHeader, delimiter, quotechar, null_values,
                                     column_name_hints, index_based_type_hints, column_based_type_hints, sampling_mode);
    }

    inline void check_is_row_type(const python::Type& row_type) {
        if(PARAM_USE_ROW_TYPE) {
            if(row_type != python::Type::UNKNOWN && !row_type.isRowType())
                throw std::runtime_error("PARAM_USE_ROW_TYPE is specified, but given type is " + row_type.desc() + ", should be Row[...]");
        }
    }

    FileInputOperator *FileInputOperator::fromJSON(const std::string &pattern,
                                                   bool unwrap_first_level,
                                                   bool treat_heterogenous_lists_as_tuples,
                                                   const ContextOptions &co,
                                                   const SamplingMode &sampling_mode,
                                                   const std::unordered_map<std::string, python::Type>& column_based_type_hints) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        auto f = new FileInputOperator();
        f->_fmt = FileFormat::OUTFMT_JSON;
        f->_json_unwrap_first_level = unwrap_first_level; //! set here for detect to work.
        f->_json_treat_heterogeneous_lists_as_tuples = treat_heterogenous_lists_as_tuples;
        f->_json_use_generic_dicts = co.EXPERIMENTAL_USE_GENERIC_DICTS();
        f->_samplingMode = normalizeSamplingMode(sampling_mode);
        f->_samplingSize = co.SAMPLE_SIZE();

        Timer timer;
        f->detectFiles(pattern);

        std::vector<std::string> sample_column_names; // <-- store names of sample.

        if (!f->_fileURIs.empty()) {

            std::vector<std::vector<std::string>> nameCollection;
            std::vector<std::vector<std::string>> *namePtr = f->_json_unwrap_first_level ? &nameCollection : nullptr;

            // fill sampling cache
            f->fillFileCache(f->_samplingMode);
            // now fill row cache (by parsing rows)
            if (!co.USE_STRATIFIED_SAMPLING())
                f->fillRowCache(f->_samplingMode, namePtr, co.SAMPLE_SIZE());
            else
                f->fillRowCacheWithStratifiedSamples(f->_samplingMode, namePtr, co.SAMPLE_SIZE(), co.SAMPLE_STRATA_SIZE(),
                                                     co.SAMPLE_SAMPLES_PER_STRATA());

            // detect normal/general type if not given
            std::tuple<python::Type, python::Type> t{python::Type::UNKNOWN, python::Type::UNKNOWN};
            // need to invoke always in order to fill sample cache.
            t = f->detectJsonTypesAndColumns(co, nameCollection);

            // save names from sample
            sample_column_names = f->_columnNames;

            python::Type normalcasetype = std::get<0>(t);
            python::Type generalcasetype = std::get<1>(t);

            // make type more general (to cover potentially more!)
            generalcasetype = generalize_type(generalcasetype);

            // special case, no unwrap -> remove inner option.
            if (!unwrap_first_level) {
                if (generalcasetype.isTupleType())
                    generalcasetype = python::Type::makeTupleType(
                            std::vector<python::Type>({generalcasetype.parameters().front().withoutOption()}));
                else
                    generalcasetype = generalcasetype.withoutOption();
            }

            // run row count estimation (required for cost-based optimizer)
            f->_estimatedRowCount = f->estimateSampleBasedRowCount();

            if (PARAM_USE_ROW_TYPE) {
                if (nameCollection.empty()) {
                    logger.info("no samples given, setting type to empty row.");
                    f->_normalCaseRowType = python::Type::EMPTYROW;
                    f->_generalCaseRowType = python::Type::EMPTYROW;
                } else {

                    normalcasetype = python::Type::makeRowType(normalcasetype.parameters(), sample_column_names);
                    generalcasetype = python::Type::makeRowType(generalcasetype.parameters(), sample_column_names);

                    assert(normalcasetype.isRowType());
                    f->_normalCaseRowType = normalcasetype;

                    assert(generalcasetype.isRowType());
                    f->_generalCaseRowType = generalcasetype;
                }
            } else {
                // get type & assign schema
                f->_normalCaseRowType = normalcasetype;
                f->_generalCaseRowType = generalcasetype;
            }
        } else {
            f->_estimatedRowCount = 0;
            logger.warn("no input files found, can't infer type from given path: " + pattern);

            auto default_type = PARAM_USE_ROW_TYPE ? python::Type::EMPTYROW : python::Type::EMPTYTUPLE;

            f->_normalCaseRowType = default_type;
            f->_generalCaseRowType = default_type;
        }

        if(!column_based_type_hints.empty()) {

            // which columns are contained? which aren't?
            std::unordered_map<std::string, python::Type> contained_hints;
            std::map<std::string, python::Type> new_hints;
            for(auto kv: column_based_type_hints) {
                if(indexInVector(kv.first, f->_columnNames) >= 0)
                    contained_hints[kv.first] = kv.second;
                else
                    new_hints[kv.first] = kv.second;
            }
            std::vector<std::string> new_columns;
            for(auto kv: new_hints)
                new_columns.push_back(kv.first);
            std::sort(new_columns.begin(), new_columns.end());

            auto n_col_types = f->_normalCaseRowType.isTupleType() ? f->_normalCaseRowType.parameters() : f->_normalCaseRowType.get_column_types();
            auto g_col_types = f->_generalCaseRowType.isTupleType() ? f->_generalCaseRowType.parameters() : f->_generalCaseRowType.get_column_types();

            std::vector<python::Type> dummy(new_columns.size(), python::Type::NULLVALUE);
            // append new columns w. hints
            std::copy(new_columns.begin(), new_columns.end(), std::back_inserter(f->_columnNames));
            std::copy(dummy.begin(), dummy.end(), std::back_inserter(n_col_types));
            std::copy(dummy.begin(), dummy.end(), std::back_inserter(g_col_types));

            f->_indexBasedHints.clear();

            // update existing types
            for(auto kv : column_based_type_hints) {
                auto idx = indexInVector(kv.first, f->_columnNames);
                n_col_types[idx] = kv.second;
                g_col_types[idx] = kv.second;

                // param check
                if(f->_normalCaseRowType.isRowType() && idx < f->_normalCaseRowType.get_column_count())
                    assert(f->_normalCaseRowType.get_column_name(idx) == kv.first);
                if(f->_generalCaseRowType.isRowType() && idx < f->_generalCaseRowType.get_column_count())
                    assert(f->_generalCaseRowType.get_column_name(idx) == kv.first);

                // convert to index based hints.
                f->_indexBasedHints[idx] = kv.second;
            }

            // update types:
            if(PARAM_USE_ROW_TYPE) {
                f->_normalCaseRowType = python::Type::makeRowType(n_col_types, f->_columnNames);
                f->_generalCaseRowType = python::Type::makeRowType(g_col_types, f->_columnNames);
            } else {
                f->_normalCaseRowType = python::Type::makeTupleType(n_col_types);
                f->_generalCaseRowType = python::Type::makeTupleType(g_col_types);
            }
        }

        // adjust if normal-case / general-case column count differs.
        auto normal_case_column_count = extract_columns_from_type(f->_normalCaseRowType);
        auto general_case_column_count = extract_columns_from_type(f->_generalCaseRowType);
        auto normal_case_column_names = f->_normalCaseRowType.isRowType() ? f->_normalCaseRowType.get_column_names() : f->_columnNames;
        auto general_case_column_names = f->_generalCaseRowType.isRowType() ? f->_generalCaseRowType.get_column_names() : f->_columnNames;
        auto treat_missing_first_level_as_null = unwrap_first_level; // TODO: expose as separate parameter?
        if(normal_case_column_count != general_case_column_count) {
            if(treat_missing_first_level_as_null) {

                // check what the total number of columns is
                std::set<std::string> unique_names(normal_case_column_names.begin(), normal_case_column_names.end());
                for(const auto& name : general_case_column_names)
                    unique_names.insert(name);
                auto n_unique_columns = unique_names.size();

                // adjust and sort names
                std::vector<std::string> names{unique_names.begin(), unique_names.end()};
                std::sort(names.begin(), names.end(), [](const std::string& sa, const std::string& sb) {
                    return lexicographical_compare(sa.begin(), sa.end(), sb.begin(), sb.end());
                });

                std::vector<python::Type> normal_types;
                std::vector<python::Type> general_types;

                auto n_tuple_type = f->_normalCaseRowType.isRowType() ? f->_normalCaseRowType.get_columns_as_tuple_type() : f->_normalCaseRowType;
                auto g_tuple_type = f->_generalCaseRowType.isRowType() ? f->_generalCaseRowType.get_columns_as_tuple_type() : f->_generalCaseRowType;
                for(const auto& name : names) {
                    auto n_idx = indexInVector(name, normal_case_column_names);
                    auto g_idx = indexInVector(name, general_case_column_names);

                    if(n_idx >= 0)
                        normal_types.push_back(n_tuple_type.parameters()[n_idx]);
                    else
                        normal_types.push_back(python::Type::NULLVALUE);
                    if(g_idx >= 0)
                        general_types.push_back(g_tuple_type.parameters()[g_idx]);
                    else {
                        if(n_idx >= 0)
                            general_types.push_back(python::Type::makeOptionType(n_tuple_type.parameters()[n_idx]));
                        else
                            general_types.push_back(python::Type::NULLVALUE);
                    }
                }

                f->_columnNames = names;
                f->_normalCaseRowType = PARAM_USE_ROW_TYPE ? python::Type::makeRowType(normal_types, names) : python::Type::makeTupleType(normal_types);
                f->_generalCaseRowType = PARAM_USE_ROW_TYPE ? python::Type::makeRowType(general_types, names) : python::Type::makeTupleType(general_types);
            } else {
                // TODO: error.
                f->useNormalCase();
            }
        }

        // use general case output type
        f->setOutputSchema(Schema(Schema::MemoryLayout::ROW, f->_generalCaseRowType));

        // set defaults for possible projection pushdown...
        f->setProjectionDefaults();
        f->_sampling_time_s += timer.time();

        assert(f->_normalCaseRowType.hash() >= 0);
        assert(f->_generalCaseRowType.hash() >= 0);

        // check
        logger.debug("JSON - normal case type: " + f->_normalCaseRowType.desc());
        logger.debug("JSON - general case type: " + f->_generalCaseRowType.desc());

        // empty? no files found? proceed.
        if(f->_estimatedRowCount == 0)
            return f;

        // need to adjust samples (fill in nulls etc.)
        assert(!sample_column_names.empty()); // <-- should be valid...
        f->adjustJsonSamples(sample_column_names, sample_column_names.size());

        // debug check:
        // if rows have different number of columns, must use row type!
#ifndef NDEBUG
        size_t num_row_types = 0;
        size_t min_column_count = 99999999;
        size_t max_column_count = 0;
        for(const auto& row: f->_rowsSample) {
            num_row_types += row.getRowType().isRowType();
            min_column_count = std::min(min_column_count, row.getNumColumns());
            max_column_count = std::max(max_column_count, row.getNumColumns());
        }

        // different counts?
        if(min_column_count != max_column_count) {
            assert(num_row_types == f->_rowsSample.size()); // <-- all must be of row type.
        }
#endif


        return f;
    }

    void FileInputOperator::adjustJsonSamples(std::vector<std::string> assumed_column_names, size_t assumed_column_count) {

        if(assumed_column_count != 0 && assumed_column_names.empty() && !_columnNames.empty())
            assumed_column_names = _columnNames;

        // check what normal case column names & general case column names are
        auto normal_case_column_names = _normalCaseRowType.isRowType() ? _normalCaseRowType.get_column_names() : _columnNames;
        auto general_case_column_names = _generalCaseRowType.isRowType() ? _generalCaseRowType.get_column_names() : _columnNames;

        if(!vec_equal(normal_case_column_names, general_case_column_names) && (!_normalCaseRowType.isRowType() || !_generalCaseRowType.isRowType())) {
            throw std::runtime_error("Both normal/general types must be of RowType.");
        }

        for(auto& row : _rowsSample) {
            assert(row.getNumColumns() == assumed_column_count); // <-- this must hold.

            // std::cout<<"row (before): "<<row.getRowType().desc()<<std::endl;

            // are assumed_column_names given?
            reorder_and_fill_missing_will_null(row, assumed_column_names, normal_case_column_names);


            // each sample is usually assumed to be a tuple with column names
            // std::cout<<"row (after): "<<row.getRowType().desc()<<std::endl;
        }

        // adjust column names based on expanded column names.
        auto adjusted_normal_case_column_names = reorder_and_expand_column_names(assumed_column_names,
                                                                                 normal_case_column_names);
        _columnNames = adjusted_normal_case_column_names;

        // // reset which columns to serialize.
        // _columnsToSerialize = std::vector<bool>(_columnNames.size(), true);
        //
        // assert(_columnsToSerialize.size() == _columnNames.size());
    }

    void FileInputOperator::fillRowCache(SamplingMode mode, std::vector<std::vector<std::string>>* outNames, size_t sample_limit) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        {
            std::lock_guard<std::mutex> lock(_sampleCacheMutex);
            // based on samples stored, fill the row cache
            if(_sampleCache.empty())
                return;
        }

        Timer timer;

        SamplingParameters params;
        params.limit = sample_limit;

        _isRowSampleProjected = false; // no projection, store raw samples!
        if(mode & SamplingMode::SINGLETHREADED)
            _rowsSample = sample(mode, outNames, params);
        else
            _rowsSample = multithreadedSample(mode, outNames, params);

        logger.info("Extracting row sample took " + std::to_string(timer.time()) + "s (sampling mode=" + samplingModeToString(mode) + ")");
    }

    void FileInputOperator::fillRowCacheWithStratifiedSamples(SamplingMode mode,
                                                              std::vector<std::vector<std::string>> *outNames,
                                                              size_t sample_limit_after_strata, size_t strata_size,
                                                              size_t samples_per_strata, int random_seed) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        {
            std::lock_guard<std::mutex> lock(_sampleCacheMutex);
            // based on samples stored, fill the row cache
            if(_sampleCache.empty())
                return;
        }

        SamplingParameters params;
        params.limit = sample_limit_after_strata;
        params.strata_size = strata_size;
        params.samples_per_strata = samples_per_strata;
        params.random_seed = random_seed;

        Timer timer;
        _isRowSampleProjected = false; // raw, fill with unprojected samples.
        if(mode & SamplingMode::SINGLETHREADED)
            _rowsSample = sample(mode, outNames, params);
        else
            _rowsSample = multithreadedSample(mode, outNames, params);
        logger.info("Extracting stratified row sample took " + std::to_string(timer.time()) + "s (sampling mode=" + samplingModeToString(mode) + ")");
    }

    void FileInputOperator::fillFileCache(SamplingMode mode) {
        auto &logger = Logger::instance().logger("fileinputoperator");
        Timer timer;

        // cache already populated?
        // drop!
        if(_cachePopulated) {
            std::lock_guard<std::mutex> lock(_sampleCacheMutex);
            _sampleCache.clear();
            _cachePopulated = false;
        }

        // if neither single-threaded nor multi-threaded are specified, use multi-threaded mode per default.
        if(!(mode & SamplingMode::SINGLETHREADED) && !(mode & SamplingMode::MULTITHREADED)) {
            mode = mode | SamplingMode::MULTITHREADED;
        }

        if(mode & SamplingMode::SINGLETHREADED) {
            // simply fill cache by drawing the proper samples & store rows.
            fillFileCacheSinglethreaded(_samplingMode);
        } else if(mode & SamplingMode::MULTITHREADED) {
            fillFileCacheMultithreaded(_samplingMode);
        } else {
            logger.debug("INTERNAL ERROR, invalid sampling mode.");
        }

        _cachePopulated = true;
        auto duration = timer.time();

        // only print if there are actually uris
        if(!_fileURIs.empty() && !_sizes.empty()) {
            std::string entry_info = _sampleCache.size() == 1 ? "1 entry" : std::to_string(_sampleCache.size()) + " entries";
            logger.info("Filling sample cache for " + name() + " operator took " + std::to_string(duration) + "s (" + entry_info + ")");
        }
    }

    void FileInputOperator::fillFileCacheSinglethreaded(SamplingMode mode) {
        using namespace std;
        auto &logger = Logger::instance().logger("fileinputoperator");

        // first, fill the internal cache
        assert(_sampleCache.empty());

        // this works by loading/requesting files in parallel
        std::set<int> file_indices;
        if(mode & SamplingMode::FIRST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(0);
        if(mode & SamplingMode::LAST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(_fileURIs.size() - 1);
        if(mode & SamplingMode::RANDOM_FILE && _fileURIs.size() >= 1)
            file_indices.insert(randi(0ul, _fileURIs.size() - 1));

        if(mode & SamplingMode::ALL_FILES) {
            for(unsigned i = 0; i < _fileURIs.size(); ++i)
                file_indices.insert(i);
        }
        // empty?
        if(file_indices.empty())
            return;

        // now load all the files in parallel into vector!
        vector<unsigned> indices(file_indices.begin(), file_indices.end());
        std::sort(indices.begin(), indices.end());
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            auto uri = _fileURIs[idx];
            auto size = _sizes[idx];
            auto file_mode = perFileMode(mode);
            auto key = std::make_tuple(uri, file_mode);

            {
                // place into cache (using true will automatically do this...)
                loadSample(_samplingSize, uri, size, mode, true);
            }
        }

        logger.info("Sample fetch done.");
    }

    void FileInputOperator::fillFileCacheMultithreaded(SamplingMode mode) {
        using namespace std;
        auto &logger = Logger::instance().logger("fileinputoperator");

        // first, fill the internal cache
        assert(_sampleCache.empty());

        // this works by loading/requesting files in parallel
        std::set<int> file_indices;
        if(mode & SamplingMode::FIRST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(0);
        if(mode & SamplingMode::LAST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(_fileURIs.size() - 1);
        if(mode & SamplingMode::RANDOM_FILE && _fileURIs.size() >= 1)
            file_indices.insert(randi(0ul, _fileURIs.size() - 1));

        if(mode & SamplingMode::ALL_FILES) {
            for(unsigned i = 0; i < _fileURIs.size(); ++i)
                file_indices.insert(i);
        }
        // empty?
        if(file_indices.empty())
            return;

        assert(_samplingSize > 0);

        // now load all the files in parallel into vector!
        vector<unsigned> indices(file_indices.begin(), file_indices.end());
        std::sort(indices.begin(), indices.end());

        vector<std::future<aligned_string>> vf(file_indices.size());
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            vf[i] = std::async(std::launch::async, [this](const URI& uri, size_t size, const SamplingMode& mode) {
                return loadSample(_samplingSize, uri, size, mode, false);
            }, _fileURIs[idx], _sizes[idx], mode);
        }

        // check till all futures are done
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            auto uri = _fileURIs[idx];
            auto file_mode = perFileMode(mode);
            auto key = std::make_tuple(uri, file_mode);
            {
                std::lock_guard<std::mutex> lock(_sampleCacheMutex);
                // place into cache
                _sampleCache[key] = vf[i].get();
            }
        }

        logger.info("Parallel sample fetch done.");
    }

    std::vector<Row> FileInputOperator::multithreadedSample(const SamplingMode &mode,
                                                            std::vector<std::vector<std::string>>* outNames,
                                                            const SamplingParameters& sampling_params) {
        using namespace std;
        auto &logger = Logger::instance().logger("fileinputoperator");

        // construct indices from cache
        if(!_cachePopulated)
            fillFileCache(mode);
        set<unsigned> file_indices;
        {
            std::lock_guard<std::mutex> lock(_sampleCacheMutex);
            for(const auto& keyval : _sampleCache) {
                auto uri = std::get<0>(keyval.first);
                auto idx = indexInVector(uri, _fileURIs);
                if(idx < 0)
                    throw std::runtime_error("fatal internal error, could not find URI " + uri.toString() + " in file cache.");
                file_indices.insert(static_cast<unsigned>(idx));
            }
        }
        vector<unsigned> indices(file_indices.begin(), file_indices.end());

        // parse now rows for detection etc.
        vector<std::future<vector<Row>>> f_rows(indices.size());
        vector<vector<vector<string>>> f_names(indices.size());
        // inline std::vector<Row> sampleFile(const URI& uri, size_t uri_size, const SamplingMode& mode)
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            auto uri = _fileURIs[idx];
            auto size = _sizes[idx];
            auto file_mode = perFileMode(mode);
            vector<vector<string>>* ptr = outNames ? &f_names[i] : nullptr; // only fetch column names if outNames is valid, else use nullptr to keep data nested.
            f_rows[i] = std::async(std::launch::async, [this, file_mode, ptr, sampling_params](const URI& uri, size_t size) {
                return sampleFile(uri, size, file_mode, ptr, sampling_params);
            }, _fileURIs[idx], _sizes[idx]);
        }

        auto sample_limit = sampling_params.limit;

        // combine rows now.
        vector<Row> res;
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto rows = f_rows[i].get();

            if(res.size() + rows.size() <= sample_limit)
                std::copy(rows.begin(), rows.end(), std::back_inserter(res));
            else {
                // how many to copy?
                auto max_to_copy = sample_limit - res.size();
                max_to_copy = std::min(max_to_copy, rows.size());
                std::copy(rows.begin(), rows.begin() + max_to_copy, std::back_inserter(res));
            }
        }

        assert(res.size() <= sample_limit);

        // if desired, combine column names
        if(outNames) {
            vector<vector<string>> sampleNames;
            for(unsigned i = 0; i < indices.size(); ++i) {
                auto names = f_names[i];

                if(sampleNames.size() + names.size() <= sample_limit)
                    std::copy(names.begin(), names.end(), std::back_inserter(sampleNames));
                else {
                    auto max_to_copy = sample_limit - sampleNames.size();
                    max_to_copy = std::min(max_to_copy, names.size());
                    std::copy(names.begin(), names.begin() + max_to_copy, std::back_inserter(sampleNames));
                }
            }
            assert(sampleNames.size() <= sample_limit);

            *outNames = sampleNames;
        }

        logger.info("Parallel sample parse done (" + pluralize(res.size(), "sample") + ").");
        return res;
    }

    FileInputOperator::FileInputOperator(const std::string &pattern,
                                         const ContextOptions &co,
                                         option<bool> hasHeader,
                                         option<char> delimiter,
                                         option<char> quotechar,
                                         const std::vector<std::string> &null_values,
                                         const std::vector<std::string>& column_name_hints,
                                         const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                                         const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                                         const SamplingMode& sampling_mode) :
            _null_values(null_values), _sampling_time_s(0.0), _samplingMode(normalizeSamplingMode(sampling_mode)), _samplingSize(co.SAMPLE_SIZE()),
            _cachePopulated(false), _estimatedRowCount(0),
            _json_unwrap_first_level(false), _json_treat_heterogeneous_lists_as_tuples(false), _json_use_generic_dicts(false) {
        auto &logger = Logger::instance().logger("fileinputoperator");
        _fmt = FileFormat::OUTFMT_CSV;

        // meaningful defaults
        _quotechar = quotechar.value_or('"');
        _delimiter = delimiter.value_or(',');
        _header = hasHeader.value_or(false);

//        // null value check. If empty array, add empty string
//        // @TODO: is this correct??
//#warning " check whether this behavior is correct! Users might wanna have empty strings"
//        if (_null_values.empty())
//            _null_values.emplace_back("");

        Timer timer;
        detectFiles(pattern);

        // infer schema using first file only
        if (!_fileURIs.empty()) {

            // fill sampling cache
            fillFileCache(sampling_mode);

            // need to load FIRST rows in order to perform header/csv detection.
            aligned_string sample;
            if(!_fileURIs.empty()) {
                sample = loadSample(co.SAMPLE_SIZE(), _fileURIs.front(), _sizes.front(),
                                    SamplingMode::FIRST_ROWS);
            }

            CSVStatistic csvstat(co.CSV_SEPARATORS(), co.CSV_COMMENTS(),
                                 co.CSV_QUOTECHAR(), co.SAMPLE_MAX_DETECTION_MEMORY(),
                                 co.SAMPLE_MAX_DETECTION_ROWS(), co.NORMALCASE_THRESHOLD(), _null_values);

            // @TODO: header is missing??
            // "what about if user specifies header? should be accounted for in estimate for small files!!!"
            csvstat.estimate(sample.c_str(), sample.size(), tuplex::option<bool>::none, delimiter, !co.OPT_NULLVALUE_OPTIMIZATION());

            // estimator for #rows across all CSV files
            double estimatedRowCount = 0.0;
            for(auto s : _sizes) {
                estimatedRowCount += (double)s / (double)csvstat.estimationBufferSize() * (double)csvstat.rowCount();
            }
            _estimatedRowCount = static_cast<size_t>(std::ceil(estimatedRowCount));

            // supplied options override estimate!
            _delimiter = delimiter.value_or(csvstat.delimiter());
            _quotechar = quotechar.value_or(csvstat.quotechar());
            _header = hasHeader.value_or(csvstat.hasHeader());

            // print info on csv header estimation
            if(!hasHeader.has_value()) {
                if(_header)
                    logger.info("Auto-detected presence of a header line in CSV files.");
                else
                    logger.info("Auto-detected there's no header line in CSV files.");
            }

            // set column names from stat
            // Note: call this BEFORE applying type hints!
            _columnNames = _header ? csvstat.columns() : std::vector<std::string>();

            // now fill row cache to detect majority type (?)
            if(!co.USE_STRATIFIED_SAMPLING())
                fillRowCache(sampling_mode, nullptr, co.SAMPLE_SIZE());
            else
                fillRowCacheWithStratifiedSamples(sampling_mode, nullptr, co.SAMPLE_SIZE(), co.SAMPLE_STRATA_SIZE(), co.SAMPLE_SAMPLES_PER_STRATA());

            // use sample to detect types
            bool use_independent_columns = true;
            // when NVO is deactivated, normalcase and general case detected here are the same.
            auto normalcasetype = detectMajorityRowType(_rowsSample, co.NORMALCASE_THRESHOLD(), use_independent_columns, co.OPT_NULLVALUE_OPTIMIZATION());

            if(python::Type::UNKNOWN == normalcasetype) {
                throw std::runtime_error("failed to detect normal case type from sample for operator " + name());
            }

            // general-case type is option[T] version of types encountered in normalcasetype.
            // for null, assume option[str].
            // -> this is required for hyperspecialization upcast + a meaningful assumption for the sample
            auto normalcase_coltypes = normalcasetype.parameters();
            std::vector<python::Type> generalcase_coltypes = normalcase_coltypes;
            for(auto& type : generalcase_coltypes) {
                if(!type.isOptionType()) {
                    if(python::Type::NULLVALUE == type)
                        type = python::Type::makeOptionType(python::Type::STRING);
                    else
                        type = python::Type::makeOptionType(type);
                }
            }
            auto generalcasetype = python::Type::makeTupleType(generalcase_coltypes);
            // NULL Value optimization on or off?
            // Note: if off, can improve performance of estimate...

            // @TODO: change this to be the majority detected row type!
            // normalcase and exception case types

            if(normalcasetype == python::Type::UNKNOWN || generalcasetype == python::Type::UNKNOWN) {
                logger.warn("sample-based detection could not infer type, using csvstat type directly.");
                normalcasetype = csvstat.type();
                generalcasetype = csvstat.superType();
            }

            // type hints?
            // ==> enforce certain type for specific columns!
            if(!index_based_type_hints.empty()) {
                assert(generalcasetype.isTupleType());
                auto paramsNormal = normalcasetype.parameters();
                auto paramsExcept = generalcasetype.parameters();
                for(const auto& keyval : index_based_type_hints) {
                    if(keyval.first >= paramsExcept.size()) {
                        // simply ignore, do not error!
                        std::stringstream ss;
                        ss<<"invalid index " + std::to_string(keyval.first) + " for type hint in " + name() +
                            " operator found. Must be between 0 and " + std::to_string(paramsExcept.size() - 1);
                        ss<<". Ignoring type hint for index "<<keyval.first;
                        logger.warn(ss.str());
                    }

                    // update params
                    paramsExcept[keyval.first] = keyval.second;
                    paramsNormal[keyval.first] = keyval.second;

                    _indexBasedHints[keyval.first] = keyval.second;
                }

                normalcasetype = python::Type::makeTupleType(paramsNormal);
                generalcasetype = python::Type::makeTupleType(paramsExcept); // update row type...
            }

            // apply column based type hints
            if(!column_based_type_hints.empty()) {
                assert(generalcasetype.isTupleType());
                auto paramsNormal = normalcasetype.parameters();
                auto paramsExcept = generalcasetype.parameters();
                for(const auto& keyval : column_based_type_hints) {

                    // translate name into index, error if out of range...!
                    size_t idx = -1;
                    auto it = std::find(std::begin(_columnNames), std::end(_columnNames), keyval.first);
                    if(it != std::end(_columnNames))
                        idx = std::distance(std::begin(_columnNames), it);
                    else {
                        // try column name hints to determine an index
                        auto it = std::find(std::begin(column_name_hints), std::end(column_name_hints), keyval.first);
                        if(it != std::end(column_name_hints))
                            idx = std::distance(std::begin(column_name_hints), it);
                    }

                    if(idx >= paramsExcept.size()) {
                        // simply ignore, do not error!
                        std::stringstream ss;
                        ss<<"no column named '"<<keyval.first<<"' found in files, ";
                        ss<<"ignoring type hint '"<<keyval.first<<"' : "<<keyval.second.desc();
                        logger.warn(ss.str());
                    }

                    // update params
                    paramsExcept[idx] = keyval.second;
                    paramsNormal[idx] = keyval.second;

                    _indexBasedHints[idx] = keyval.second;
                }

                normalcasetype = python::Type::makeTupleType(paramsNormal);
                generalcasetype = python::Type::makeTupleType(paramsExcept); // update row type...
            }

            // get type & assign schema
            _normalCaseRowType = normalcasetype;
            _generalCaseRowType = generalcasetype;

            // make row types if set
            if(PARAM_USE_ROW_TYPE) {
                _normalCaseRowType = python::Type::makeRowType(_normalCaseRowType.parameters(), _columnNames);
                _generalCaseRowType = python::Type::makeRowType(_generalCaseRowType.parameters(), _columnNames);
            }

            setOutputSchema(Schema(Schema::MemoryLayout::ROW, generalcasetype));

            // if csv stat produced unknown, issue warning and use empty
            if (csvstat.type() == python::Type::UNKNOWN) {
                setOutputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
                Logger::instance().defaultLogger().warn("csv stats could not determine type, use () for type.");
            }

            // set defaults for possible projection pushdown...
            setProjectionDefaults();

//            // now parse from the allocated buffer all rows and store as internal sample.
//            // there should be at least one 3 rows!
//            // store as internal sample
//            // => use here strlen because _firstRowsSample is zero terminated!
//            assert(sample.back() == '\0');
//            _firstRowsSample = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())), _null_values, _delimiter, _quotechar);
//            // header? ignore first row!
//            if(_header && !_firstRowsSample.empty())
//                _firstRowsSample.erase(_firstRowsSample.begin());
//
//            // draw last rows sample from last file & append.
//            if(!_fileURIs.empty()) {
//                // only draw sample IF > 1 file or file_size > 2 * sample size
//                bool draw_sample = _fileURIs.size() >= 2 || _sizes.back() >= 2 * _samplingSize;
//                if(draw_sample) {
//                    auto last_sample = loadSample(_samplingSize, _fileURIs.back(), _sizes.back(), SamplingMode::LAST_ROWS);
//                    // search CSV beginning
//                    auto column_count = inputColumnCount();
//                    auto offset = csvFindLineStart(last_sample.c_str(), _samplingSize, column_count, csvstat.delimiter(), csvstat.quotechar());
//                    if(offset >= 0) {
//                        // parse into last rows
//                        _lastRowsSample = parseRows(last_sample.c_str(), last_sample.c_str() + std::min(last_sample.size() - 1, strlen(last_sample.c_str())), _null_values, _delimiter, _quotechar);
//                    } else {
//                        logger.warn("could not find CSV line start in last rows sample.");
//                    }
//                }
//            }
//
//            // @TODO: could draw also additional random sample from data!

        } else {
            logger.warn("no input files found, can't infer type from given path: " + pattern);
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
        }
        _sampling_time_s += timer.time();
    }

    FileInputOperator *FileInputOperator::fromOrc(const std::string &pattern, const ContextOptions &co, const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, sampling_mode);
    }

    FileInputOperator::FileInputOperator(const std::string &pattern, const ContextOptions &co, const SamplingMode& sampling_mode): _sampling_time_s(0.0), _samplingMode(normalizeSamplingMode(sampling_mode)), _samplingSize(co.SAMPLE_SIZE()), _cachePopulated(false), _estimatedRowCount(0) {

#ifdef BUILD_WITH_ORC
        auto &logger = Logger::instance().logger("fileinputoperator");
        _fmt = FileFormat::OUTFMT_ORC;
        Timer timer;
        detectFiles(pattern);

        if (!_fileURIs.empty()) {
            auto uri = _fileURIs.front();

            using namespace ::orc;
            auto inStream = std::make_unique<orc::VirtualInputStream>(uri);
            ReaderOptions options;
            ORC_UNIQUE_PTR<Reader> reader = createReader(std::move(inStream), options);

            auto &orcType = reader->getType();
            auto numRows = reader->getNumberOfRows();
            std::vector<bool> columnHasNull;
            for (int i = 0; i < orcType.getSubtypeCount(); ++i) {
                columnHasNull.push_back(reader->getColumnStatistics(i + 1)->hasNull());
            }
            auto tuplexType = tuplex::orc::orcRowTypeToTuplex(orcType, columnHasNull);

            _normalCaseRowType = tuplexType;
            _generalCaseRowType = tuplexType;
            _estimatedRowCount = numRows * _fileURIs.size();

            bool hasColumnNames = false;
            for (uint64_t i = 0; i < orcType.getSubtypeCount(); ++i) {
                const auto& name = orcType.getFieldName(i);
                if (!name.empty()) {
                    hasColumnNames = true;
                }
                _columnNames.push_back(name);
            }
            if (!hasColumnNames) {
                _columnNames.clear();
            }

            inStream.reset();
            reader.reset();

            setOutputSchema(Schema(Schema::MemoryLayout::ROW, tuplexType));

            setProjectionDefaults();
        } else {
            logger.warn("no input files found, can't infer type from sample.");
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
        }
        _sampling_time_s += timer.time();
#else
        throw std::runtime_error(MISSING_ORC_MESSAGE);
#endif
    }

    void FileInputOperator::setProjectionDefaults() {// set optimized schema to current one
//        // set optimized schema to current one
//        _optimizedSchema = getInputSchema();
//        _optimizedColumnNames = _columnNames;
        // create per default true array, i.e. serialize all columns
        _columnsToSerialize = std::vector<bool>();

        for (int i = 0; i < inputColumnCount(); ++i)
            _columnsToSerialize.push_back(true);

//        _optimizedNormalCaseRowType = _normalCaseRowType;
    }

    void FileInputOperator::setRowsSample(const std::vector<Row> &sample, bool is_projected) {
        // remove cached data and replace
        _rowsSample = sample;
        _isRowSampleProjected = is_projected;

        // remove file cache
        _sampleCache.clear();
    }

    std::vector<Row> FileInputOperator::getSample(const size_t num) const {
        if(!_cachePopulated)
            const_cast<FileInputOperator*>(this)->fillFileCache(_samplingMode);
        if(_rowsSample.empty())
            const_cast<FileInputOperator*>(this)->fillRowCache(_samplingMode);

        if(!_cachePopulated || _rowsSample.empty()) {
            // empty -> silent warning b.c. hyperspecialize triggers this.
            return {};
        }


        // restrict to num
        if(num <= _rowsSample.size()) {
            // select num random rows...
            // use https://en.wikipedia.org/wiki/Reservoir_sampling the optimal algorithm there to fetch the sample quickly...
            // i.e., https://www.geeksforgeeks.org/reservoir-sampling/
            if(!_isRowSampleProjected)
                return projectSample(randomSampleFromReservoir(_rowsSample, num));
            else return randomSampleFromReservoir(_rowsSample, num);
        } else {
            // need to increase sample size!
            Logger::instance().defaultLogger().warn("requested " + std::to_string(num)
                                                    + " rows for sampling, but only "
                                                    + std::to_string(_rowsSample.size())
                                                    + " stored. Consider decreasing sample size. Returning all available rows.");
            if(!_isRowSampleProjected)
                return projectSample(_rowsSample);
            else
                return _rowsSample;
        }
    }

    std::vector<Row> FileInputOperator::projectSample(const std::vector<Row>& rows) const {
        if(rows.empty())
            return {};

        // trivial case? no projection?
        if(!usesProjectionMap())
            return rows;

        // projection is used (i.e. at least one of columnsToSerialize is false...)
        std::vector<Row> v;
        std::vector<Field> fields;
        for(const auto& row : rows) {
            fields.clear();
            for(unsigned i = 0; i < std::min(row.getNumColumns(), _columnsToSerialize.size()); ++i) {
                if(_columnsToSerialize[i]) {
                    fields.push_back(row.get(i));
                }
            }
            v.push_back(Row::from_vector(fields));
        }

        return v;
    }

    std::vector<size_t> FileInputOperator::translateOutputToInputIndices(const std::vector<size_t> &output_indices) {
        using namespace std;
        vector<size_t> indices;

        // create map

        unordered_map<size_t, size_t> m; // output index -> original index
        size_t pos = 0;
        for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
            if(_columnsToSerialize[i]) {
                m[pos++] = i;
            }
        }

        // make sure this works with current output schema!
        if(m.size() != outputColumnCount()) {
            throw std::runtime_error("INTERNAL ERROR: invalid pushdown map created.");
        }

        // go through output indices
        for(auto idx : output_indices) {
            auto it = m.find(idx);
            if(it == m.end()) {
                // invalid index
                throw std::runtime_error("INTERNAL ERROR: invalid index to select from given. Index is "
                + to_string(idx) + " but allowed range is 0-"+ to_string(pos));
            } else {
                indices.push_back(it->second); // push back original index
            }
        }

        return indices;
    }

    void FileInputOperator::selectColumns(const std::vector<std::string>& columns) {
        using namespace std;

        auto& logger = Logger::instance().logger("logical");

        if(columns.empty()) {
            auto columns_before = this->columns();
            _columnsToSerialize = vector<bool>(_columnsToSerialize.size(), false);

            stringstream ss;
            ss<<"Columns before selectColumns: "<<columns_before<<"\nColumns after  selectColumns: "<<this->columns();
            logger.debug(ss.str());

            assert(extract_columns_from_type(getOutputSchema().getRowType()) == columns.size());

            return;
        }

        // check that all columns are actually contained.
        std::vector<int> indices;
        std::vector<std::string> names_not_found;
        for(const auto& name : columns) {
            auto idx = indexInVector(name, this->_columnNames);
            indices.push_back(idx);
            if(idx < 0)
                names_not_found.push_back(name);
        }

        if(!names_not_found.empty()) {
            stringstream ss;
            ss << "Can not select columns " << names_not_found << " from " << name()
               << " operator, because columns are not contained.";
            throw std::runtime_error(ss.str());
        }

        // reset columns to serialize, and print out result
        auto columns_before = this->columns();
        _columnsToSerialize = vector<bool>(_columnsToSerialize.size(), false);

        for(auto idx : indices)
            _columnsToSerialize[idx] = true;

        stringstream ss;
        ss<<"Columns before selectColumns: "<<columns_before<<"\nColumns after  selectColumns: "<<this->columns();
        logger.debug(ss.str());

        assert(extract_columns_from_type(getOutputSchema().getRowType()) == columns.size());
    }

    void FileInputOperator::selectColumns(const std::vector<size_t> &columnsToSerialize, bool original_indices) {
        using namespace std;

#ifndef NDEBUG
        {
            // debug print:
            std::stringstream ss;
            ss<<"file input operator projection::\n";
            ss<<"columns before projection pushdown: "<<columns()<<"\n";

            // which columns to keep?
            std::vector<std::string> new_cols;
            for(auto idx : columnsToSerialize) {
                auto original_idx = original_indices ? idx : reverseProjectToReadIndex(idx);
                if(original_idx < inputColumns().size()) {
                    new_cols.push_back(inputColumns()[original_idx]);
                } else {
                    new_cols.push_back("<< INVALID INDEX>>");
                }
            }
            ss<<"columns after projection pushdown: "<<new_cols<<"\n";
            auto& logger = Logger::instance().logger("logical");
            logger.debug(ss.str());
        }
#endif


        // are indices relative to original columns or to currently pushed down columns?
        auto original_col_indices_to_serialize = columnsToSerialize;
        // if so, translate them!
        if(!original_indices)
            original_col_indices_to_serialize = translateOutputToInputIndices(columnsToSerialize);

        // reset to defaults
        setProjectionDefaults();

        auto schema = getInputSchema();
        auto rowType = schema.getRowType();

        if(!PARAM_USE_ROW_TYPE || !schema.getRowType().isRowType()) {
            assert(schema.getRowType().isTupleType());
            assert(schema.getRowType().parameters().size() == inputColumnCount());
        } else {
            //assert(schema.getRowType().isRowType());
        }

        // set internal (original) column indices to serialize to false
        for (int i = 0; i < _columnsToSerialize.size(); ++i)
            _columnsToSerialize[i] = false;

        for (auto idx : original_col_indices_to_serialize) {
            if(!PARAM_USE_ROW_TYPE)
                assert(idx < rowType.parameters().size());

            // set to true for idx
            _columnsToSerialize[idx] = true;
        }
    }

    std::unordered_map<int, int> FileInputOperator::projectionMap() const {
        // check if all are positive
        bool all_true = true;
        for(auto keep_col : columnsToSerialize())
            if(!keep_col) {
                all_true = false;
            }

        // trivial case, no map necessary. full row is serialized...
        if(all_true)
            return std::unordered_map<int, int>();

        // mapping case
        std::unordered_map<int, int> m;
        int pos = 0;
        for(int i = 0; i < columnsToSerialize().size(); ++i) {
            if(columnsToSerialize()[i]) {
                m[i] = pos++;
            }
        }
        return m;
    }

    void FileInputOperator::setColumns(const std::vector<std::string> &columnNames) {
        if(_fileURIs.empty())
            return;

        if(_columnNames.empty()) {
            if(getOutputSchema() != Schema::UNKNOWN && columnNames.size() != inputColumnCount())
                throw std::runtime_error("number of columns given (" + std::to_string(columnNames.size()) +
                                         ") does not match detected count (" + std::to_string(inputColumnCount()) + ")");
            _columnNames = columnNames;
            return;
        }

        // check whether it's a match for input Column Count (i.e. full replace) or output column count (projected replace)
        if(columnNames.size() == inputColumnCount()) {
            _columnNames = columnNames;
        } else {
            // must match
            if (columnNames.size() != outputColumnCount())
                throw std::runtime_error("number of columns given (" + std::to_string(columnNames.size()) +
                                         ") does not match projected column count (" + std::to_string(outputColumnCount()) + ")");

            assert(_columnNames.size() == _columnsToSerialize.size());

            // replace columns for kept columns
            unsigned pos = 0;
            for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
                if(pos < columnNames.size() && _columnsToSerialize[i]) {
                    _columnNames[i] = columnNames[pos++];
                }
            }
        }
    }

    std::shared_ptr<LogicalOperator> FileInputOperator::clone(bool cloneParents) const {
        // make here copy a bit more efficient, i.e. avoid resampling files by using specialized copy constructor (private)
        auto copy = new FileInputOperator(*this); // no children, no parents so all good with this method
        assert(checkBasicEqualityOfOperators(*copy, *this));
        return std::shared_ptr<LogicalOperator>(copy);
    }

    FileInputOperator::FileInputOperator(const tuplex::FileInputOperator &other) : _fileURIs(other._fileURIs),
                                                                                   _sizes(other._sizes),
                                                                                   _estimatedRowCount(other._estimatedRowCount),
                                                                                   _fmt(other._fmt),
                                                                                   _quotechar(other._quotechar),
                                                                                   _delimiter(other._delimiter),
                                                                                   _header(other._header),
                                                                                   _null_values(other._null_values),
                                                                                   _columnsToSerialize(other._columnsToSerialize),
                                                                                   _columnNames(other._columnNames),
                                                                                   _normalCaseRowType(other._normalCaseRowType),
                                                                                   _generalCaseRowType(other._generalCaseRowType),
                                                                                   _indexBasedHints(other._indexBasedHints),
                                                                                   _rowsSample(other._rowsSample),
                                                                                   _isRowSampleProjected(other._isRowSampleProjected),
                                                                                   _cachePopulated(other._cachePopulated),
                                                                                   _samplingMode(other._samplingMode),
                                                                                   _sampling_time_s(other._sampling_time_s),
                                                                                   _samplingSize(other._samplingSize),
                                                                                   _json_unwrap_first_level(other._json_unwrap_first_level),
                                                                                   _json_treat_heterogeneous_lists_as_tuples(other._json_treat_heterogeneous_lists_as_tuples),
                                                                                   _json_use_generic_dicts(other._json_use_generic_dicts) {
        // copy members for logical operator
        LogicalOperator::copyMembers(&other);
        LogicalOperator::setDataSet(other.getDataSet());

        // copy cache (?)
        for (const auto& kv: other._sampleCache) { _sampleCache[kv.first] = kv.second; }
    }

    void FileInputOperator::cloneCaches(const tuplex::FileInputOperator &other) {
        std::lock_guard<std::mutex> lock(_sampleCacheMutex);

        _cachePopulated = other._cachePopulated;

        // clear internal caches
        _sampleCache.clear();
        _rowsSample.clear();

        _sampleCache = other._sampleCache;
        _rowsSample = other._rowsSample;
        _isRowSampleProjected = other._isRowSampleProjected;
    }

    int64_t FileInputOperator::cost() const {
        // use number of input rows as cost
        return _estimatedRowCount;
    }

    bool FileInputOperator::isEmpty() const {
        if(_fileURIs.empty() || _sizes.empty())
            return true;

        size_t totalSize = 0;
        for(auto s: _sizes)
            totalSize += s;
        return totalSize == 0;
    }

    template<typename T> std::vector<T> limit_vector(const std::vector<T>& v, size_t limit) {
        return std::vector<T>(v.begin(), v.begin() + limit);
    }

    SamplingMode optimize_sampling_mode(const SamplingMode& sm, unsigned num_uris=std::numeric_limits<unsigned>::max()) {
        SamplingMode opt_sm = sm;

        // how many files are sampled?
        unsigned num_files = 0;
        num_files += sm & SamplingMode::FIRST_FILE;
        num_files += sm & SamplingMode::LAST_FILE;
        num_files += 10 * (sm & SamplingMode::ALL_FILES);
        num_files += sm & SamplingMode::RANDOM_FILE;

        // if a single file is sampled then use single-threaded mode. Not worth to spawn threads
        if(num_files <= 1) {
            opt_sm = opt_sm ^ SamplingMode::MULTITHREADED;
            opt_sm = opt_sm | SamplingMode::SINGLETHREADED;
        }

        // how many uris are actually given?
        if(num_uris <= 1) {
            // only single-threaded makes sense
            opt_sm = opt_sm ^ SamplingMode::MULTITHREADED;
            opt_sm = opt_sm | SamplingMode::SINGLETHREADED;
        }

        return opt_sm;
    }

    // HACK !!!!
    void FileInputOperator::setInputFiles(const std::vector<URI> &uris, const std::vector<size_t> &uri_sizes,
                                          bool resample, size_t sample_limit,
                                          bool use_stratified_sampling,
                                          size_t strata_size,
                                          size_t samples_per_strata,
                                          int random_seed) {
        auto& logger = Logger::instance().logger("logical");

        assert(uris.size() == uri_sizes.size());
        _fileURIs = uris;
        _sizes = uri_sizes;

        // for JSON files, it could happen that column names change!
        // -> need to account for that, i.e. put into rewrite map
        auto cols_to_serialize_before_resample = columnsToSerialize();
        auto input_cols_before_resample = inputColumns();
        auto cols_before_resample = columns();

        if(resample && !_fileURIs.empty()) {
            // clear sample cache
            _cachePopulated = false;
            _sampleCache.clear();
            _rowsSample.clear(); // reset row samples!
            _isRowSampleProjected = false;

            // restrict sampling mode
            auto sm = optimize_sampling_mode(_samplingMode, uris.size());

            fillFileCache(sm);

            // if JSON -> resort rows
            if(FileFormat::OUTFMT_JSON == _fmt) {
                ContextOptions co = ContextOptions::defaults();
                // manipulate settings...
                std::vector<std::vector<std::string>> namesCollection;
                std::vector<std::vector<std::string>>* namePtr = _json_unwrap_first_level ? &namesCollection : nullptr;
                if(!use_stratified_sampling) {
                    // fill row cache
                    fillRowCache(sm, namePtr, sample_limit);
                } else {
                    fillRowCacheWithStratifiedSamples(sm, namePtr, sample_limit, strata_size, samples_per_strata, random_seed);
                }


                // Quick check: Empty sample? Then warn and return.
                if(estimateSampleBasedRowCount() == 0) {
                    std::stringstream ss;
                    ss<<"Could not retrieve sample from "<<_fileURIs<<". Skipping resample/retype step for "<<name();
                    logger.warn(ss.str());
                    return;
                }

                // detect new type
                auto t = detectJsonTypesAndColumns(co, namesCollection);

                auto normalcase = std::get<0>(t);
                auto generalcase = std::get<1>(t);

                // make even more general
                generalcase = generalize_type(generalcase);

                // special case, no unwrap -> remove inner option.
                if(!_json_unwrap_first_level) {
                    if(generalcase.isTupleType())
                        generalcase = python::Type::makeTupleType(std::vector<python::Type>({generalcase.parameters().front().withoutOption()}));
                    else
                        generalcase = generalcase.withoutOption();
                }

                // create row type from input columns
                if(PARAM_USE_ROW_TYPE) {
                    normalcase = python::Type::makeRowType(normalcase.parameters(), inputColumns());
                    generalcase = python::Type::makeRowType(generalcase.parameters(), inputColumns());
                }

                _normalCaseRowType = normalcase;
                _generalCaseRowType = generalcase;
            } else {
                if(!use_stratified_sampling)
                    fillRowCache(sm, nullptr, sample_limit);
                else
                    fillRowCacheWithStratifiedSamples(sm, nullptr, sample_limit, strata_size, samples_per_strata, random_seed);
            }

            _estimatedRowCount = estimateSampleBasedRowCount();
            // Quick check: Empty sample? Then warn and return.
            if(_estimatedRowCount == 0) {
                std::stringstream ss;
                ss<<"Could not retrieve sample from "<<_fileURIs<<". Skipping resample/retype step for "<<name();
                logger.warn(ss.str());
                return;
            }

            // correct for if columns are different
            if(!vec_equal(inputColumns(), cols_before_resample)) {
                // correct for this
                logger.debug("found file with different order or different columns. Need to correct for this.");

                // need to update rewriteMap (projection)

                // which of the new columns are found in the old type AFTER pushdown? -> save the indices.
                std::vector<int> indices_of_columns_before_resample;
                for(unsigned i = 0; i < inputColumns().size(); ++i) {
                    auto name = inputColumns()[i];
                    auto idx = indexInVector(name, cols_before_resample);
                    if(idx >= 0) {
                        indices_of_columns_before_resample.push_back(idx);
                    }
                }

                // now create (unprojected) type based on new columns
                std::vector<python::Type> new_unprojected_col_types_normal = _normalCaseRowType.isRowType() ? _normalCaseRowType.get_column_types() : _normalCaseRowType.parameters();
                std::vector<python::Type> new_unprojected_col_types_general = _generalCaseRowType.isRowType() ? _generalCaseRowType.get_column_types() : _generalCaseRowType.parameters();
                std::vector<std::string>  new_unprojected_col_names = inputColumns();

                python::Type exception_type = python::TypeFactory::instance().getByName("Exception");

                std::unordered_map<int, int> oldIndexToNewIndex;

                std::vector<int> indices_to_keep;

                // all the old rows that are not present anymore are automatically exceptions (IndexError?)
                size_t num_columns_not_present_anymore = 0;
                for(unsigned i = 0; i < cols_before_resample.size(); ++i) {

                    // is a column of name present in current columns?
                    auto name = cols_before_resample[i];
                    auto idx = indexInVector(name, new_unprojected_col_names);

                    if(idx >= 0) {
                        // mark as being serialized!
                        indices_to_keep.push_back(idx);
                    } else {
                        // old column not found in new columns -> add and mark as exception.
                        // update
                        auto last_idx = new_unprojected_col_types_normal.size();
                        oldIndexToNewIndex[i] = last_idx; // current size...

                        new_unprojected_col_names.push_back(cols_before_resample[i]);
                        new_unprojected_col_types_normal.push_back(exception_type);
                        new_unprojected_col_types_general.push_back(exception_type);

                        // mark this row as being serialized? yes.
                        indices_to_keep.push_back(last_idx);
                        num_columns_not_present_anymore++;
                    }
                }

                // mark any new columns as being serialized (automatically)
                size_t num_new_columns = 0;
                for(unsigned i = 0; i < inputColumns().size(); ++i) {
                    auto new_name = inputColumns()[i];
                    // found in old input columns? -> ignore. --> case already handled above
                    // if not found! mark!
                    auto idx = indexInVector(new_name, input_cols_before_resample);
                    if(idx < 0) {
                        indices_to_keep.push_back(i);
                        num_new_columns++;
                    }
                }

                logger.debug("Discovered " + pluralize(num_new_columns, "new column") + " during resampling, " + pluralize(num_columns_not_present_anymore, "column") + " not present in this file anymore.");

                // unprojected is now ok. -> need to update projection map accordingly.
                auto num_columns = new_unprojected_col_types_general.size();
                logger.debug("Parsing in total " + pluralize(num_columns, "input column") + " of which " + pluralize(indices_to_keep.size(), "column") + " are serialized.");
                _columnsToSerialize = std::vector<bool>(num_columns, false); // <-- all per default false, then add indices to keep!
                assert(!indices_to_keep.empty()); // dummy check?

                // go through indices
                for(auto idx : indices_to_keep) {
                    _columnsToSerialize[idx] = true;
                }


                // restrict to actual found count.
                auto num_found = inputColumns().size();
                _columnsToSerialize = limit_vector(_columnsToSerialize, num_found);
                new_unprojected_col_types_general = limit_vector(new_unprojected_col_types_general, num_found);
                new_unprojected_col_types_normal = limit_vector(new_unprojected_col_types_normal, num_found);

                // list which columns are now output
                auto new_projected_columns = columns();

                // update types
                _normalCaseRowType = python::Type::makeTupleType(new_unprojected_col_types_normal);
                _generalCaseRowType = python::Type::makeTupleType(new_unprojected_col_types_general);

                // if using row type, update
                if(PARAM_USE_ROW_TYPE) {
                    _normalCaseRowType = python::Type::makeRowType(_normalCaseRowType.parameters(), inputColumns());
                    _generalCaseRowType = python::Type::makeRowType(_generalCaseRowType.parameters(), inputColumns());
                }
            }

            setOutputSchema(Schema(Schema::MemoryLayout::ROW, _generalCaseRowType));
        }

        // check for flag that proper row type is used (not tuple type)
        if(PARAM_USE_ROW_TYPE) {
            assert(_normalCaseRowType.isRowType());
            assert(_generalCaseRowType.isRowType());
        }
    }

    bool FileInputOperator::retype(const RetypeConfiguration& conf, bool ignore_check_for_str_option) {

        auto input_row_type = conf.row_type;
        bool is_projected_row_type = conf.is_projected;

        // simpler way, create lookup maps (assume columns are always existing. Need to fix else).
        std::set<std::string> all_column_names;
        std::unordered_map<std::string, python::Type> conf_name_to_type_map;
        std::unordered_map<std::string, python::Type> this_name_to_type_map;
        for(unsigned i = 0; i < conf.columns.size(); ++i) {
            auto name = conf.columns[i];
            auto col_type = conf.row_type.isRowType() ? conf.row_type.get_column_type(name) : conf.row_type.parameters()[i];
            all_column_names.insert(name);
            conf_name_to_type_map[name] = col_type;
        }

        for(unsigned i = 0; i < columns().size(); ++i) {
            auto name = columns()[i];
            auto col_type = normalCaseSchema().getRowType().isRowType() ? normalCaseSchema().getRowType().get_column_type(name) : normalCaseSchema().getRowType().parameters()[i];
            all_column_names.insert(name);
            this_name_to_type_map[name] = col_type;
        }

        // debug:
#ifndef NDEBUG
        {
            auto& logger = Logger::instance().logger("codegen");


            // now check by printing everything:
            std::stringstream ss;
            ss<<"Performing file operator retype with:\n";
            for(auto name : all_column_names) {
                auto current_type = this_name_to_type_map.find(name) != this_name_to_type_map.end() ? this_name_to_type_map[name].desc() : "";
                auto new_type = conf_name_to_type_map.find(name) != conf_name_to_type_map.end() ? conf_name_to_type_map[name].desc() : "";
                ss<<"name= "<<name<<" current= "<<current_type<<" new= "<<new_type<<"\n";
            }
            logger.debug(ss.str());
        }
#endif


        // check that #columns corresponds to number of col types!
        if(!conf.columns.empty()) {
            assert(input_row_type.isTupleType());
            assert(input_row_type.parameters().size() == conf.columns.size());
        }

        assert(input_row_type.isTupleType());
        auto col_types = input_row_type.parameters();
        auto col_names = conf.columns;

        // check this is valid
        assert(!col_names.empty() && col_types.size() == col_names.size());

        auto old_col_types = normalCaseSchema().getRowType().isRowType() ? normalCaseSchema().getRowType().get_column_types() : normalCaseSchema().getRowType().parameters();
        auto old_general_col_types = getOutputSchema().getRowType().isRowType() ? getOutputSchema().getRowType().get_column_types() : getOutputSchema().getRowType().parameters();

        size_t num_projected_columns = getOptimizedOutputSchema().getRowType().isRowType() ? getOptimizedOutputSchema().getRowType().get_column_count() : getOptimizedOutputSchema().getRowType().parameters().size();

        // assert(old_col_types.size() <= old_general_col_types.size());
        auto& logger = Logger::instance().logger("codegen");

        // check whether number of columns are compatible (necessary when no concrete columns are given)
        // if this fails, need to update projected columns first!
        if(conf.columns.size() != projectColumns(inputColumns()).size()) {
            if (is_projected_row_type) {
                if (col_types.size() != num_projected_columns) {
                    logger.error("Provided incompatible (projected) rowtype to retype " + name() +
                                 ", provided type has " + pluralize(col_types.size(), "column") +
                                 " but optimized schema in operator has "
                                 + pluralize(old_col_types.size(), "column"));
                    return false;
                }
            } else {
                if (col_types.size() != old_col_types.size()) {
                    logger.error("Provided incompatible rowtype to retype " + name() +
                                 ", provided type has " + pluralize(col_types.size(), "column") +
                                 " but optimized schema in operator has "
                                 + pluralize(old_col_types.size(), "column"));
                    return false;
                }
            }
        }


        auto str_opt_type = python::Type::makeOptionType(python::Type::STRING);

        // go over and check they're compatible!
        for(unsigned i = 0; i < col_types.size(); ++i) {
            auto t = col_types[i];
            if(col_types[i].isConstantValued())
                t = t.underlying();

            // old type
            unsigned lookup_index = i;

            if(!is_projected_row_type) {
                lookup_index = reverseProjectToReadIndex(i);
            }

            assert(lookup_index < old_general_col_types.size());

            // TODO: why is this block here needed? => always use new types.
            // if(!python::canUpcastType(t, old_general_col_types[lookup_index])) {
            //     // both struct types? => replace, can have different format. that's ok
            //     if((t.withoutOption().isSparseStructuredDictionaryType() || t.withoutOption().isStructuredDictionaryType()) &&
            //             (old_general_col_types[lookup_index].withoutOption().isStructuredDictionaryType() || old_general_col_types[lookup_index].withoutOption().isSparseStructuredDictionaryType())) {
            //         logger.debug("encountered struct dict with different structure at index " + std::to_string(i));
            //     } else {
            //         if(!(ignore_check_for_str_option && old_general_col_types[lookup_index] == str_opt_type)) {
            //             logger.warn("provided specialized type " + col_types[i].desc() + " can't be upcast to "
            //                         + old_general_col_types[lookup_index].desc() + ", ignoring in retype.");
            //             assert(lookup_index < old_col_types.size());
            //             col_types[i] = old_col_types[lookup_index];
            //         }
            //     }
            // }
        }

        // type hints have precedence over sampling! I.e., include them here!
        for(const auto& kv : typeHints()) {
            auto idx = kv.first;

            auto name = inputColumns()[idx];

            // this here is buggy, use instead name search.
            // // are we having a projected type? then lookup in projection map!
            // auto m = projectionMap();
            // if(is_projected_row_type && !m.empty())
            //     idx = m[idx]; // get projected index...

            idx = indexInVector(name, col_names);

            if(idx >= 0 && idx < col_types.size()) {
                col_types[idx] = kv.second;
            } else if(idx < 0)
                logger.error("internal error, invalid type hint (with wrong index?)");
        }

        // retype optimized schema!
        // -> this requires reprojection if possible!
        if(is_projected_row_type) {
            // set normal-case type with updated projected fields
            auto nc_col_types = _normalCaseRowType.isRowType() ? _normalCaseRowType.get_column_types() : _normalCaseRowType.parameters();
            auto m = projectionMap();
            for(auto kv : m) {
                assert(kv.second < col_types.size());
                nc_col_types[kv.first] = col_types[kv.second];
            }

            // special case: empty map & match -> i.e., default mapping.
            if(m.empty() && nc_col_types.size() == col_types.size())
                nc_col_types = col_types;

            if(PARAM_USE_ROW_TYPE)
                _normalCaseRowType = python::Type::makeRowType(nc_col_types, _normalCaseRowType.get_column_names());
            else
                _normalCaseRowType = python::Type::makeTupleType(nc_col_types);
        } else {
            if(PARAM_USE_ROW_TYPE)
                _normalCaseRowType = python::Type::makeRowType(col_types, _normalCaseRowType.get_column_names());
            else
                _normalCaseRowType = python::Type::makeTupleType(col_types);
        }

        return true;
    }

    bool FileInputOperator::retype(const RetypeConfiguration& conf) {
        return retype(conf, false);
    }

    std::vector<Row> FileInputOperator::sample(const SamplingMode& mode,
                                               std::vector<std::vector<std::string>>* outNames,
                                               const SamplingParameters& sampling_params) {
        auto& logger = Logger::instance().logger("logical");
        if(_fileURIs.empty())
            return {};

        Timer timer;
        std::vector<Row> v;

        // TODO: open questions -> limit per file? or total limit?
        // -> could configure that. It would affect sample quality...

        auto sample_limit = sampling_params.limit;

        // check sampling mode, i.e. how files should be sample?
        std::set<unsigned> sampled_file_indices;
        if(mode & SamplingMode::ALL_FILES) {
            assert(_fileURIs.size() == _sizes.size());
            for(unsigned i = 0; i < _fileURIs.size() && v.size() < sample_limit; ++i) {
                auto s = sampleFile(_fileURIs[i], _sizes[i], mode, outNames, sampling_params);
                std::copy(s.begin(), s.end(), std::back_inserter(v));
                sampled_file_indices.insert(i);
            }
        }

        // sample the first file present?
        if(mode & SamplingMode::FIRST_FILE
            && sampled_file_indices.find(0) == sampled_file_indices.end()
            && v.size() < sample_limit) {
            auto s = sampleFile(_fileURIs.front(), _sizes.front(), mode, outNames, sampling_params);
            std::copy(s.begin(), s.end(), std::back_inserter(v));
            sampled_file_indices.insert(0);
        }

        // sample the last file present?
        if(mode & SamplingMode::LAST_FILE
            && sampled_file_indices.find(_fileURIs.size() - 1) == sampled_file_indices.end()
            && v.size() < sample_limit) {
            auto s = sampleFile(_fileURIs.back(), _sizes.back(), mode, outNames, sampling_params);
            std::copy(s.begin(), s.end(), std::back_inserter(v));
            sampled_file_indices.insert(_fileURIs.size() - 1);
        }

        // sample a random file?
        if(mode & SamplingMode::RANDOM_FILE
            && sampled_file_indices.size() < _fileURIs.size()
            && v.size() < sample_limit) {
            // form set of valid indices
            std::vector<unsigned> valid_indices;
            for(unsigned i = 0; i < _fileURIs.size(); ++i)
                if(sampled_file_indices.find(i) == sampled_file_indices.end())
                    valid_indices.push_back(i);
            assert(!valid_indices.empty());
            auto random_idx = valid_indices[randi(0ul, valid_indices.size() - 1)];
            logger.debug("Sampling file (random) idx=" + std::to_string(random_idx));
            auto s = sampleFile(_fileURIs[random_idx], _sizes[random_idx], mode, outNames, sampling_params);
            std::copy(s.begin(), s.end(), std::back_inserter(v));
            sampled_file_indices.insert(random_idx);
        }

        // restrict sample
        if(v.size() > sample_limit) {
            v = std::vector<Row>(v.begin(), v.begin() + sample_limit);
        }

        assert(v.size() <= sample_limit);
        logger.debug("Sampling (mode=" + std::to_string(mode) + ") took " + std::to_string(timer.time()) + "s, ("  + pluralize(v.size(), "sample") + ").");
        return v;
    }

    std::vector<Row> FileInputOperator::sampleCSVFile(const URI& uri, size_t uri_size, const SamplingMode& mode, const SamplingParameters& sampling_params) {
        auto& logger = Logger::instance().logger("logical");
        std::vector<Row> v;
        assert(mode & SamplingMode::FIRST_ROWS || mode & SamplingMode::LAST_ROWS || mode & SamplingMode::RANDOM_ROWS);
        Timer timer;

        if(0 == uri_size || uri == URI::INVALID) {
            logger.debug("empty file, can't obtain sample from it");
            return {};
        }

        auto sample_limit = sampling_params.limit;

        // decode range based uri (and adjust seeking accordingly!)
        // is the URI range based?
        size_t range_start = 0, range_end = 0;
        size_t range_size = 0;
        URI target_uri;
        decodeRangeURI(uri.toString(), target_uri, range_start, range_end);

        size_t original_sample_limit = sample_limit;
        size_t mode_count = 0;
        mode_count += (mode & SamplingMode::FIRST_ROWS) > 0;
        mode_count += (mode & SamplingMode::LAST_ROWS) > 0;
        mode_count += (mode & SamplingMode::RANDOM_ROWS) > 0;

        sample_limit = std::max(mode_count, sample_limit);
        size_t per_mode_limit = sample_limit / mode_count;

        // both 0? no restriction
        if(range_start == 0 && range_end == 0) {
            range_end = uri_size;
            range_size = uri_size;
        } else {
            // restrict!
            range_size = range_end - range_start;
        }

        // reuse old column count for estimation. -> else need to start CSV estimation from scratch!
        auto expectedColumnCount = std::max(_columnNames.size(), _columnsToSerialize.size());
        if(expectedColumnCount == 0)
            expectedColumnCount = 1;

        if(0 == range_size || target_uri == URI::INVALID)
            return {};


        SamplingMode m = mode;
        size_t sampling_size = std::min(range_size, _samplingSize);

        // if uri_size < file_size -> first rows only
        if(uri_size <= sampling_size)
            m = SamplingMode::FIRST_ROWS;

        // check file sampling modes & then load the samples accordingly
        if(m & SamplingMode::FIRST_ROWS) {
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::FIRST_ROWS, true);

            auto limit = std::min(sample_limit, per_mode_limit);

            if(sampling_params.use_stratified_sampling()) {
                std::set<unsigned> skip_rows;
                if((0 == range_start && _header) || 0 != range_start)
                    skip_rows.insert(0);
                v = csv_parseRowsStratified(sample.c_str(), sample.size(),
                                            expectedColumnCount, range_start, _delimiter,
                                            _quotechar, _null_values, limit,
                                            sampling_params.strata_size, sampling_params.samples_per_strata,
                                            sampling_params.random_seed, skip_rows);
            } else {
                // parse as rows using the settings detected.
                v = csv_parseRows(sample.c_str(), sample.size(), expectedColumnCount,
                                  range_start, _delimiter, _quotechar, _null_values, limit);
                // header? ignore first row!
                if(0 == range_start && _header && !v.empty())
                    v.erase(v.begin());
            }

            sample_limit -= std::min(v.size(), limit);
        }

        if(m & SamplingMode::LAST_ROWS) {
            // the smaller of remaining and sample size!
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::LAST_ROWS, true, &file_offset);
            size_t offset = 0;
            if(!v.empty()) {
                if(uri_size < 2 * sampling_size) {
                    offset = sampling_size - (uri_size - sampling_size);
                    assert(offset <= sampling_size);
                }
                assert(offset <= sample.size());
                auto sample_global_offset = file_offset + offset;
                auto limit = std::min(sample_limit, per_mode_limit);
                std::vector<Row> rows;
                if(sampling_params.use_stratified_sampling()) {
                    std::set<unsigned> skip_rows;
                    if(0 != sample_global_offset)
                        skip_rows.insert(0);
                    rows = csv_parseRowsStratified(sample.c_str(), sample.size(),
                                            expectedColumnCount, range_start, _delimiter,
                                            _quotechar, _null_values, limit,
                                            sampling_params.strata_size, sampling_params.samples_per_strata,
                                            sampling_params.random_seed, skip_rows);
                } else {
                    rows = csv_parseRows(sample.c_str() + offset, sample.size() - offset, expectedColumnCount,
                                         sample_global_offset, _delimiter, _quotechar, _null_values, limit);
                    // offset = 0?
                    if(file_offset == 0) {
                        // header? ignore first row!
                        if(_header && !rows.empty())
                            rows.erase(rows.begin());
                    } else {
                        // always erase first row, b.c. could be faulty.
                        // I.e., let's say first column is 1999 but by chance the read starts from 99. Then this would screw up estimation.
                        rows.erase(rows.begin());
                    }
                }

                sample_limit -= std::min(v.size(), std::min(limit, rows.size()));
                std::copy(rows.begin(), rows.end(), std::back_inserter(v));

            } else {
                if(sampling_params.use_stratified_sampling()) {
                    auto limit = std::min(sample_limit, per_mode_limit);

                    v = csv_parseRowsStratified(sample.c_str(), sample.size(),
                                            expectedColumnCount, file_offset, _delimiter,
                                            _quotechar, _null_values, limit,
                                            sampling_params.strata_size, sampling_params.samples_per_strata,
                                            sampling_params.random_seed, {0});
                    sample_limit -= std::min(v.size(), limit);
                } else {
                    auto limit = std::min(sample_limit, per_mode_limit);
                    v = csv_parseRows(sample.c_str(), sample.size(), expectedColumnCount, file_offset,
                                      _delimiter, _quotechar, _null_values, limit);
                    sample_limit -= std::min(v.size(), limit);

                    if(!v.empty()) {
                        // offset = 0?
                        if(file_offset == 0) {
                            // header? ignore first row!
                            if(_header)
                                v.erase(v.begin());
                        } else {
                            v.erase(v.begin());
                        }
                    }
                }
            }
        }

        // most complicated: random -> make sure no overlap with first/last rows
        if(m & SamplingMode::RANDOM_ROWS) {

            if(sampling_params.use_stratified_sampling())
                throw std::runtime_error("stratified for CSV with random not yet implemented");

            // @TODO: there could be overlap with first/last rows.
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::RANDOM_ROWS, true, &file_offset);

            // detect csv start ?? -> test.
            // parse as rows using the settings detected.
            auto limit = std::min(sample_limit, per_mode_limit);
            auto rows = csv_parseRows(sample.c_str(), sample.size(), expectedColumnCount, file_offset,
                                      _delimiter, _quotechar, _null_values, limit);
            sample_limit -= std::min(v.size(), limit);
            // offset = 0?
            if(file_offset == 0) {
                // header? ignore first row!
                if(_header && !rows.empty())
                    rows.erase(rows.begin());
            } else {
                rows.erase(rows.begin());
            }

            std::copy(rows.begin(), rows.end(), std::back_inserter(v));
        }

        auto sampling_time = timer.time();
        auto per_row_sampling_time_ms = 1000.0 * sampling_time / (std::max(1.0, 1.0 * v.size()));
        logger.info("sampled " + pluralize(v.size(), "row") + " in " + std::to_string(sampling_time) + "s (" + std::to_string(per_row_sampling_time_ms) + "ms / row)");

        return v;
    }

    static Row lineToRow(const aligned_string& line, const std::set<std::string>& null_values, bool includes_newline) {
        // process line
        auto line_without_newline = (includes_newline && line.back() == '\n') ? line.substr(0, line.size() -1) : line;
        assert(line_without_newline.back() != '\n' || line_without_newline.size() < line.size());
        auto it = null_values.find(line_without_newline.c_str());
        if(it != null_values.end())
            return Row(Field::null());
        else
            return Row(line.c_str());
    }

    static std::vector<Row> parseTextRows(const aligned_string& sample, const std::set<std::string>& null_values) {
        std::vector<Row> v;
        // parse lines (ignore \r\n line endings?)
        unsigned markbegin = 0;
        unsigned markend = 0;
        for (int i = 0; i < sample.length(); ++i) {
            if(sample[i] == '\0')
                return v;

            // Windows End of Line (EOL) characters – Carriage Return (CR) & Line Feed (LF)
            bool weol = (i + 1 < sample.length() && sample[i] == '\r' && sample[i+1] == '\n');
            if(sample[i] == '\n' || weol) {
                markend = i;
                auto line = sample.substr(markbegin, markend - markbegin);
                if(weol)
                    line[line.size() - 1] = '\n'; // convert line feed.

                v.push_back(lineToRow(line, null_values, true));
                markbegin = (i + 1);
            }
        }

        // end of line?
        if(markbegin != markend) {
            auto line = sample.substr(markbegin, markend - markbegin);
            v.push_back(lineToRow(line, null_values, true));
        }
        return v;
    }

    std::vector<Row>
    FileInputOperator::sampleJsonFile(const tuplex::URI &uri,
                                      size_t uri_size,
                                      const tuplex::SamplingMode &mode,
                                      std::vector<std::vector<std::string>>* outNames,
                                      const SamplingParameters& sampling_params) {
        auto& logger = Logger::instance().logger("logical");
        std::vector<Row> v;
        assert(mode & SamplingMode::FIRST_ROWS || mode & SamplingMode::LAST_ROWS || mode & SamplingMode::RANDOM_ROWS);

        auto PARAM_USE_GENERIC_DICT = _json_use_generic_dicts;

        if(0 == uri_size || uri == URI::INVALID) {
            logger.debug("empty file, can't obtain sample from it");
            return {};
        }

        auto sample_limit = sampling_params.limit;

        if(0 == sample_limit)
            return {};

        size_t original_sample_limit = sample_limit;

        // decode range based uri (and adjust seeking accordingly!)
        // is the URI range based?
        size_t range_start = 0, range_end = 0;
        size_t range_size = 0;
        URI target_uri;
        decodeRangeURI(uri.toString(), target_uri, range_start, range_end);

        // both 0? no restriction
        if(range_start == 0 && range_end == 0) {
            range_end = uri_size;
            range_size = uri_size;
        } else {
            // restrict!
            range_size = range_end - range_start;
        }

        if(0 == range_size || target_uri == URI::INVALID)
            return {};


        SamplingMode m = mode;

        size_t mode_count = 0;
        mode_count += (mode & SamplingMode::FIRST_ROWS) > 0;
        mode_count += (mode & SamplingMode::LAST_ROWS) > 0;
        mode_count += (mode & SamplingMode::RANDOM_ROWS) > 0;

        sample_limit = std::max(mode_count, sample_limit);
        size_t per_mode_limit = sample_limit / mode_count;

        // sample in no unwrap mode -> unwrap later on demand. -> requires special treatment in resample.

        size_t sampling_size = std::min(range_size, _samplingSize);

        // if uri_size < file_size -> first rows only
        if(uri_size <= sampling_size)
            m = SamplingMode::FIRST_ROWS;

        // check file sampling modes & then load the samples accordingly
        if(m & SamplingMode::FIRST_ROWS && sample_limit > 0) {
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::FIRST_ROWS, true);
            auto sample_length = std::min(sample.size() - 1, strlen(sample.c_str()) + 1);

            size_t start_offset = 0;
            // range start != 0?
            if(0 != range_start) {
                start_offset = findNLJsonStart(sample.c_str(), sample_length);
                if(start_offset < 0)
                    return {};
                sample_length -= std::min((size_t)start_offset, sample_length);
            }

            auto limit = std::min(sample_limit, per_mode_limit);

            if(sampling_params.use_stratified_sampling()) {
                std::set<unsigned> skip_rows;
                assert(sample.size() >= start_offset + sample_length);
                v = parseRowsFromJSONStratified(sample.c_str() + start_offset, sample_length,
                                                outNames, outNames, _json_treat_heterogeneous_lists_as_tuples, limit,
                                                sampling_params.strata_size, sampling_params.samples_per_strata,
                                                sampling_params.random_seed, skip_rows, PARAM_USE_GENERIC_DICT);
            } else {
                v = parseRowsFromJSON(sample.c_str() + start_offset, sample_length,
                                      outNames, outNames, _json_treat_heterogeneous_lists_as_tuples, limit, PARAM_USE_GENERIC_DICT);
            }
            sample_limit -= std::min(v.size(), limit);
        }

        if(m & SamplingMode::LAST_ROWS && sample_limit > 0) {
            // the smaller of remaining and sample size!
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::LAST_ROWS, true, &file_offset);
            auto sample_length = std::min(sample.size() - 1, strlen(sample.c_str()) + 1);
            size_t offset = 0;
            if(!v.empty()) {
                if(range_size < 2 * sampling_size) {
                    offset = sampling_size - (range_size - sampling_size);
                    assert(offset <= sampling_size);
                }
                sample_length -= std::min(sample_length, offset);
                if(0 == sample_length)
                    return v;

                // search for JSON newline start offset.
                auto start_offset = findNLJsonStart(sample.c_str() + offset, sample_length);
                if(start_offset < 0)
                    return v;
                sample_length -= std::min((size_t)start_offset, sample_length);
                auto limit = std::min(sample_limit, per_mode_limit);
                std::vector<Row> rows;
                if(sampling_params.use_stratified_sampling()) {
                    std::set<unsigned> skip_rows;
                    rows = parseRowsFromJSONStratified(sample.c_str() + offset + start_offset, sample_length,
                                                       outNames, outNames, _json_treat_heterogeneous_lists_as_tuples,
                                                       limit, sampling_params.strata_size, sampling_params.samples_per_strata,
                                                       sampling_params.random_seed, skip_rows, PARAM_USE_GENERIC_DICT);
                } else {
                    rows = parseRowsFromJSON(sample.c_str() + offset + start_offset, sample_length,
                                             outNames, outNames, _json_treat_heterogeneous_lists_as_tuples,
                                             limit, PARAM_USE_GENERIC_DICT);
                }
                sample_limit -= std::min(rows.size(), limit);
                std::copy(rows.begin(), rows.end(), std::back_inserter(v));

            } else {
                // search for JSON newline start offset.
                auto start_offset = findNLJsonStart(sample.c_str() + offset, sample_length);
                if(start_offset < 0)
                    return v;
                sample_length -= std::min((size_t)start_offset, sample_length);

                auto limit = std::min(sample_limit, per_mode_limit);
                if(sampling_params.use_stratified_sampling()) {
                    std::set<unsigned> skip_rows;
                    v = parseRowsFromJSONStratified(sample.c_str() + start_offset, sample_length,
                                                    outNames, outNames, _json_treat_heterogeneous_lists_as_tuples,
                                                    limit, sampling_params.strata_size, sampling_params.samples_per_strata,
                                                    sampling_params.random_seed, skip_rows, PARAM_USE_GENERIC_DICT);
                } else {
                    v = parseRowsFromJSON(sample.c_str() + start_offset, sample_length,
                                          outNames, outNames, _json_treat_heterogeneous_lists_as_tuples,
                                          limit, PARAM_USE_GENERIC_DICT);
                }

                sample_limit -= std::min(v.size(), limit);
            }
        }

        // random
        if(m & SamplingMode::RANDOM_ROWS && sample_limit > 0) {
            // @TODO: there could be overlap with first/last rows.
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::RANDOM_ROWS, true, &file_offset);
            auto sample_length = std::min(sample.size() - 1, strlen(sample.c_str()));

            auto start_offset = findNLJsonStart(sample.c_str(), sample_length);
            if(start_offset < 0)
                return v;
            sample_length -= std::min((size_t)start_offset, sample_length);
            // parse as rows using the settings detected.
            auto limit = std::min(sample_limit, per_mode_limit);
            std::vector<Row> rows;
            if(sampling_params.use_stratified_sampling()) {
                std::set<unsigned> skip_rows;
                rows = parseRowsFromJSONStratified(sample.c_str() + start_offset, sample_length,
                                                   outNames, outNames, _json_treat_heterogeneous_lists_as_tuples,
                                                   limit, sampling_params.strata_size, sampling_params.samples_per_strata,
                                                   sampling_params.random_seed, skip_rows, PARAM_USE_GENERIC_DICT);
            } else {
                rows = parseRowsFromJSON(sample.c_str() + start_offset, sample_length,
                                         outNames, outNames, _json_treat_heterogeneous_lists_as_tuples,
                                         limit, PARAM_USE_GENERIC_DICT);
            }

            sample_limit -= std::min(rows.size(), limit);

            // erase last row, b.c. it may be partial
            if(!rows.empty()) {
                rows.erase(rows.end() - 1);
                if(outNames)
                    outNames->erase(outNames->end() - 1);
            }

            std::copy(rows.begin(), rows.end(), std::back_inserter(v));
        }
        v.resize(std::min(v.size(), original_sample_limit));
        if(outNames) {
            outNames->resize(std::min(outNames->size(), original_sample_limit));
            assert(outNames->size() <= original_sample_limit);
        }
        assert(v.size() <= original_sample_limit);

        return v;
    }

    std::vector<Row> FileInputOperator::sampleTextFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        auto& logger = Logger::instance().logger("logical");
        std::vector<Row> v;
        assert(mode & SamplingMode::FIRST_ROWS || mode & SamplingMode::LAST_ROWS || mode & SamplingMode::RANDOM_ROWS);

        if(0 == uri_size || uri == URI::INVALID) {
            logger.debug("empty file, can't obtain sample from it");
            return {};
        }

        SamplingMode m = mode;

        std::set<std::string> null_values(_null_values.begin(), _null_values.end());

        // if uri_size < file_size -> first rows only
        if(uri_size <= _samplingSize)
            m = SamplingMode::FIRST_ROWS;

        // check file sampling modes & then load the samples accordingly
        if(m & SamplingMode::FIRST_ROWS) {
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::FIRST_ROWS, true);
            v = parseTextRows(sample, null_values);
        }

        if(m & SamplingMode::LAST_ROWS) {
            // the smaller of remaining and sample size!
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::LAST_ROWS, true, &file_offset);
            size_t offset = 0;
            if(!v.empty()) {
                if(uri_size < 2 * _samplingSize) {
                    offset = _samplingSize - (uri_size - _samplingSize);
                    assert(offset <= _samplingSize);
                }
                auto rows = parseTextRows(sample, null_values);
                // offset != 0? =? remove first row b.c. it may be partial row
                if(file_offset != 0) {
                    // header? ignore first row!
                    if(!rows.empty())
                        rows.erase(rows.begin());
                }

                std::copy(rows.begin(), rows.end(), std::back_inserter(v));

            } else {
                v = parseTextRows(sample, null_values);

                // offset = 0?
                if(file_offset != 0) {
                    // ignore first row b.c. may be partial row...
                    if(!v.empty())
                        v.erase(v.begin());
                }
            }
        }

        // most complicated: random -> make sure no overlap with first/last rows
        if(m & SamplingMode::RANDOM_ROWS) {
            // @TODO: there could be overlap with first/last rows.
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::RANDOM_ROWS, true, &file_offset);
            // parse as rows using the settings detected.
            auto rows = parseTextRows(sample, null_values);

            // offset = 0?
            if(file_offset != 0) {
                // ignore first row b.c. partial
                if(!rows.empty())
                    rows.erase(rows.begin());
            }

            // erase last row, b.c. partial
            if(!rows.empty())
                rows.erase(rows.end() - 1);

            std::copy(rows.begin(), rows.end(), std::back_inserter(v));
        }

        return v;
    }

    std::vector<Row> FileInputOperator::sampleORCFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        throw std::runtime_error(std::string(__FILE__) + ":" + std::to_string(__LINE__) + " not yet implemented");
        return {};
    }

    std::tuple<python::Type, python::Type> FileInputOperator::detectJsonTypesAndColumns(const ContextOptions& co,
                                                                                        const std::vector<std::vector<std::string>>& columnNamesSampleCollection) {
        auto& logger = Logger::instance().logger("logical");

        // make sure row cache is filled
        if(!_rowsSample.empty() && !_cachePopulated)
            throw std::runtime_error("row cache needs to be populated for JSON reorg");

        if(_cachePopulated && _rowsSample.empty())
            return std::make_tuple(python::Type::EMPTYTUPLE, python::Type::EMPTYTUPLE);

        auto& rows = _rowsSample;
        std::vector<std::string> ordered_names;

        // rows do exist. -> should they get unwrapped?
        if(_json_unwrap_first_level) {

            assert(_rowsSample.size() == columnNamesSampleCollection.size());

            // first step is column name estimation and subsequent row reorganization.
            auto t = sortRowsAndIdentifyColumns(_rowsSample, columnNamesSampleCollection);
            rows = std::get<0>(t);
            ordered_names = std::get<1>(t);
            // are there no rows? => too small a sample. increase sample size.
            if(rows.empty()) {
                throw std::runtime_error("not a single JSON document was found in the file, can't determine type. Please increase sample size.");
            }
        } else {
            // if not, sample is fine as it is. -> maximize type cover
        }

        // check mode: is it unwrapping? => find dominating number of columns! And most likely combo of column names
        std::unordered_map<python::Type, size_t> typeCountMap;
        // simply calc all row counts
        for(const auto& row : rows)
            typeCountMap[row.getRowType()]++;


        // transform to vector for cover
        auto type_counts = std::vector<std::pair<python::Type, size_t>>(typeCountMap.begin(), typeCountMap.end());

        TypeUnificationPolicy normal_policy;
        normal_policy.allowAutoUpcastOfNumbers = co.AUTO_UPCAST_NUMBERS();
        normal_policy.allowUnifyWithPyObject = false;
        normal_policy.treatMissingDictKeysAsNone = false; // maybe as option?
        normal_policy.unifyMissingDictKeys = false; // could be potentially allowed for subschemas etc.?

        TypeUnificationPolicy general_policy;
        general_policy.allowAutoUpcastOfNumbers = co.AUTO_UPCAST_NUMBERS();
        general_policy.allowUnifyWithPyObject = false;
        general_policy.treatMissingDictKeysAsNone = false; // maybe as option?
        general_policy.unifyMissingDictKeys = true; // this sets it apart from normal policy...

        // execute cover
        auto normal_case_max_type = maximizeStructuredDictTypeCover(type_counts, co.NORMALCASE_THRESHOLD(), co.OPT_NULLVALUE_OPTIMIZATION(), normal_policy);
        auto general_case_max_type = maximizeStructuredDictTypeCover(type_counts, co.NORMALCASE_THRESHOLD(), co.OPT_NULLVALUE_OPTIMIZATION(), general_policy);

        // either invalid?
        if(python::Type::UNKNOWN == normal_case_max_type.first || python::Type::UNKNOWN == general_case_max_type.first) {
            logger.warn("could not detect schema for JSON input");
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
            return std::make_tuple(python::Type::EMPTYTUPLE, python::Type::EMPTYTUPLE);
        }

        // set ALL column names (as they appear)
        // -> pushdown for JSON is special case...
        if(_json_unwrap_first_level) {
           _columnNames = ordered_names;
            // for columns() to work as well -> need to set projection defaults first!
            assert(inputColumns().size() == ordered_names.size());
        }

        // normalcase and exception case types
        // -> use type cover for this from sample!
        auto normalcasetype = !_json_unwrap_first_level ? normal_case_max_type.first.parameters().front() : normal_case_max_type.first;
        auto generalcasetype = !_json_unwrap_first_level ? general_case_max_type.first.parameters().front() : general_case_max_type.first;

        // can normal case be upgraded to general case?
        if(!python::canUpcastType(normalcasetype, generalcasetype)) {
            logger.debug("normal type can't be upcasted to general case type.");

            // unify current normal and general case type using optional keys -> this should help.
            auto uni_type = unifyTypes(normalcasetype, generalcasetype, general_policy);
            if(uni_type == python::Type::UNKNOWN)
                logger.warn("got incompatible normal and general case types.");
            else {
                if(uni_type != generalcasetype)
                    logger.debug("generalized general case further, so normal case can be upcasted to it.");
                generalcasetype = uni_type;
                if(!python::canUpcastType(normalcasetype, generalcasetype)) // now upcast should work
                    logger.info("detected incompatible normal and general types, proceeding with different paths.");
            }
        }

        // rowtype is always, well a row
        normalcasetype = python::Type::propagateToTupleType(normalcasetype);
        generalcasetype = python::Type::propagateToTupleType(generalcasetype);

        return std::make_tuple(normalcasetype, generalcasetype);
    }

    size_t FileInputOperator::storedSampleRowCount() const {
        return _rowsSample.size();
    }

    // this function is not accurate but rather an estimate. Can be easily improved.
    size_t FileInputOperator::sampleSize() const {
        size_t total_sample_size = 0;

        // per-file factor
        size_t sample_size_per_file = 0;

        if(_samplingMode & SamplingMode::FIRST_ROWS)
            sample_size_per_file += _samplingSize;

        if(_samplingMode & SamplingMode::LAST_ROWS)
            sample_size_per_file += _samplingSize;

        if(_samplingMode & SamplingMode::RANDOM_ROWS)
            sample_size_per_file += _samplingSize;

        size_t num_files_to_sample = 0;

        // base upon sampling mode.
        if(_samplingMode & SamplingMode::FIRST_FILE)
            num_files_to_sample++;
        if(_samplingMode & SamplingMode::LAST_FILE)
            num_files_to_sample++;
        if(_samplingMode & SamplingMode::RANDOM_FILE)
            num_files_to_sample++;

        if(_samplingMode & SamplingMode::ALL_FILES) {
            num_files_to_sample = _sizes.size();
        }

        // sanitize values
        num_files_to_sample = std::min(num_files_to_sample, _sizes.size());

        // how many per file?
        // -> are there smaller files than the sample size? scale accordingly.
        size_t size_smaller = 0;
        size_t num_smaller = 0;
        size_t total_size = 0;
        for(auto s : _sizes) {
            if(s < sample_size_per_file) {
                num_smaller++;
                size_smaller += s;
            }
            total_size += s;
        }

        // scale accordingly
        if(num_smaller > 0) {
            double fraction_smaller = (double)num_smaller / (double)_sizes.size();
            double smaller_avg_size = (double)size_smaller / (double)num_smaller;
            size_t estimate = (fraction_smaller * smaller_avg_size + (1.0 - fraction_smaller) * sample_size_per_file) * num_files_to_sample;
            return estimate;
        } else {
            return num_files_to_sample * sample_size_per_file;
        }
    }

    size_t FileInputOperator::estimateSampleBasedRowCount() {
        auto& logger = Logger::instance().logger("logical");

        // cache & mode should be populated
        assert(_samplingMode != SamplingMode::UNKNOWN && _cachePopulated);

        // make sure rows are filled.
        if(_rowsSample.empty()) {
            logger.warn("no row sample detected, using dummy value of 1000 rows.");
            return 1000;
        }

        // base estimation on number of samples stored vs. sample sizes vs. rest
        size_t num_samples = _rowsSample.size();
        if(num_samples < 5)
            num_samples = 5; // attribute at least some cost to this

        // total file size
        size_t total_size = 0;
        for(auto s : _sizes)
            total_size += s;

        // num_samples rows == sample_size_used, how much rows are there for total_size?
        double factor = 1.0 / (double)sampleSize();
        double estimate = (factor * num_samples) * total_size;

        // safe-guard
        if(estimate < 10.0)
            return 10;

        return (size_t)ceil(estimate);
    }
}