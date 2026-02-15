#include "lsm/lsm.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <filesystem>
#include "lsm/storage/file.h"

#include <lsm/storage/buffer_pool.h>
#include <lsm/bloom_filter/bloom_filter.h>
#include <lsm/common/merge.h>
#include <lsm/memtable.h>

namespace lsm {

class SimpleLSMImpl : public ILSM {

   public:
    SimpleLSMImpl(const LsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory, uint64_t* read_bytes)
        : options_(options), levels_provider_(levels_provider), sstable_factory_(sstable_factory) {
        mem_table_ = MakeMemTable(options_.max_level_skip_list);
        std::filesystem::create_directory(dir_);
        buffer_pool_ = storage::MakeReadBufferPool(dir_, options_.buffer_pool_size, options_.frame_size, read_bytes);
    }

    void Put(const UserKey& user_key, const Value& value) {
        mem_table_->Add(++sequence_number_, user_key, value);
        CheckMemTable();
    }

    void Delete(const UserKey& user_key) {
        mem_table_->Delete(++sequence_number_, user_key);
        CheckMemTable();
    }

    void CheckMemTable() {
        if (mem_table_->ApproximateMemoryUsage() > options_.memtable_bytes) {
            std::shared_ptr<storage::IFile> file = std::make_shared<storage::BufferedMemoryFile>(dir_, sstable_sequence_number_++, buffer_pool_, options_.frame_size);
            auto sstable_builder = sstable_factory_->NewFileBuilder(file);
            auto scan = mem_table_->MakeScan();
            auto object = scan->Next();
            std::optional<SSTableMetadata> meta = std::nullopt;
            if (object.has_value()) {
                meta = SSTableMetadata();
                meta->min_key = object->first.user_key;
            }

            while (object.has_value()) {
                sstable_builder->Add(object->first, object->second);
                meta->max_key = object->first.user_key;
                object = scan->Next();
            }
            sstable_builder->Finish();
            if (meta.has_value()) {
                meta->file_size = file->Size();
            }

            size_t lvl = 0;
            while (levels_provider_->NumTables(lvl)) {
                auto old_file = levels_provider_->GetTableFile(lvl, 0);
                auto old_meta = levels_provider_->GetTableMetadata(lvl, 0);
                levels_provider_->EraseTable(lvl, 0);
                auto [new_file, new_meta] = MergeSSTables(file, meta, old_file, old_meta);
                file = new_file;
                meta = new_meta;
                ++lvl;
            }
            levels_provider_->InsertTableFile(lvl, 0, file, nullptr, meta);

            mem_table_ = MakeMemTable(options_.max_level_skip_list);
        }
    }

    std::pair<std::shared_ptr<storage::IFile>, std::optional<SSTableMetadata>> MergeSSTables(const std::shared_ptr<const storage::IFile>& file1, const std::optional<SSTableMetadata>& meta1,
                                                                                             const std::shared_ptr<const storage::IFile>& file2, const std::optional<SSTableMetadata>& meta2) {
        auto file = std::make_shared<storage::BufferedMemoryFile>(dir_, sstable_sequence_number_++, buffer_pool_, options_.frame_size);
        std::optional<SSTableMetadata> meta = std::nullopt;
        if (meta1.has_value() && meta2.has_value()) {
            meta = SSTableMetadata();
            meta->min_key = std::min(meta1->min_key, meta2->min_key);
            meta->max_key = std::max(meta1->max_key, meta2->max_key);
        } else if (meta1.has_value()) {
            meta = meta1;
        } else if (meta2.has_value()) {
            meta = meta2;
        }

        auto reader1 = sstable_factory_->FromFile(file1)->MakeScan();
        auto reader2 = sstable_factory_->FromFile(file2)->MakeScan();

        auto merge_scan = MakeMerger<std::pair<InternalKey, Value>>({reader1, reader2});
        auto builder = sstable_factory_->NewFileBuilder(file);
        auto object = merge_scan->Next();
        while (object.has_value()) {
            builder->Add(object->first, object->second);
            object = merge_scan->Next();
        }
        builder->Finish();

        if (meta.has_value()) {
            meta->file_size = file->Size();
        }

        return {file, meta};
    }

    std::optional<Value> Get(const UserKey& user_key, uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const {
        Value value;
        auto type = mem_table_->Get(user_key, &value, sequence_number);
        if (type == IMemTable::GetKind::kFound) {
            return value;
        } else if (type == IMemTable::GetKind::kDeletion) {
            return std::nullopt;
        } else {
            for (size_t lvl = 0; lvl < levels_provider_->NumLevels(); ++lvl) {
                if (levels_provider_->NumTables(lvl) == 0) {
                    continue;
                }
                auto meta = levels_provider_->GetTableMetadata(lvl, 0);
                if (!meta.has_value() || meta->max_key < user_key || user_key < meta->min_key) {
                    continue;
                }
                auto sstable_reader = sstable_factory_->FromFile(levels_provider_->GetTableFile(lvl, 0));
                Value value;
                auto type = sstable_reader->Get(user_key, &value, sequence_number);
                if (type == ISSTableReader::GetKind::kFound) {
                    return value;
                } else if (type == ISSTableReader::GetKind::kDeletion) {
                    return std::nullopt;
                }
            }
            return std::nullopt;
        }
    }

    std::shared_ptr<IStream<std::pair<UserKey, Value>>> Scan(const std::optional<UserKey>& start_key, const std::optional<UserKey>& end_key,
                                                             uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const {
        return std::make_shared<SimpleLSMStream>(mem_table_, levels_provider_, sstable_factory_, start_key, end_key, sequence_number);
    }

    virtual uint64_t GetCurrentSequenceNumber() const { return sequence_number_; }

    virtual ~SimpleLSMImpl() {
        std::filesystem::remove_all(dir_);
    }

   private:
    class SimpleLSMStream : public IStream<std::pair<UserKey, Value>> {
       public:
        SimpleLSMStream(std::shared_ptr<IMemTable> mem_table, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory,
                        const std::optional<UserKey>& start_key, const std::optional<UserKey>& end_key, uint64_t sequence_number)
            : sequence_number_(sequence_number), start_key_(start_key), end_key_(end_key) {
            std::vector<std::shared_ptr<IStream<std::pair<InternalKey, Value>>>> sources;
            sources.push_back(mem_table->MakeScan());
            for (size_t lvl = 0; lvl < levels_provider->NumLevels(); ++lvl) {
                if (levels_provider->NumTables(lvl)) {
                    auto meta = levels_provider->GetTableMetadata(lvl, 0);
                    if (!meta.has_value()) {
                        continue;
                    }
                    auto start_key = start_key_.has_value() ? *start_key_ : meta->min_key;
                    auto end_key = end_key_.has_value() ? *end_key_ : meta->max_key;
                    if (meta->Overlaps(start_key, end_key)) {
                        sources.push_back(sstable_factory->FromFile(levels_provider->GetTableFile(lvl, 0))->MakeScan());
                    }
                }
            }
            merge_scan_ = MakeMerger<std::pair<InternalKey, Value>>(sources);
        }

        std::optional<std::pair<UserKey, Value>> Next() {
            std::optional<std::pair<InternalKey, Value>> object;
            do {
                do {
                    object = merge_scan_->Next();
                    if (start_key_.has_value()) {
                        while (object.has_value() && object->first.user_key < *start_key_) {
                            object = merge_scan_->Next();
                        }
                    }
                    if (!object.has_value() || (end_key_.has_value() && object->first.user_key >= *end_key_)) {
                        return std::nullopt;
                    }
                } while (sequence_number_ < object->first.sequence_number);
                if (object->first.type == ValueType::kDeletion) {
                    used_ = object->first.user_key;
                }
            } while (object->first.user_key == used_);
            used_ = object->first.user_key;
            return std::make_pair(object->first.user_key, object->second);
        }

       private:
        UserKey used_ = {};
        std::shared_ptr<IMerger<std::pair<InternalKey, Value>>> merge_scan_;
        uint64_t sequence_number_;
        std::optional<UserKey> start_key_;
        std::optional<UserKey> end_key_;
    };

   private:
    uint64_t sequence_number_ = 0;
    uint64_t sstable_sequence_number_ = 0;
    std::string dir_ = "simple_lsm";
    LsmOptions options_;
    std::shared_ptr<IMemTable> mem_table_;
    std::shared_ptr<ILevelsProvider> levels_provider_;
    std::shared_ptr<ISSTableSerializer> sstable_factory_;
    std::shared_ptr<storage::IReadBufferPool> buffer_pool_;
};

class GranularLSMImpl : public ILSM {
   public:
    GranularLSMImpl(const GranularLsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory, uint64_t* read_bytes)
        : options_(options), levels_provider_(levels_provider), sstable_factory_(sstable_factory) {
        mem_table_ = MakeMemTable(options_.max_level_skip_list);
        std::filesystem::create_directory(dir_);
        buffer_pool_ = storage::MakeReadBufferPool(dir_, options_.buffer_pool_size, options_.frame_size, read_bytes);
    }

    void Put(const UserKey& user_key, const Value& value) {
        mem_table_->Add(++sequence_number_, user_key, value);
        CheckMemTable();
    }

    void Delete(const UserKey& user_key) {
        mem_table_->Delete(++sequence_number_, user_key);
        CheckMemTable();
    }

    void CheckMemTable() {
        if (mem_table_->ApproximateMemoryUsage() > options_.memtable_bytes) {
            std::vector<std::shared_ptr<IStream<std::pair<InternalKey, Value>>>> sources(1, mem_table_->MakeScan());
            mem_table_ = MakeMemTable(options_.max_level_skip_list);
            for (size_t lvl = 0, max_tables = options_.l0_capacity; !sources.empty(); ++lvl, max_tables *= options_.level_size_multiplier) {
                auto main_scan = MakeMerger(sources);
                sources.resize(0);
                if (levels_provider_->NumTables(lvl)) {
                    size_t ind = 0;
                    auto object = main_scan->Next();
                    while (ind < levels_provider_->NumTables(lvl)) {
                        std::optional<UserKey> end_key = std::nullopt;
                        if (ind + 1 < levels_provider_->NumTables(lvl)) {
                            end_key = levels_provider_->GetTableMetadata(lvl, ind)->max_key;
                        }

                        std::vector<std::pair<InternalKey, Value>> merge_objects;
                        while (object.has_value() && (!end_key.has_value() || object->first.user_key <= *end_key)) {
                            merge_objects.push_back(*object);
                            object = main_scan->Next();
                        }

                        if (merge_objects.empty()) {
                            if (!object.has_value()) {
                                break;
                            }
                            ++ind;
                            continue;
                        }
                        auto vector_scan = std::make_shared<StreamFromVector>(merge_objects);

                        auto sstable_scan = sstable_factory_->FromFile(levels_provider_->GetTableFile(lvl, ind))->MakeScan();

                        levels_provider_->EraseTable(lvl, ind);

                        auto files = GetFilesSplitByKeys(MakeMerger<std::pair<InternalKey, Value>>({vector_scan, sstable_scan}), max_tables - 1);
                        for (auto [sf_files, meta] : files) {
                            auto [file, filter_builder] = sf_files;
                            if (levels_provider_->NumTables(lvl) + 1 == max_tables) {
                                sources.push_back(sstable_factory_->FromFile(file)->MakeScan());
                                continue;
                            }
                            if (options_.bloom_filter_size != 0) {
                                auto filter_file = std::make_shared<storage::MemoryFile>(dir_ + "/filter_" + std::to_string(filter_sequence_number_++));
                                auto serialized_filter = filter_builder->Serialize();
                                filter_file->Write(serialized_filter.data(), serialized_filter.size());
                                levels_provider_->InsertTableFile(lvl, ind++, file, filter_file, meta);
                            } else {
                                levels_provider_->InsertTableFile(lvl, ind++, file, nullptr, meta);
                            }
                        }
                    }
                } else {
                    size_t ind = 0;
                    auto files = GetFilesSplitByKeys(main_scan, max_tables - 1);
                    for (auto [sf_files, meta] : files) {
                        auto [file, filter_builder] = sf_files;
                        if (levels_provider_->NumTables(lvl) + 1 == max_tables) {
                            sources.push_back(sstable_factory_->FromFile(file)->MakeScan());
                            continue;
                        }
                        if (options_.bloom_filter_size != 0) {
                            auto filter_file = std::make_shared<storage::MemoryFile>(dir_ + "/filter_" + std::to_string(filter_sequence_number_++));
                            auto serialized_filter = filter_builder->Serialize();
                            filter_file->Write(serialized_filter.data(), serialized_filter.size());
                            levels_provider_->InsertTableFile(lvl, ind++, file, filter_file, meta);
                        } else {
                            levels_provider_->InsertTableFile(lvl, ind++, file, nullptr, meta);
                        }
                    }
                }
            }
        }
    }

    std::vector<std::pair<std::pair<std::shared_ptr<storage::IFile>, std::shared_ptr<IFilterBuilder>>, std::optional<SSTableMetadata>>> GetFilesSplitByKeys(std::shared_ptr<IStream<std::pair<InternalKey, Value>>> scan, size_t max_tables) {
        std::vector<std::pair<std::pair<std::shared_ptr<storage::IFile>, std::shared_ptr<IFilterBuilder>>, std::optional<SSTableMetadata>>> result;
        uint64_t sum_mem = sizeof(uint64_t);
        std::vector<std::pair<InternalKey, Value>> objects;
        uint64_t key_mem = 0;
        std::vector<std::pair<InternalKey, Value>> key_objects;
        auto object = scan->Next();
        while (object.has_value()) {
            if (!key_objects.empty() && key_objects.back().first.user_key == object->first.user_key) {
                key_mem += 3 * sizeof(uint64_t) + object->first.user_key.size() + object->second.size();
                key_objects.push_back(*object);
            } else {
                if (sum_mem + key_mem > options_.max_sstable_size) {
                    result.push_back(MakeFileFromVector(objects, options_.bloom_filter_size != 0 && max_tables-- > 0));
                    objects.resize(0);
                    sum_mem = sizeof(uint64_t);
                }
                sum_mem += key_mem;
                for (auto& obj : key_objects) {
                    objects.push_back(obj);
                }
                key_objects.resize(0);
                key_mem = 3 * sizeof(uint64_t) + object->first.user_key.size() + object->second.size();
                key_objects.push_back(*object);
            }
            object = scan->Next();
        }
        if (!key_objects.empty()) {
            if (sum_mem + key_mem > options_.max_sstable_size) {
                result.push_back(MakeFileFromVector(objects, options_.bloom_filter_size != 0 && max_tables-- > 0));
                objects.resize(0);
                sum_mem = sizeof(uint64_t);
            }
            sum_mem += key_mem;
            for (auto& obj : key_objects) {
                objects.push_back(obj);
            }
        }
        if (!objects.empty()) {
            result.push_back(MakeFileFromVector(objects, options_.bloom_filter_size != 0 && max_tables-- > 0));
        }
        return result;
    }

    std::pair<std::pair<std::shared_ptr<storage::IFile>, std::shared_ptr<IFilterBuilder>>, std::optional<SSTableMetadata>> MakeFileFromVector(const std::vector<std::pair<InternalKey, Value>>& objects, bool generate_filter) {
        std::shared_ptr<storage::IFile> file = std::make_shared<storage::BufferedMemoryFile>(dir_, sstable_sequence_number_++, buffer_pool_, options_.frame_size);
        auto sstable_builder = sstable_factory_->NewFileBuilder(file);
        std::shared_ptr<IFilterBuilder> filter_builder = nullptr;
        if (generate_filter) {
            filter_builder = MakeFilterBuilder(8 * options_.bloom_filter_size, options_.bloom_filter_hash_count);
        }
        for (auto& object : objects) {
            sstable_builder->Add(object.first, object.second);
            if (generate_filter) {
                filter_builder->Add(object.first.user_key);
            }
        }
        sstable_builder->Finish();
        SSTableMetadata meta = {objects.front().first.user_key, objects.back().first.user_key, file->Size()};
        return {{file, filter_builder}, meta};
    }

    std::optional<Value> Get(const UserKey& user_key, uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const {
        Value value;
        auto type = mem_table_->Get(user_key, &value, sequence_number);
        if (type == IMemTable::GetKind::kFound) {
            return value;
        } else if (type == IMemTable::GetKind::kDeletion) {
            return std::nullopt;
        } else {
            for (size_t lvl = 0; lvl < levels_provider_->NumLevels(); ++lvl) {
                size_t l = 0, r = levels_provider_->NumTables(lvl);
                while (r - l > 1) {
                    size_t ind = (l + r) / 2;
                    auto meta = levels_provider_->GetTableMetadata(lvl, ind - 1);
                    if (meta.has_value() && meta->max_key < user_key) {
                        l = ind;
                    } else {
                        r = ind;
                    }
                }
                if (options_.bloom_filter_size != 0) {
                    auto filter = MakeFilterDeserializer()->Deserialize(levels_provider_->GetTableBloomFilter(lvl, r - 1)->Read(0, options_.bloom_filter_size));
                    if (!filter->MayContain(user_key)) {
                        continue;
                    }
                }
                auto sstable_reader = sstable_factory_->FromFile(levels_provider_->GetTableFile(lvl, r - 1));
                Value value;
                auto type = sstable_reader->Get(user_key, &value, sequence_number);
                if (type == ISSTableReader::GetKind::kFound) {
                    return value;
                } else if (type == ISSTableReader::GetKind::kDeletion) {
                    return std::nullopt;
                }
            }
            return std::nullopt;
        }
    }

    std::shared_ptr<IStream<std::pair<UserKey, Value>>> Scan(const std::optional<UserKey>& start_key, const std::optional<UserKey>& end_key,
                                                             uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const {
        return std::make_shared<GranularLSMStream>(mem_table_, levels_provider_, sstable_factory_, start_key, end_key, sequence_number);
    }

    virtual uint64_t GetCurrentSequenceNumber() const { return sequence_number_; }

    virtual ~GranularLSMImpl() {
        std::filesystem::remove_all(dir_);
    }

   private:
    class GranularLSMStream : public IStream<std::pair<UserKey, Value>> {
       public:
        GranularLSMStream(std::shared_ptr<IMemTable> mem_table, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory,
                          const std::optional<UserKey>& start_key, const std::optional<UserKey>& end_key, uint64_t sequence_number)
            : sequence_number_(sequence_number), start_key_(start_key), end_key_(end_key) {
            std::vector<std::shared_ptr<IStream<std::pair<InternalKey, Value>>>> sources;
            sources.push_back(mem_table->MakeScan());
            for (size_t lvl = 0; lvl < levels_provider->NumLevels(); ++lvl) {
                if (levels_provider->NumTables(lvl)) {
                    sources.push_back(std::make_shared<LevelLSMStream>(lvl, levels_provider, sstable_factory));
                }
            }
            merge_scan_ = MakeMerger<std::pair<InternalKey, Value>>(sources);
        }

        std::optional<std::pair<UserKey, Value>> Next() {
            std::optional<std::pair<InternalKey, Value>> object;
            do {
                do {
                    object = merge_scan_->Next();
                    if (start_key_.has_value()) {
                        while (object.has_value() && object->first.user_key < *start_key_) {
                            object = merge_scan_->Next();
                        }
                    }
                    if (!object.has_value() || (end_key_.has_value() && object->first.user_key >= *end_key_)) {
                        return std::nullopt;
                    }
                } while (sequence_number_ < object->first.sequence_number);
                if (object->first.type == ValueType::kDeletion) {
                    used_ = object->first.user_key;
                }
            } while (object->first.user_key == used_);
            used_ = object->first.user_key;
            return std::make_pair(object->first.user_key, object->second);
        }

       private:
        UserKey used_ = {};
        std::shared_ptr<IMerger<std::pair<InternalKey, Value>>> merge_scan_;
        uint64_t sequence_number_;
        std::optional<UserKey> start_key_;
        std::optional<UserKey> end_key_;
    };

    class LevelLSMStream : public IStream<std::pair<InternalKey, Value>> {
       public:
        LevelLSMStream(size_t level, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory)
            : level_(level), levels_provider_(levels_provider), sstable_factory_(sstable_factory) {
            current_stream_ = sstable_factory_->FromFile(levels_provider->GetTableFile(level_, ind_++))->MakeScan();
        }

        std::optional<std::pair<InternalKey, Value>> Next() {
            auto object = current_stream_->Next();
            while (!object.has_value()) {
                if (ind_ == levels_provider_->NumTables(level_)) {
                    return std::nullopt;
                }
                current_stream_ = sstable_factory_->FromFile(levels_provider_->GetTableFile(level_, ind_++))->MakeScan();
                object = current_stream_->Next();
            }
            return object;
        }

       private:
        size_t level_;
        size_t ind_ = 0;
        std::shared_ptr<ILevelsProvider> levels_provider_;
        std::shared_ptr<ISSTableSerializer> sstable_factory_;
        std::shared_ptr<IStream<std::pair<InternalKey, Value>>> current_stream_;
    };

    class StreamFromVector : public IStream<std::pair<InternalKey, Value>> {
       public:
        StreamFromVector(std::vector<std::pair<InternalKey, Value>> data) : data_(data) {}

        std::optional<std::pair<InternalKey, Value>> Next() {
            if (ind_ == data_.size()) {
                return std::nullopt;
            }
            return data_[ind_++];
        }

       private:
        size_t ind_ = 0;
        std::vector<std::pair<InternalKey, Value>> data_;
    };

   private:
    uint64_t sequence_number_ = 0;
    uint64_t sstable_sequence_number_ = 0;
    uint64_t filter_sequence_number_ = 0;
    std::string dir_ = "granular_lsm";
    GranularLsmOptions options_;
    std::shared_ptr<IMemTable> mem_table_;
    std::shared_ptr<ILevelsProvider> levels_provider_;
    std::shared_ptr<ISSTableSerializer> sstable_factory_;
    std::shared_ptr<storage::IReadBufferPool> buffer_pool_;
};

std::unique_ptr<ILSM> MakeLsm(const LsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory, uint64_t* read_bytes) {
    return std::make_unique<SimpleLSMImpl>(options, levels_provider, sstable_factory, read_bytes);
}

std::unique_ptr<ILSM> MakeGranularLsm(const GranularLsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory, uint64_t* read_bytes) {
    return std::make_unique<GranularLSMImpl>(options, levels_provider, sstable_factory, read_bytes);
}

}  // namespace lsm
