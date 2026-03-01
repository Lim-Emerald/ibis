#include "index/lsm/lsm.h"

#include <index/bloom_filter/bloom_filter.h>
#include <index/common/merge.h>
#include <index/lsm/memtable.h>
#include <index/storage/buffer_pool.h>
#include <index/storage/file.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace invindex::lsm {

// Test implementation of ILevelsProvider that stores SSTables in memory
class LevelsProviderImpl final : public ILevelsProvider {
   public:
    size_t NumLevels() const override { return levels_.size(); }

    size_t NumTables(size_t level_index) const override {
        if (level_index >= levels_.size()) {
            return 0;
        }
        return levels_[level_index].size();
    }

    size_t GetLevelVersion(size_t level_index) const override {
        if (level_index >= levels_.size()) {
            return 0;
        }
        return level_versions_.at(level_index);
    }

    void UpLevelVersion(size_t level_index) override {
        ++level_versions_[level_index];
    }

    std::shared_ptr<const storage::IFile> GetTableFile(size_t level_index, size_t table_index) const override { return levels_.at(level_index).at(table_index); }

    void InsertTableFile(size_t level_index, size_t table_index, std::shared_ptr<const storage::IFile> file, std::shared_ptr<const storage::IFile> bloom_filter,
                         std::optional<SSTableMetadata> metadata = std::nullopt) override {
        // Auto-create levels as needed
        if (levels_.size() <= level_index) {
            levels_.resize(level_index + 1);
            filters_.resize(level_index + 1);
            metadata_.resize(level_index + 1);
            level_versions_.resize(level_index + 1);
            level_versions_[level_index] = 0;
            std::cout << "Up lsm level: " << levels_.size() << '\n';
        }
        auto& v = levels_[level_index];
        auto& f = filters_[level_index];
        auto& m = metadata_[level_index];
        if (table_index > v.size()) {
            table_index = v.size();
        }
        v.insert(v.begin() + table_index, std::move(file));
        f.insert(f.begin() + table_index, std::move(bloom_filter));

        if (metadata.has_value()) {
            m.insert(m.begin() + table_index, *metadata);
        } else {
            m.insert(m.begin() + table_index, std::nullopt);
        }
    }

    void EraseTable(size_t level_index, size_t table_index) override {
        auto& v = levels_.at(level_index);
        auto& f = filters_.at(level_index);
        auto& m = metadata_.at(level_index);
        v.erase(v.begin() + table_index);
        f.erase(f.begin() + table_index);
        m.erase(m.begin() + table_index);
    }

    std::optional<SSTableMetadata> GetTableMetadata(size_t level_index, size_t table_index) const override {
        if (level_index >= metadata_.size() || table_index >= metadata_[level_index].size()) {
            return std::nullopt;
        }
        return metadata_[level_index][table_index];
    }

    std::shared_ptr<const storage::IFile> GetTableBloomFilter(size_t level_index, size_t table_index) const override { return filters_.at(level_index).at(table_index); }

   private:
    std::vector<std::vector<std::shared_ptr<const storage::IFile>>> levels_;
    std::vector<std::vector<std::shared_ptr<const storage::IFile>>> filters_;
    std::vector<std::vector<std::optional<SSTableMetadata>>> metadata_;
    std::vector<size_t> level_versions_;
};

std::shared_ptr<ILevelsProvider> MakeLevelsProvider() {
    return std::make_shared<LevelsProviderImpl>();
}

class LeveledLSMImpl : public ILSM {
   public:
    LeveledLSMImpl(const std::string dir, const GranularLsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory)
        : dir_(dir), options_(options), levels_provider_(levels_provider), sstable_factory_(sstable_factory) {
        mem_table_ = MakeMemTable(options_.max_level_skip_list);
        std::filesystem::create_directory(dir_);
        buffer_pool_ = storage::MakeReadBufferPool(options_.buffer_pool_size, options_.frame_size);
    }

    void Put(const UserKey& user_key, const Value& value) {
        mem_table_->Add(user_key, value);
        CheckMemTable();
    }

    void Delete(const UserKey& user_key) {
        mem_table_->Delete(user_key);
        CheckMemTable();
    }

    std::optional<Value> Get(const UserKey& user_key) const {
        Value result;
        auto type = mem_table_->Get(user_key, &result);
        if (type != IMemTable::GetKind::kDeletion) {
            for (size_t lvl = 0; lvl < levels_provider_->NumLevels(); ++lvl) {
                if (!levels_provider_->NumTables(lvl)) {
                    continue;
                }
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
                auto type = sstable_reader->Get(user_key, &value);
                if (type == ISSTableReader::GetKind::kFound) {
                    result |= value;
                } else if (type == ISSTableReader::GetKind::kDeletion) {
                    break;
                }
            }
        }
        if (result.isEmpty()) {
            return std::nullopt;
        } else {
            return result;
        }
    }

    std::shared_ptr<IStream<std::pair<UserKey, Value>>> Scan(const std::optional<UserKey>& start_key, const std::optional<UserKey>& end_key) const {
        return std::make_shared<LeveledLSMStream>(mem_table_, levels_provider_, sstable_factory_, start_key, end_key);
    }

    virtual ~LeveledLSMImpl() { std::filesystem::remove_all(dir_); }

   private:
    std::string MakeFileName(std::string type, size_t level_index, size_t table_index, size_t version) {
        return dir_ + "/file_l" + std::to_string(level_index) + "_t" + std::to_string(table_index) + "_v" + std::to_string(version) + "." + type;
    }

    void CheckMemTable() {
        if (mem_table_->ApproximateMemoryUsage() > options_.memtable_bytes) {
            std::vector<std::shared_ptr<IStream<std::pair<InternalKey, Value>>>> sources(1, mem_table_->MakeScan());
            mem_table_ = MakeMemTable(options_.max_level_skip_list);
            for (size_t level_index = 0, level_capacity = options_.l0_capacity; !sources.empty(); ++level_index, level_capacity *= options_.level_size_multiplier) {
                for (size_t table_index = 0; table_index < levels_provider_->NumTables(level_index); ++table_index) {
                    sources.push_back(sstable_factory_->FromFile(levels_provider_->GetTableFile(level_index, table_index))->MakeScan());
                }
                if (levels_provider_->NumTables(level_index)) {
                    while (levels_provider_->NumTables(level_index)) {
                        levels_provider_->EraseTable(level_index, levels_provider_->NumTables(level_index) - 1);
                    }
                    levels_provider_->UpLevelVersion(level_index);
                }

                auto sources_scan = MakeMerger(sources);
                sources.resize(0);

                auto files = MergeSources(sources_scan, level_index, level_capacity - 1);
                if (files.size() < level_capacity) {
                    for (auto [sst_and_filter, meta] : files) {
                        auto [file, filter_builder] = sst_and_filter;
                        std::shared_ptr<storage::MemoryFile> filter_file = nullptr;
                        if (options_.bloom_filter_size) {
                            filter_file = std::make_shared<storage::MemoryFile>(MakeFileName("blm", level_index, levels_provider_->NumTables(level_index), levels_provider_->GetLevelVersion(level_index)));
                            auto serialized_filter = filter_builder->Serialize();
                            filter_file->Write(serialized_filter.data(), serialized_filter.size());
                        }
                        levels_provider_->InsertTableFile(level_index, levels_provider_->NumTables(level_index), file, filter_file, meta);
                    }
                } else {
                    for (auto [sst_and_filter, meta] : files) {
                        auto [file, filter_builder] = sst_and_filter;
                        sources.push_back(sstable_factory_->FromFile(file)->MakeScan());
                    }
                }
            }
        }
    }

    std::vector<std::pair<std::pair<std::shared_ptr<storage::IFile>, std::shared_ptr<IFilterBuilder>>, std::optional<SSTableMetadata>>> MergeSources(
        std::shared_ptr<IStream<std::pair<InternalKey, Value>>> scan, size_t level_index, size_t max_tables) {
        std::vector<std::pair<std::pair<std::shared_ptr<storage::IFile>, std::shared_ptr<IFilterBuilder>>, std::optional<SSTableMetadata>>> result;
        uint64_t sum_mem = sizeof(uint64_t);
        std::vector<std::pair<InternalKey, Value>> objects;
        auto object = scan->Next();
        if (!object.has_value()) {
            return result;
        }
        std::pair<InternalKey, Value> key_object = *object;
        while (object.has_value()) {
            if (key_object.first.user_key == object->first.user_key) {
                key_object.second |= object->second;
            } else {
                key_object.second.runOptimize();
                if (sum_mem + key_object.second.getSizeInBytes() > options_.max_sstable_size) {
                    result.push_back(MakeFileFromVector(objects, MakeFileName("sst", level_index, result.size(), levels_provider_->GetLevelVersion(level_index)), options_.bloom_filter_size != 0 && (max_tables--) > 0));
                    objects.resize(0);
                    sum_mem = sizeof(uint64_t);
                }
                sum_mem += key_object.second.getSizeInBytes();
                objects.push_back(key_object);
                key_object = *object;
            }
            object = scan->Next();
        }
        key_object.second.runOptimize();
        if (sum_mem + key_object.second.getSizeInBytes() > options_.max_sstable_size) {
            result.push_back(MakeFileFromVector(objects, MakeFileName("sst", level_index, result.size(), levels_provider_->GetLevelVersion(level_index)), options_.bloom_filter_size != 0 && (max_tables--) > 0));
            objects.resize(0);
            sum_mem = sizeof(uint64_t);
        }
        sum_mem += key_object.second.getSizeInBytes();
        objects.push_back(key_object);
        if (!objects.empty()) {
            result.push_back(MakeFileFromVector(objects, MakeFileName("sst", level_index, result.size(), levels_provider_->GetLevelVersion(level_index)), options_.bloom_filter_size != 0 && (max_tables--) > 0));
        }
        return result;
    }

    std::pair<std::pair<std::shared_ptr<storage::IFile>, std::shared_ptr<IFilterBuilder>>, std::optional<SSTableMetadata>> MakeFileFromVector(const std::vector<std::pair<InternalKey, Value>>& objects, std::string path, bool generate_filter) {
        std::shared_ptr<storage::IFile> file = std::make_shared<storage::BufferedMemoryFile>(path, sstable_sequence_number_++, buffer_pool_, options_.frame_size);
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

    class LeveledLSMStream : public IStream<std::pair<UserKey, Value>> {
       public:
        LeveledLSMStream(std::shared_ptr<IMemTable> mem_table, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory,
                         const std::optional<UserKey>& start_key, const std::optional<UserKey>& end_key)
            : start_key_(start_key), end_key_(end_key) {
            std::vector<std::shared_ptr<IStream<std::pair<InternalKey, Value>>>> sources;
            sources.push_back(mem_table->MakeScan());
            for (size_t lvl = 0; lvl < levels_provider->NumLevels(); ++lvl) {
                if (levels_provider->NumTables(lvl)) {
                    sources.push_back(std::make_shared<LevelLSMStream>(lvl, levels_provider, sstable_factory));
                }
            }
            merge_scan_ = MakeMerger<InternalKey, Value>(sources);
        }

        std::optional<std::pair<UserKey, Value>> Next() {
            std::optional<std::pair<InternalKey, Value>> object;
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
                if (object->first.type == ValueType::kDeletion) {
                    used_ = object->first.user_key;
                }
            } while (object->first.user_key == used_);
            used_ = object->first.user_key;
            return std::make_pair(object->first.user_key, object->second);
        }

       private:
        UserKey used_ = {};
        std::shared_ptr<IMerger<InternalKey, Value>> merge_scan_;
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
    uint64_t sstable_sequence_number_ = 0;
    std::string dir_;
    GranularLsmOptions options_;
    std::shared_ptr<IMemTable> mem_table_;
    std::shared_ptr<ILevelsProvider> levels_provider_;
    std::shared_ptr<ISSTableSerializer> sstable_factory_;
    std::shared_ptr<storage::IReadBufferPool> buffer_pool_;
};

std::unique_ptr<ILSM> MakeLeveledLsm(const std::string& dir, const GranularLsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory) {
    return std::make_unique<LeveledLSMImpl>(dir, options, levels_provider, sstable_factory);
}

}  // namespace invindex::lsm
