#pragma once

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include <lsm/common/stream.h>
#include <lsm/common/types.h>
#include <lsm/lsm.h>
#include <lsm/storage/file.h>

namespace lsm {

// Test implementation of ILevelsProvider that stores SSTables in memory
class TestVectorLevelsProvider final : public ILevelsProvider {
   public:
    size_t NumLevels() const override { return levels_.size(); }

    size_t NumTables(size_t level_index) const override {
        if (level_index >= levels_.size()) {
            return 0;
        }
        return levels_[level_index].size();
    }

    std::shared_ptr<const storage::IFile> GetTableFile(size_t level_index, size_t table_index) const override {
        if (visit_counters_.size() <= level_index) {
            visit_counters_.resize(level_index + 1, 0);
        }
        ++visit_counters_[level_index];
        ++total_visits_;

        auto result = levels_.at(level_index).at(table_index);
        total_bytes_read_ += result->Size();

        return result;
    }

    void InsertTableFile(size_t level_index, size_t table_index, std::shared_ptr<const storage::IFile> file, std::shared_ptr<const storage::IFile> bloom_filter,
                         std::optional<SSTableMetadata> metadata = std::nullopt) override {
        // Auto-create levels as needed
        if (levels_.size() <= level_index) {
            levels_.resize(level_index + 1);
            filters_.resize(level_index + 1);
            metadata_.resize(level_index + 1);
        }
        auto& v = levels_[level_index];
        auto& f = filters_[level_index];
        auto& m = metadata_[level_index];
        if (table_index > v.size()) {
            table_index = v.size();
        }
        if (file) {
            total_bytes_inserted_ += file->Size();
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

    void ResetVisitCounters() {
        total_visits_ = 0;
        std::fill(visit_counters_.begin(), visit_counters_.end(), 0);
    }

    uint64_t TotalVisits() const { return total_visits_; }

    const std::vector<uint64_t>& VisitsByLevel() const { return visit_counters_; }

    void ResetBytesInserted() { total_bytes_inserted_ = 0; }

    uint64_t TotalBytesInserted() const { return total_bytes_inserted_; }

    uint64_t TotalBytesRead() const { return total_bytes_read_; }

   private:
    std::vector<std::vector<std::shared_ptr<const storage::IFile>>> levels_;
    std::vector<std::vector<std::shared_ptr<const storage::IFile>>> filters_;
    std::vector<std::vector<std::optional<SSTableMetadata>>> metadata_;
    mutable std::vector<uint64_t> visit_counters_;
    mutable uint64_t total_visits_ = 0;
    mutable uint64_t total_bytes_inserted_ = 0;
    mutable uint64_t total_bytes_read_ = 0;
};

// Generate a random key with length between min_len and max_len
inline UserKey GenerateRandomKey(std::mt19937& rng, int min_len = 7, int max_len = 11) {
    std::uniform_int_distribution<int> len_dist(min_len, max_len);
    std::uniform_int_distribution<uint8_t> byte_dist(0, 255);

    int length = len_dist(rng);
    UserKey key;
    key.reserve(length);
    for (int i = 0; i < length; i++) {
        key.push_back(byte_dist(rng));
    }
    return key;
}

// Collect all elements from a stream into a vector
template <typename T>
std::vector<T> CollectAll(IStream<T>& stream) {
    std::vector<T> result;
    while (auto val = stream.Next()) {
        result.push_back(*val);
    }
    return result;
}

}  // namespace lsm
