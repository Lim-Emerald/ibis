#pragma once

#include <lsm/common/stream.h>
#include <lsm/common/types.h>
#include <lsm/sstable.h>
#include <lsm/storage/file.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>

namespace lsm {

// Minimal LSM-Tree interface (single-threaded).
//
// Semantics:
// - Keys and values are arbitrary byte sequences (user keys). Comparison of keys
//   is lexicographic over bytes.
// - Put/Delete calls are applied in the order they are invoked. Implementations
//   may assign internal sequence numbers to preserve this order.
// - Get returns the latest (most recently written) live value for the given user key
//   across the entire LSM (considering in-memory and on-disk state). If the latest
//   entry is a deletion tombstone or the key is absent, returns std::nullopt.
// - Concurrency: this interface is single-threaded; concurrent calls are not supported.
class ILSM {
   public:
    // Insert or overwrite the value for user_key. Subsequent Get should observe this
    // value unless a newer Delete/Put overrides it.
    virtual void Put(const UserKey& user_key, const Value& value) = 0;

    // Write a deletion tombstone for user_key. Subsequent Get should return std::nullopt
    // until a newer Put for the same key appears.
    virtual void Delete(const UserKey& user_key) = 0;

    // Lookup the latest live value for user_key. Returns std::nullopt if the key is absent
    // or its newest entry is a deletion tombstone.
    // If sequence_number is specified, returns the entry with the largest sequence_number <= given sequence_number
    virtual std::optional<Value> Get(const UserKey& user_key, uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const = 0;

    // Range scan: returns iterator over live key-value pairs in [start_key, end_key) range.
    // Keys are returned in ascending order. Tombstones are filtered out.
    // Only the latest version of each key is returned.
    //
    // Parameters:
    //   start_key: inclusive lower bound (std::nullopt means -infinity)
    //   end_key: exclusive upper bound (std::nullopt means +infinity)
    //   sequence_number: return only entries with sequence_number <= this value
    //                    (defaults to max = latest version)
    virtual std::shared_ptr<IStream<std::pair<UserKey, Value>>> Scan(const std::optional<UserKey>& start_key, const std::optional<UserKey>& end_key,
                                                                     uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const = 0;

    virtual uint64_t GetCurrentSequenceNumber() const = 0;

    virtual ~ILSM() = default;
};

// Used for range queries and overlapping detection in leveled compaction.
struct SSTableMetadata {
    UserKey min_key;  // Smallest user key in this SSTable
    UserKey max_key;  // Largest user key in this SSTable
    uint64_t file_size = 0;

    // Returns true if this SSTable's key range overlaps with [start_key, end_key]
    bool Overlaps(const UserKey& start_key, const UserKey& end_key) const { return min_key <= end_key && max_key >= start_key; }
};

// Pluggable levels provider that owns and organizes SSTables per level.
// Supports insertion/erasure at arbitrary indices with index shifting.
// Levels provider stores opaque SSTable byte images per level/position and can
// reconstruct readers via an injected ISSTableFactory. This keeps serialization
// concerns in user code, not inside the provider.
//
// Also maintains metadata (min_key, max_key)
// for each SSTable to enable efficient overlap detection.
class ILevelsProvider {
   public:
    virtual size_t NumLevels() const = 0;

    virtual size_t NumTables(size_t level_index) const = 0;

    virtual std::shared_ptr<const storage::IFile> GetTableFile(size_t level_index, size_t table_index) const = 0;

    // Insert file at index (0..NumTables), shifting subsequent to the right.
    // Metadata is optional; if not provided, empty metadata will be stored.
    virtual void InsertTableFile(size_t level_index, size_t table_index, std::shared_ptr<const storage::IFile> file, std::shared_ptr<const storage::IFile> bloom_filter,
                                 std::optional<SSTableMetadata> metadata = std::nullopt) = 0;

    // Erase table at given index, shifting subsequent indices to the left
    virtual void EraseTable(size_t level_index, size_t table_index) = 0;

    virtual std::optional<SSTableMetadata> GetTableMetadata(size_t level_index, size_t table_index) const = 0;

    virtual std::shared_ptr<const storage::IFile> GetTableBloomFilter(size_t level_index, size_t table_index) const = 0;

    virtual ~ILevelsProvider() = default;
};

// LSM-tree configuration for compaction with leveled granularity: single SSTable per level
// After compaction each level contains at most ONE SSTable file of arbitrary size
// Simple, predictable structure with low write amplification
struct LsmOptions {
    uint64_t frame_size = 4096;
    uint64_t buffer_pool_size = 64ull * 1024 * 1024;
    uint64_t memtable_bytes = 64ull * 1024 * 1024;
    uint32_t max_level_skip_list = 20;
    // Compaction trigger: start merging a level when it reaches this many files
    // (each level can contain at most 'compaction_trigger_files - 1' files)
    uint32_t compaction_trigger_files = 2;
};

// Granular LSM-tree configuration: multiple size-bounded SSTables per level
// Each level can store many SSTables with exponentially growing capacity
// Better read performance and more granular compaction at cost of write amplification
struct GranularLsmOptions {
    uint64_t frame_size = 4096;
    uint64_t buffer_pool_size = 64ull * 1024 * 1024;
    uint64_t memtable_bytes = 64ull * 1024 * 1024;
    // Target SSTable size
    // Actual files may be up to max_sstable_size plus size of one key
    uint64_t max_sstable_size = 128ull * 1024 * 1024;
    uint32_t max_level_skip_list = 20;
    // L0 base capacity: maximum number of files on L0 before compaction
    // Level N capacity = l0_capacity * (level_size_multiplier ^ N)
    // Example with l0_capacity=4, multiplier=10: L0=4, L1=40, L2=400 files
    uint32_t l0_capacity = 2;
    // Capacity growth factor per level
    uint32_t level_size_multiplier = 2;
    uint32_t bloom_filter_size = 4ull * 1024 * 1024;
    uint32_t bloom_filter_hash_count = 23;
};

std::shared_ptr<ILevelsProvider> MakeLevelsProvider();

// Create a LSM instance (single file per level)
std::unique_ptr<ILSM> MakeLsm(const LsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory, uint64_t* read_bytes = new uint64_t());

// Create a granular LSM instance (multiple size-bounded files per level)
std::unique_ptr<ILSM> MakeGranularLsm(const GranularLsmOptions& options, std::shared_ptr<ILevelsProvider> levels_provider, std::shared_ptr<ISSTableSerializer> sstable_factory, uint64_t* read_bytes = new uint64_t());

}  // namespace lsm
