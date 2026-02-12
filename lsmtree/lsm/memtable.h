#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include <lsm/common/stream.h>
#include <lsm/common/types.h>

namespace lsm {

class IMemTable {
   public:
    enum class GetKind { kNotFound, kDeletion, kFound };
    // sequence_number is a monotonically increasing counter within the LSM.
    // Internal key ordering is defined as: (user_key ascending, sequence_number descending).
    // If Add is called without monotonically increasing sequence_number, behavior is undefined.
    // It is recommended to throw an exception/assert to simplify debugging.
    virtual void Add(uint64_t sequence_number, const UserKey& user_key, const Value& value) = 0;

    // Write a tombstone for user_key at the given sequence_number.
    // Tombstones are visible in scans (as internal-key entries) and affect Get semantics.
    virtual void Delete(uint64_t sequence_number, const UserKey& user_key) = 0;

    // Returns the latest entry kind for user_key within this MemTable.
    // kFound: out_value is set to the latest value
    // kDeletion: the latest entry is a tombstone
    // kNotFound: key not present in this MemTable
    // If sequence_number is specified, returns the entry with the largest sequence_number <= given sequence_number
    virtual GetKind Get(const UserKey& user_key, Value* out_value, uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const = 0;

    // Iterator over (internal_key, value) in internal key order
    // (user_key ascending, sequence_number descending). Returning internal keys
    // allows simple k-way merge between MemTable and SSTable iterators. Tombstones
    // are returned as entries with internal_key corresponding to a deletion tag.
    virtual std::shared_ptr<IStream<std::pair<InternalKey, Value>>> MakeScan() const = 0;

    // Returns approximate memory usage in bytes for this MemTable, including
    // stored keys/values and internal metadata. Intended for heuristics (e.g.,
    // flush triggers), not exact accounting. Should be non-decreasing with
    // inserts (deletes are tombstones and also consume memory).
    virtual uint64_t ApproximateMemoryUsage() const = 0;

    virtual ~IMemTable() = default;
};

// Minimal constructor for a default std::map-backed MemTable.
std::shared_ptr<IMemTable> MakeMemTable(uint32_t max_level);

}  // namespace lsm
