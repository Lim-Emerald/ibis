#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include <lsm/common/stream.h>
#include <lsm/common/types.h>
#include <lsm/storage/file.h>

namespace lsm {

// Note: internal_key is not the raw user key. It encodes user_key plus a tag
// containing sequence number and value type (e.g., Put/Delete).
// SSTable stores internal keys; LSM composes user operations
// into internal keys before writing.

class ISSTableBuilder {
   public:
    // Precondition: calls to Add must provide entries in strictly increasing
    // internal_key order. If violated, implementation may assert, ignore, or produce a corrupted table.
    virtual void Add(const InternalKey& internal_key, const Value& value) = 0;

    // Finalize table creation. Subsequent calls are undefined.
    virtual void Finish() = 0;

    virtual ~ISSTableBuilder() = default;
};

class ISSTableReader {
   public:
    // Iterator over (internal_key, value) in internal key order
    // (user_key ascending, sequence descending). Returning internal keys
    // allows simple k-way merge between MemTable and SSTable iterators.
    virtual std::shared_ptr<IStream<std::pair<InternalKey, Value>>> MakeScan() const = 0;

    // Get returns the newest entry kind for user_key within THIS table only.
    // Semantics mirror IMemTable::Get:
    // - kFound: newest entry is a value; writes the value to out_value
    // - kDeletion: newest entry is a tombstone
    // - kNotFound: user_key does not appear in this table
    // If sequence_number is specified, returns the entry with the largest sequence_number <= given sequence_number
    enum class GetKind { kNotFound, kDeletion, kFound };
    virtual GetKind Get(const UserKey& user_key, Value* out_value, uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const = 0;

    virtual ~ISSTableReader() = default;
};

// Factory to create SSTable readers and builders that write into a byte buffer.
// Implementations define the on-disk/in-memory byte format. LSM should not
// assume any specific encoding and must go through this factory.
// File-backed factory: preferred scalable backend (auto-grows via IFile)
class ISSTableSerializer {
   public:
    // Read entire table image from file starting at offset 0 up to file->Size().
    virtual std::shared_ptr<ISSTableReader> FromFile(const std::shared_ptr<const storage::IFile>& file) const = 0;

    // Prepare to write a table image into an empty file from offset 0.
    // Implementations may truncate the file to 0.
    virtual std::unique_ptr<ISSTableBuilder> NewFileBuilder(const std::shared_ptr<storage::IFile>& file) const = 0;

    virtual ~ISSTableSerializer() = default;
};

std::shared_ptr<ISSTableSerializer> MakeSSTableFileFactory();

}  // namespace lsm
