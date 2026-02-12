#pragma once

#include <memory>
#include <vector>

#include "lsm/common/types.h"

namespace lsm {

// Filter operates on user keys (raw byte sequences), not on internal keys.
// MayContain may return false positives but must not return false negatives
// for keys that were added via the corresponding builder.
class IFilter {
   public:
    virtual bool MayContain(const UserKey& key) const = 0;

    virtual ~IFilter() = default;
};

// Builder constructs a filter incrementally from user keys.
// After calling any Finish* method, the builder is considered consumed and
// further calls to Add/Finish* are undefined (implementations may throw/assert).
class IFilterBuilder {
   public:
    virtual void Add(const UserKey& key) = 0;
    virtual std::vector<uint8_t> Serialize() = 0;

    virtual ~IFilterBuilder() = default;
};

class IFilterDeserializer {
   public:
    // Create a filter instance from its opaque serialized representation.
    // Implementations may throw if the input is malformed or unsupported.
    virtual std::unique_ptr<IFilter> Deserialize(const std::vector<uint8_t>& data) const = 0;

    virtual ~IFilterDeserializer() = default;
};

// Create bloom filter components
std::shared_ptr<IFilterBuilder> MakeFilterBuilder(size_t bit_count, size_t hash_count);
std::unique_ptr<IFilterDeserializer> MakeFilterDeserializer();

}  // namespace lsm
