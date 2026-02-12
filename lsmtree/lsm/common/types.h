#pragma once

#include <compare>
#include <cstdint>
#include <cstring>
#include <vector>

namespace lsm {

// key provided by the user of the LSM
using UserKey = std::vector<uint8_t>;

using Value = std::vector<uint8_t>;

// internal representation used in storage/merges
// (e.g., user_key plus sequence number and operation type).
enum class ValueType : uint8_t { kValue = 0x0, kDeletion = 0x1 };

struct InternalKey {
    UserKey user_key;
    uint64_t sequence_number = 0;  // monotonic increasing
    ValueType type = ValueType::kValue;

    // Order matches InternalKeyComparator: user_key ascending, then
    // sequence_number descending (newer first), then type ascending.
    std::strong_ordering operator<=>(const InternalKey &key) const {
        {
            std::strong_ordering key_cmp = user_key <=> key.user_key;
            if (key_cmp != std::strong_ordering::equal) {
                return key_cmp;
            }
        }
        {
            std::strong_ordering seq_cmp = key.sequence_number <=> sequence_number;
            if (seq_cmp != std::strong_ordering::equal) {
                return seq_cmp;
            }
        }
        {
            std::strong_ordering type_cmp = static_cast<uint8_t>(type) <=> static_cast<uint8_t>(key.type);
            return type_cmp;
        }
    }

    bool operator==(const InternalKey &key) const { return *this <=> key == 0; }
};

}  // namespace lsm
