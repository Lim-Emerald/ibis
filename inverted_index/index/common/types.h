#pragma once

#include <algorithm>
#include <compare>
#include <cstdint>
#include <cstring>
#include <index/roaring/roaring.hh>
#include <vector>

namespace invindex {

using Token = std::string;

struct TokenId {
    uint32_t block_id : 8;
    uint32_t feature_id : 24;

    TokenId Deserialize(const std::vector<uint8_t> &rev_buffer) {
        std::vector<uint8_t> buffer = rev_buffer;
        std::reverse(buffer.begin(), buffer.end());
        std::memcpy(this, buffer.data(), sizeof(TokenId));
        return *this;
    }

    std::vector<uint8_t> Serialize() const {
        std::vector<uint8_t> buffer(sizeof(TokenId));
        std::memcpy(buffer.data(), this, sizeof(TokenId));
        std::reverse(buffer.begin(), buffer.end());
        return buffer;
    }
};

struct TokenWithBlockId {
    uint32_t block_id;
    Token feature_id;

    TokenWithBlockId Deserialize(const std::vector<uint8_t> &buffer) {
        feature_id = Token(buffer.begin(), --buffer.end());
        block_id = buffer.back();
        return *this;
    }

    std::vector<uint8_t> Serialize() const {
        std::vector<uint8_t> buffer(feature_id.size() + 1);
        std::memcpy(buffer.data(), feature_id.data(), feature_id.size());
        auto t_block_id = static_cast<uint8_t>(block_id);
        std::memcpy(buffer.data() + feature_id.size(), &t_block_id, sizeof(uint8_t));
        return buffer;
    }
};

using PostingList = roaring::Roaring64Map;

using IndexKey = std::vector<uint8_t>;

namespace lsm {

// key provided by the user of the LSM
using UserKey = std::vector<uint8_t>;

using DefaultValue = std::vector<uint8_t>;

using IndexValue = PostingList;

// internal representation used in storage/merges
// (e.g., user_key plus sequence number and operation type).
enum class ValueType : uint8_t { kValue = 0x0, kDeletion = 0x1 };

struct InternalKey {
    UserKey user_key;
    // uint64_t sequence_number = 0;  // monotonic increasing
    // ValueType type = ValueType::kValue;

    // Order matches InternalKeyComparator: user_key ascending, then
    // sequence_number descending (newer first), then type ascending.
    std::strong_ordering operator<=>(const InternalKey &key) const {
        {
            std::strong_ordering key_cmp = user_key <=> key.user_key;
            return key_cmp;
            // if (key_cmp != std::strong_ordering::equal) {
            //     return key_cmp;
            // }
        }
        // {
        //     std::strong_ordering seq_cmp = key.sequence_number <=> sequence_number;
        //     if (seq_cmp != std::strong_ordering::equal) {
        //         return seq_cmp;
        //     }
        // }
        // {
        //     std::strong_ordering type_cmp = static_cast<uint8_t>(type) <=> static_cast<uint8_t>(key.type);
        //     return type_cmp;
        // }
    }

    bool operator==(const InternalKey &key) const { return *this <=> key == 0; }

    uint64_t Size() { return user_key.size(); }
};

}  // namespace lsm

}  // namespace invindex
