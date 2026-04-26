#pragma once

#include <compare>
#include <cstdint>
#include <cstring>
#include <index/roaring/roaring.hh>
#include <iostream>
#include <memory>
#include <vector>

namespace invindex {

namespace lsm {

    class IUserKey {
    public:
        virtual void Deserialize(const std::vector<char> &buffer) = 0;

        virtual std::vector<char> Serialize() const = 0;

        virtual uint64_t GetSerializedSize() const = 0;

        virtual std::strong_ordering operator<=>(const std::shared_ptr<IUserKey>& user_key) const = 0;

        bool operator==(const std::shared_ptr<IUserKey>& user_key) const {
            return (*this <=> user_key) == std::strong_ordering::equal;
        }

        virtual ~IUserKey() = default;
    };

    using UserKey = std::shared_ptr<IUserKey>;

    class IValue {
    public:
        virtual void Deserialize(const std::vector<char> &buffer) = 0;

        virtual std::vector<char> Serialize() const = 0;

        virtual uint64_t GetSerializedSize() const = 0;

        virtual void Merge(const std::shared_ptr<IValue>& value) = 0;

        virtual void RunOptimize() = 0;

        virtual bool Empty() const = 0;

        virtual ~IValue() = default;
    };

    using Value = std::shared_ptr<IValue>;

    // internal representation used in storage/merges
    // (e.g., user_key plus sequence number and operation type).
    enum class ValueType : uint8_t { kValue = 0x0, kDeletion = 0x1 };

}  // namespace lsm

using Token = std::string;

}  // namespace invindex
