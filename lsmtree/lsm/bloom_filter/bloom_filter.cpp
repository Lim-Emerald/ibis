#include "lsm/bloom_filter/bloom_filter.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>

namespace lsm {

class FilterImpl : public IFilter {
   public:
    FilterImpl(size_t bit_count, size_t hash_count) : bit_count_(bit_count) {
        bitset_.resize((bit_count + 7) / 8, 0);
        for (size_t p = 5; p_.size() < hash_count; ++p) {
            bool prime = true;
            for (size_t d = 2; d * d <= p; ++d) {
                if (p % d == 0) {
                    prime = false;
                    break;
                }
            }
            if (prime) {
                p_.push_back(p);
            }
        }
    }

    FilterImpl(const std::vector<uint8_t>& data) {
        std::memcpy(&bit_count_, data.data(), sizeof(size_t));
        bitset_.resize((bit_count_ + 7) / 8, 0);
        std::memcpy(bitset_.data(), data.data() + sizeof(size_t), (bit_count_ + 7) / 8);
        for (size_t ind = sizeof(size_t) + (bit_count_ + 7) / 8; ind < data.size(); ++ind) {
            p_.push_back(data[ind]);
        }
    }

    void Add(const UserKey& key) {
        for (auto& p : p_) {
            uint64_t bit = 0, pw = 1;
            for (auto& c : key) {
                bit += pw * static_cast<uint64_t>(c);
                pw *= static_cast<uint64_t>(p);
            }
            bit %= bit_count_;
            bitset_[bit / 8ull] |= (1 << (bit % 8ull));
        }
    }

    bool MayContain(const UserKey& key) const {
        for (auto& p : p_) {
            uint64_t bit = 0, pw = 1;
            for (auto& c : key) {
                bit += pw * static_cast<uint64_t>(c);
                pw *= static_cast<uint64_t>(p);
            }
            bit %= bit_count_;
            if (!(bitset_[bit / 8ull] & (1 << (bit % 8ull)))) {
                return false;
            }
        }
        return true;
    }

    std::vector<uint8_t> Serialize() {
        std::vector<uint8_t> buffer(sizeof(size_t) + bitset_.size() + p_.size());
        std::memcpy(buffer.data(), &bit_count_, sizeof(size_t));
        std::memcpy(buffer.data() + sizeof(size_t), bitset_.data(), bitset_.size());
        std::memcpy(buffer.data() + sizeof(size_t) + bitset_.size(), p_.data(), p_.size());
        return buffer;
    }

    virtual ~FilterImpl() = default;

   private:
    size_t bit_count_;
    std::vector<uint8_t> bitset_;
    std::vector<uint8_t> p_;
};

class FilterBuilderImpl : public IFilterBuilder {
   public:
    FilterBuilderImpl(size_t bit_count, size_t hash_count) : filter_(bit_count, hash_count) {}

    void Add(const UserKey& key) { filter_.Add(key); }

    std::vector<uint8_t> Serialize() { return filter_.Serialize(); }

    virtual ~FilterBuilderImpl() = default;

   private:
    FilterImpl filter_;
};

class FilterDeserializerImpl : public IFilterDeserializer {
   public:
    std::unique_ptr<IFilter> Deserialize(const std::vector<uint8_t>& data) const { return std::make_unique<FilterImpl>(data); }

    virtual ~FilterDeserializerImpl() = default;
};

std::shared_ptr<IFilterBuilder> MakeFilterBuilder(size_t bit_count, size_t hash_count) { return std::make_shared<FilterBuilderImpl>(bit_count, hash_count); }

std::unique_ptr<IFilterDeserializer> MakeFilterDeserializer() { return std::make_unique<FilterDeserializerImpl>(); }

}  // namespace lsm
