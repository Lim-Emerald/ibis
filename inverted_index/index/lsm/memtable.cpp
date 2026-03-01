#include "index/lsm/memtable.h"

#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <random>
#include <utility>
#include <vector>
#include "index/common/types.h"

namespace invindex::lsm {

class MemTableImpl : public IMemTable {
   public:
    MemTableImpl(uint64_t max_level) : max_level_(max_level) {
        head_ = std::make_shared<Node>();
        head_->links.resize(max_level_, nullptr);
        std::random_device device;
        random_generator_.seed(device());
    }

    void Add(const UserKey& user_key, const Value& value) {
        InternalKey key = {user_key, ValueType::kValue};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && cur->links[0]->key.user_key == user_key) {
            amu_ -= cur->links[0]->value.getSizeInBytes();
            if (cur->links[0]->key.type == ValueType::kValue) {
                cur->links[0]->value |= value;
                cur->links[0]->value.runOptimize();
            } else {
                cur->links[0]->key.type = ValueType::kValue;
                cur->links[0]->value = value;
            }
            amu_ += cur->links[0]->value.getSizeInBytes();
            return;
        }
        auto node = std::make_shared<Node>();
        node->key = {user_key, ValueType::kValue};
        node->value = value;
        InsertNode(node);
    }

    void Delete(const UserKey& user_key) {
        InternalKey key = {user_key, ValueType::kValue};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && cur->links[0]->key.user_key == user_key) {
            if (cur->links[0]->key.type == ValueType::kValue) {
                amu_ -= cur->links[0]->value.getSizeInBytes();
                cur->links[0]->key.type = ValueType::kDeletion;
                cur->links[0]->value = {};
                amu_ += cur->links[0]->value.getSizeInBytes();
            }
            return;
        }
        auto node = std::make_shared<Node>();
        node->key = {user_key, ValueType::kDeletion};
        node->value = {};
        InsertNode(node);
    }

    GetKind Get(const UserKey& user_key, Value* out_value) const {
        *out_value = {};
        InternalKey key = {user_key, ValueType::kValue};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && cur->links[0]->key.user_key == user_key) {
            if (cur->links[0]->key.type == ValueType::kValue) {
                *out_value = cur->links[0]->value;
                return GetKind::kFound;
            } else {
                return GetKind::kDeletion;
            }
        } else {
            return GetKind::kNotFound;
        }
    }

    std::shared_ptr<IStream<std::pair<InternalKey, Value>>> MakeScan() const { return std::make_shared<MemTableStream>(head_); }

    uint64_t ApproximateMemoryUsage() const { return amu_; }

    virtual ~MemTableImpl() = default;

   private:
    struct Node {
        InternalKey key;
        Value value;
        std::vector<std::shared_ptr<Node>> links;
    };

    class MemTableStream : public IStream<std::pair<InternalKey, Value>> {
       public:
        MemTableStream(std::shared_ptr<Node> head) : cur_(head->links[0]) {}

        std::optional<std::pair<InternalKey, Value>> Next() {
            if (!cur_) {
                return std::nullopt;
            }
            auto result = std::make_pair(cur_->key, cur_->value);
            cur_ = cur_->links[0];
            return result;
        }

       private:
        std::shared_ptr<Node> cur_;
    };

    void InsertNode(const std::shared_ptr<Node>& node) {
        std::uniform_int_distribution<int> hit(0, 1);
        do {
            node->links.push_back(nullptr);
        } while (node->links.size() < max_level_ && hit(random_generator_));
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < node->key) {
                cur = cur->links[level - 1];
            } else {
                --level;
                if (level < node->links.size()) {
                  node->links[level] = cur->links[level];
                  cur->links[level] = node;
                }
            }
        }
        amu_ += node->key.user_key.size() + sizeof(node->key.type) + node->value.getSizeInBytes() + node->links.size() * sizeof(Node*);
    }

   private:
    uint64_t amu_ = 0;
    uint64_t max_level_;
    std::shared_ptr<Node> head_;
    std::mt19937 random_generator_;
};

std::shared_ptr<IMemTable> MakeMemTable(uint32_t max_level) { return std::make_shared<MemTableImpl>(max_level); }

}  // namespace invindex::lsm
