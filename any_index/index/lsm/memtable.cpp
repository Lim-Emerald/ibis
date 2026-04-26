#include "index/common/types.h"
#include "index/lsm/memtable.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <random>
#include <vector>


namespace invindex::lsm {

namespace {

class IndexMemTableImpl : public IMemTable {
   public:
    IndexMemTableImpl(uint64_t max_level) : max_level_(max_level) {
        head_ = std::make_shared<Node>();
        head_->links.resize(max_level_, nullptr);
        std::random_device device;
        random_generator_.seed(device());
    }

    void Add(const UserKey& user_key, const Value& value) override {
        // std::cout << "Mem Add\n";
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && *cur->links[level - 1]->key < user_key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && *cur->links[0]->key == user_key) {
            amu_ -= cur->links[0]->value->GetSerializedSize();
            cur->links[0]->value->Merge(value);
            cur->links[0]->value->RunOptimize();
            amu_ += cur->links[0]->value->GetSerializedSize();
            return;
        }
        auto node = std::make_shared<Node>();
        node->key = {user_key};
        node->value = value;
        InsertNode(node);
        // std::cout << "Mem Add END\n";
    }

    GetKind Get(const UserKey& user_key, Value& out_value) const override {
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && *cur->links[level - 1]->key < user_key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && *cur->links[0]->key == user_key) {
            out_value = cur->links[0]->value;
            return GetKind::kFound;
        } else {
            return GetKind::kNotFound;
        }
    }

    std::shared_ptr<IStream<std::pair<UserKey, Value>>> MakeScan() const override { return std::make_shared<MemTableStream>(head_); }

    uint64_t ApproximateMemoryUsage() const override { return amu_; }

    virtual ~IndexMemTableImpl() = default;

   private:
    struct Node {
        UserKey key;
        Value value;
        std::vector<std::shared_ptr<Node>> links;
    };

    class MemTableStream : public IStream<std::pair<UserKey, Value>> {
       public:
        MemTableStream(std::shared_ptr<Node> head) : cur_(head->links[0]) {}

        std::optional<std::pair<UserKey, Value>> Next() {
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
            if (cur->links[level - 1] && *cur->links[level - 1]->key < node->key) {
                cur = cur->links[level - 1];
            } else {
                --level;
                if (level < node->links.size()) {
                    node->links[level] = cur->links[level];
                    cur->links[level] = node;
                }
            }
        }
        amu_ += node->key->GetSerializedSize() + node->value->GetSerializedSize() + node->links.size() * sizeof(Node*);
    }

   private:
    uint64_t amu_ = 0;
    uint64_t max_level_;
    std::shared_ptr<Node> head_;
    std::mt19937 random_generator_;
};

}  // namespace

std::shared_ptr<IMemTable> MakeIndexMemTable(uint32_t max_level) { return std::make_shared<IndexMemTableImpl>(max_level); }

}  // namespace invindex::lsm
