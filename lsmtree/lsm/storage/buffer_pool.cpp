#include "lsm/storage/buffer_pool.h"

#include <cstdint>
#include <cstring>
#include <fstream>
#include <list>
#include <memory>
#include <stack>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace lsm::storage {

class ReadFrameProvider : public IReadFrameProvider {
   public:
    ReadFrameProvider(const std::string& dir) : dir_(dir) {}

    void Start(uint32_t table_id) override {
        file_ = std::make_unique<std::ifstream>(dir_ + "/sstable_" + std::to_string(table_id), std::ios::binary);
    }

    std::shared_ptr<IFrame> GetFrame(FrameId id) override {
        if (!file_) {
            Start(id.table_id);
        }
        std::vector<uint8_t> buffer(kFrameSize);
        file_->seekg(id.page_id * kFrameSize);
        file_->read(reinterpret_cast<char*>(buffer.data()), buffer.size());
        return std::make_shared<ReadFrame>(buffer);
    }

    void Finish() override {
        if (file_ && file_->is_open()) {
            file_->close();
            file_ = nullptr;
        }
    }

   private:
    class ReadFrame : public IFrame {
    public:
        ReadFrame(std::vector<uint8_t> storage) : storage_(storage) {}

        uint8_t* Data() override { return reinterpret_cast<uint8_t*>(storage_.data()); }
        uint64_t Size() override { return storage_.size(); }

        void MarkDirty() override {}

    private:
        std::vector<uint8_t> storage_;
    };

    std::string dir_;
    std::unique_ptr<std::ifstream> file_ = nullptr;
};

std::shared_ptr<IReadFrameProvider> MakeReadFrameProvider(const std::string& dir) { return std::make_shared<ReadFrameProvider>(dir); }

class ReadBufferPool : public IReadBufferPool {
   public:
    explicit ReadBufferPool(std::shared_ptr<IReadFrameProvider> frame_provider, uint64_t entries_limit)
        : frame_provider_(frame_provider), hot_limit_(entries_limit / 2), entries_limit_(entries_limit) {}

    std::shared_ptr<IFrame> GetFrame(FrameId id) override {
        uint64_t uid;
        std::memcpy(&uid, &id, sizeof(uid));
        if (hot_iterators_.contains(uid)) {
            return hot_iterators_[uid]->second;
        } else if (cold_iterators_.contains(uid)) {
            auto cold_frame = *cold_iterators_[uid];
            cold_list_.erase(cold_iterators_[uid]);
            cold_iterators_.erase(uid);
            if (hot_list_.size() == hot_limit_) {
                auto hot_frame = hot_list_.back();
                hot_list_.pop_back();
                hot_iterators_.erase(hot_frame.first);
                cold_list_.push_front(hot_frame);
                cold_iterators_[hot_frame.first] = cold_list_.begin();
            }
            hot_list_.push_front(cold_frame);
            hot_iterators_[cold_frame.first] = hot_list_.begin();
            return cold_frame.second;
        } else {
            auto new_frame = std::pair(uid, frame_provider_->GetFrame(id));
            if (cold_list_.size() == entries_limit_ - hot_list_.size()) {
                auto cold_frame = cold_list_.back();
                cold_list_.pop_back();
                cold_iterators_.erase(cold_frame.first);
                std::stack<std::pair<uint64_t, std::shared_ptr<IFrame>>> return_back;
                while (cold_frame.second.use_count() > 1) {
                    return_back.push(cold_frame);
                    cold_frame = cold_list_.back();
                    cold_list_.pop_back();
                    cold_iterators_.erase(cold_frame.first);
                }
                while (!return_back.empty()) {
                    cold_list_.push_back(return_back.top());
                    cold_iterators_[return_back.top().first] = --cold_list_.end();
                    return_back.pop();
                }
            }
            cold_list_.push_front(new_frame);
            cold_iterators_[new_frame.first] = cold_list_.begin();
            return new_frame.second;
        }
    }

    std::vector<std::shared_ptr<IFrame>> GetFrames(uint32_t table_id, uint32_t l, uint32_t r) override {
        std::vector<std::shared_ptr<IFrame>> result;
        result.reserve(r - l + 1);
        for (uint32_t ind = l; ind <= r; ++ind) {
            result.push_back(GetFrame({table_id, ind}));
        }
        frame_provider_->Finish();
        return result;
    }

   private:
    std::list<std::pair<uint64_t, std::shared_ptr<IFrame>>> hot_list_, cold_list_;
    std::unordered_map<uint64_t, std::_List_iterator<std::pair<uint64_t, std::shared_ptr<IFrame>>>> hot_iterators_, cold_iterators_;
    std::shared_ptr<IReadFrameProvider> frame_provider_;
    uint64_t hot_limit_;
    uint64_t entries_limit_;
};

std::shared_ptr<IReadBufferPool> MakeReadBufferPool(std::string dir, uint64_t entries_limit) { return std::make_shared<ReadBufferPool>(MakeReadFrameProvider(dir), entries_limit); }

}  // namespace lsm::storage
