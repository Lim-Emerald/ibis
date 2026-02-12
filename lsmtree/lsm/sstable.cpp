#include "lsm/sstable.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>
#include <vector>

namespace lsm {
namespace {

class FileSSTableReader final : public ISSTableReader {
   public:
    explicit FileSSTableReader(std::shared_ptr<const storage::IFile> file) : page_(std::make_shared<const SSTableViewer>(std::move(file))) {}

    std::shared_ptr<IStream<std::pair<InternalKey, Value>>> MakeScan() const override { return std::make_shared<SSTableStream>(page_); }

    GetKind Get(const UserKey& user_key, Value* out_value, uint64_t sequence_number = std::numeric_limits<uint64_t>::max()) const override {
        *out_value = {};
        InternalKey internal_key = {user_key, sequence_number, ValueType::kValue};
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            if (page_->GetObject(m - 1).first < internal_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1 || page_->GetObject(r - 1).first.user_key != user_key) {
            return GetKind::kNotFound;
        } else if (page_->GetObject(r - 1).first.type == ValueType::kValue) {
            *out_value = page_->GetObject(r - 1).second;
            return GetKind::kFound;
        } else {
            return GetKind::kDeletion;
        }
    }

   private:
    class SSTableViewer {
       public:
        explicit SSTableViewer(std::shared_ptr<const storage::IFile> file) : file_(std::move(file)) { std::memcpy(&object_count_, file_->Read(0, sizeof(uint64_t)).data(), sizeof(uint64_t)); }

        std::pair<InternalKey, Value> GetObject(size_t ind) const {
            if (ind >= object_count_) {
                throw "SSTablePage: out of bounds";
            }
            std::pair<uint64_t, uint64_t> offsets;
            std::memcpy(&offsets, file_->Read((2 * ind + 1) * sizeof(uint64_t), 2 * sizeof(uint64_t)).data(), 2 * sizeof(uint64_t));

            std::pair<InternalKey, Value> object;
            std::memcpy(&object.first.sequence_number, file_->Read(file_->Size() - offsets.first, sizeof(uint64_t)).data(), sizeof(uint64_t));

            object.first.user_key = file_->Read(file_->Size() - offsets.first + sizeof(uint64_t), offsets.first - offsets.second - sizeof(uint64_t));

            uint64_t bytes_value = offsets.second;
            if (ind) {
                uint64_t ofs;
                std::memcpy(&ofs, file_->Read((2 * ind - 1) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));
                bytes_value -= ofs;
            }
            if (bytes_value) {
                object.first.type = ValueType::kValue;
                object.second = file_->Read(file_->Size() - offsets.second, bytes_value);
            } else {
                object.first.type = ValueType::kDeletion;
            }
            return object;
        }

        size_t GetObjectCount() const { return object_count_; }

       private:
        uint64_t object_count_;
        std::shared_ptr<const storage::IFile> file_;
    };

    class SSTableStream : public IStream<std::pair<InternalKey, Value>> {
       public:
        SSTableStream(std::shared_ptr<const SSTableViewer> page) : page_(page) {}

        std::optional<std::pair<InternalKey, Value>> Next() {
            if (ind_ == page_->GetObjectCount()) {
                return std::nullopt;
            }
            return page_->GetObject(ind_++);
        }

       private:
        size_t ind_ = 0;
        std::shared_ptr<const SSTableViewer> page_;
    };

   private:
    std::shared_ptr<const SSTableViewer> page_;
};

class FileSSTableBuilder final : public ISSTableBuilder {
   public:
    explicit FileSSTableBuilder(std::shared_ptr<storage::IFile> file) : file_(file) {}

    void Add(const InternalKey& k, const Value& v) override { objects_.push_back({k, v}); }

    void Finish() override {
        uint64_t mem = (2 * objects_.size() + 1) * sizeof(uint64_t);
        for (auto& object : objects_) {
            mem += object.first.user_key.size() + object.second.size() + sizeof(uint64_t);
        }
        std::vector<uint8_t> buffer_file(mem);

        std::vector<uint8_t> header((2 * objects_.size() + 1) * sizeof(uint64_t) / sizeof(uint8_t));
        size_t object_count = objects_.size();
        std::memcpy(&header[0], &object_count, sizeof(object_count));
        uint64_t shift = 0;
        for (size_t ind = 0; ind < object_count; ++ind) {
            auto object = objects_[ind];

            shift += object.second.size() * sizeof(uint8_t);
            std::memcpy(&header[(2 * ind + 2) * sizeof(uint64_t) / sizeof(uint8_t)], &shift, sizeof(shift));
            if (!object.second.empty()) {
                std::memcpy(buffer_file.data() + buffer_file.size() - shift, object.second.data(), object.second.size());
            }

            shift += object.first.user_key.size() * sizeof(uint8_t);
            std::memcpy(buffer_file.data() + buffer_file.size() - shift, object.first.user_key.data(), object.first.user_key.size());

            shift += sizeof(uint64_t);
            std::memcpy(&header[(2 * ind + 1) * sizeof(uint64_t) / sizeof(uint8_t)], &shift, sizeof(shift));

            std::vector<uint8_t> sequence_number(sizeof(uint64_t) / sizeof(uint8_t));
            std::memcpy(sequence_number.data(), &object.first.sequence_number, sizeof(object.first.sequence_number));
            std::memcpy(buffer_file.data() + buffer_file.size() - shift, sequence_number.data(), sizeof(uint64_t));
        }
        std::memcpy(buffer_file.data(), header.data(), header.size());
        file_->Write(buffer_file.data(), buffer_file.size());
    }

   private:
    std::vector<std::pair<InternalKey, Value>> objects_;
    std::shared_ptr<storage::IFile> file_;
};

class SSTableFactory final : public ISSTableSerializer {
   public:
    std::shared_ptr<ISSTableReader> FromFile(const std::shared_ptr<const storage::IFile>& file) const override { return std::make_shared<FileSSTableReader>(file); }
    std::unique_ptr<ISSTableBuilder> NewFileBuilder(const std::shared_ptr<storage::IFile>& file) const override { return std::make_unique<FileSSTableBuilder>(file); }
};

}  // namespace

std::shared_ptr<ISSTableSerializer> MakeSSTableFileFactory() { return std::make_shared<SSTableFactory>(); }

}  // namespace lsm
