#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

namespace lsm::storage {

class IFrame {
   public:
    virtual uint8_t* Data() = 0;
    virtual uint64_t Size() = 0;

    virtual void MarkDirty() = 0;

    virtual ~IFrame() = default;
};

struct FrameId {
    uint32_t table_id;
    uint32_t page_id;
};
static_assert(sizeof(FrameId) == 8);

class IReadFrameProvider {
   public:
    virtual void Start(uint32_t) = 0;
    virtual std::shared_ptr<IFrame> GetFrame(FrameId) = 0;
    virtual void Finish() = 0;

    virtual ~IReadFrameProvider() = default;
};

std::shared_ptr<IReadFrameProvider> MakeReadFrameProvider(const std::string& dir, uint64_t frame_size, uint64_t* read_bytes);

class IReadBufferPool {
   public:
    virtual std::shared_ptr<IFrame> GetFrame(FrameId id) = 0;
    virtual std::vector<std::shared_ptr<IFrame>> GetFrames(uint32_t table_id, uint32_t l, uint32_t r) = 0;

    virtual ~IReadBufferPool() = default;
};

std::shared_ptr<IReadBufferPool> MakeReadBufferPool(std::string dir, uint64_t pool_size, uint64_t frame_size = 4096, uint64_t* read_bytes = new uint64_t());

}  // namespace lsm::storage
