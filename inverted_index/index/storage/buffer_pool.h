#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

namespace invindex::storage {

class IFrame {
   public:
    virtual char* Data() = 0;
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
    virtual void Start(const std::string& path) = 0;
    virtual std::shared_ptr<IFrame> GetFrame(FrameId) = 0;
    virtual void Finish() = 0;

    virtual ~IReadFrameProvider() = default;
};

class IReadBufferPool {
   public:
    virtual std::vector<std::shared_ptr<IFrame>> GetFrames(const std::string& path, uint32_t table_id, uint32_t l, uint32_t r) = 0;

    virtual ~IReadBufferPool() = default;
};

std::shared_ptr<IReadBufferPool> MakeReadBufferPool(uint64_t pool_size, uint64_t frame_size = 4096);

}  // namespace invindex::storage
