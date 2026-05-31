#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace fm_index {

class FMIndex {
public:
    FMIndex(const std::string& s, uint64_t k);

    uint64_t Count(const std::string& request) const;

    std::vector<uint64_t> Locate(const std::string& request) const;

    ~FMIndex();

private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

} // namespace fm_index