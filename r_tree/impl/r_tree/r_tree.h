#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace r_tree {

struct RTreeConfig {
    uint64_t d;
    uint64_t b = 3;
    double alpha = 0.4;
};

class RTree {
public:
    RTree(const RTreeConfig& config);

    void Insert(const std::vector<int64_t>& point);

    std::vector<int64_t> BestFirst(const std::vector<int64_t>& point) const;

    uint64_t GetLvl() const;

    ~RTree();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace r_tree
