#include "impl/r_tree/r_tree.h"
#include <sys/stat.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

namespace r_tree {

class RTree::Impl {
private:
    struct Node {
        bool is_leaf;
        std::vector<int64_t> left;
        std::vector<int64_t> right;
        std::vector<Node*> links;
        std::vector<std::vector<int64_t>> points;
    };

public:
    Impl(const RTreeConfig& config) : config_(config) {
        minimal_batch_ = static_cast<uint64_t>(std::ceil(config_.alpha * config_.b));
    }

    void Insert(const std::vector<int64_t>& point) {
        if (root_ == nullptr) {
            root_ = new Node(
                true,
                point,
                point,
                {},
                {point}
            );
            return;
        }
        auto new_root = InsertInNode(root_, point);
        if (new_root != nullptr) {
            root_ = new Node(
                false,
                root_->left,
                root_->right,
                {root_, new_root},
                {}
            );
            for (uint64_t dim = 0; dim < config_.d; ++dim) {
                root_->left[dim] = std::min(root_->left[dim], new_root->left[dim]);
                root_->right[dim] = std::max(root_->right[dim], new_root->right[dim]);
            }
        }
    }

    std::vector<int64_t> BestFirst(const std::vector<int64_t>& point) const {
        std::priority_queue<std::pair<uint64_t, Node*>, std::vector<std::pair<uint64_t, Node*>>, std::greater<std::pair<uint64_t, Node*>>> queue;
        uint64_t best_dist = 0;
        Node* best_node = nullptr;
        uint64_t best_point_id;
        queue.push({0, root_});
        while (!queue.empty()) {
            auto [dist, node] = queue.top();
            // std::cout << dist << ' ' << node->left[0] << ' ' << node->right[0] << '\n';
            queue.pop();
            if (best_node != nullptr && best_dist < dist) {
                continue;
            }
            if (node->is_leaf) {
                // std::cout << "leaf " << node->points.size() << '\n';
                for (uint64_t ind = 0; ind < node->points.size(); ++ind) {
                    dist = GetDist2(point, node->points[ind], node->points[ind]);
                    // std::cout << "new " << dist << ' ' << node->points[ind][0] << '\n';
                    if (best_node == nullptr || best_dist > dist) {
                        best_dist = dist;
                        best_node = node;
                        best_point_id = ind;
                    }
                }
            } else {
                // std::cout << "mid" << '\n';
                for (auto & link : node->links) {
                    // std::cout << GetDist2(point, link->left, link->right) << ' ' << link->left[0] << ' ' << link->right[0] << '\n';
                    queue.push({GetDist2(point, link->left, link->right), link});
                }
            }
        }
        // std::cout << "end " << best_node << ' ' << best_point_id << ' ' << best_dist << '\n';
        return best_node->points[best_point_id];
    }

    // Only for bench
    uint64_t GetLvl() const {
        uint64_t max_lvl = 0;
        std::queue<std::pair<uint64_t, Node*>> queue;
        queue.push({0, root_});
        while (!queue.empty()) {
            auto [lvl, node] = queue.front();
            queue.pop();
            max_lvl = std::max(max_lvl, lvl);
            if (!node->is_leaf) {
                for (auto & link : node->links) {
                    queue.push({lvl + 1, link});
                }
            }
        }
        return max_lvl;
    }

    ~Impl() {
        std::queue<Node*> nodes;
        nodes.push(root_);
        while (!nodes.empty()) {
            auto node = nodes.front();
            nodes.pop();
            for (auto & link : node->links) {
                nodes.push(link);
            }
            delete node;
        }
    }

private:
    uint64_t GetDist2(const std::vector<int64_t>& point, const std::vector<int64_t>& left, const std::vector<int64_t>& right) const {
        uint64_t dist = 0;
        for (uint64_t dim = 0; dim < config_.d; ++dim) {
            uint64_t dim_dist = 0;
            if (point[dim] < left[dim]) {
                dim_dist = left[dim] - point[dim];
            } else if (point[dim] > right[dim]) {
                dim_dist = point[dim] - right[dim];
            }
            dist += dim_dist * dim_dist;
        }
        return dist;
    }

    Node* InsertInNode(Node* current, const std::vector<int64_t>& new_point) {
        if (current->is_leaf) {
            current->points.push_back(new_point);
            if (current->points.size() > config_.b) {
                std::vector<std::vector<int64_t>> points = current->points;
                current->points.clear();
                uint64_t best_dim, best_count, best_p;
                for (uint64_t cur_dim = 0; cur_dim < config_.d; ++cur_dim) {
                    auto comp = [cur_dim](const std::vector<int64_t>& lhs, const std::vector<int64_t>& rhs) {
                        return lhs[cur_dim] < rhs[cur_dim];
                    };
                    std::sort(points.begin(), points.end(), comp);
                    std::vector<uint64_t> perimeters;
                    std::vector<int64_t> left;
                    std::vector<int64_t> right;
                    uint64_t cur_p = 0;
                    for (uint64_t count = 1; count <= points.size() - minimal_batch_; ++count) {
                        if (count == 1) {
                            left = points[points.size() - count];
                            right = points[points.size() - count];
                        } else {
                            for (uint64_t i = 0; i < config_.d; ++i) {
                                if (points[points.size() - count][i] < left[i]) {
                                    cur_p += left[i] - points[points.size() - count][i];
                                    left[i] = points[points.size() - count][i];
                                } else if (points[points.size() - count][i] > right[i]) {
                                    cur_p += points[points.size() - count][i] - right[i];
                                    right[i] = points[points.size() - count][i];
                                }
                            }
                        }
                        if (count >= minimal_batch_) {
                            perimeters.push_back(cur_p);
                        }
                    }
                    std::reverse(perimeters.begin(), perimeters.end());
                    cur_p = 0;
                    uint64_t cur_best_count, cur_best_p;
                    for (uint64_t count = 1; count <= points.size() - minimal_batch_; ++count) {
                        if (count == 1) {
                            left = points[count - 1];
                            right = points[count - 1];
                        } else {
                            for (uint64_t i = 0; i < config_.d; ++i) {
                                if (points[count - 1][i] < left[i]) {
                                    cur_p += left[i] - points[count - 1][i];
                                    left[i] = points[count - 1][i];
                                } else if (points[count - 1][i] > right[i]) {
                                    cur_p += points[count - 1][i] - right[i];
                                    right[i] = points[count - 1][i];
                                }
                            }
                        }
                        if (count >= minimal_batch_) {
                            if (count == minimal_batch_ || cur_best_p > cur_p + perimeters[count - minimal_batch_]) {
                                cur_best_count = count;
                                cur_best_p = cur_p + perimeters[count - minimal_batch_];
                            }
                        }
                    }
                    if (cur_dim == 0 || best_p > cur_best_p) {
                        best_p = cur_best_p;
                        best_count = cur_best_count;
                        best_dim = cur_dim;
                    }
                }
                auto comp = [best_dim](const std::vector<int64_t>& lhs, const std::vector<int64_t>& rhs) {
                    return lhs[best_dim] < rhs[best_dim];
                };
                std::sort(points.begin(), points.end(), comp);
                for (uint64_t ind = 0; ind < best_count; ++ind) {
                    if (ind == 0) {
                        current->left = points[ind];
                        current->right = points[ind];
                    } else {
                        for (uint64_t i = 0; i < config_.d; ++i) {
                            current->left[i] = std::min(current->left[i], points[ind][i]);
                            current->right[i] = std::max(current->right[i], points[ind][i]);
                        }
                    }
                    current->points.push_back(points[ind]);
                }
                auto new_node = new Node(
                    true,
                    {},
                    {},
                    {},
                    {}
                );
                for (uint64_t ind = best_count; ind < points.size(); ++ind) {
                    if (ind == best_count) {
                        new_node->left = points[ind];
                        new_node->right = points[ind];
                    } else {
                        for (uint64_t i = 0; i < config_.d; ++i) {
                            new_node->left[i] = std::min(new_node->left[i], points[ind][i]);
                            new_node->right[i] = std::max(new_node->right[i], points[ind][i]);
                        }
                    }
                    new_node->points.push_back(points[ind]);
                }
                return new_node;
            }
        } else {
            uint64_t best_subtree, best_dist;
            for (uint64_t ind = 0; ind < current->links.size(); ++ind) {
                auto dist = GetDist2(new_point, current->links[ind]->left, current->links[ind]->right);
                if (ind == 0 || best_dist > dist) {
                    best_dist = dist;
                    best_subtree = ind;
                }
            }
            auto new_node = InsertInNode(current->links[best_subtree], new_point);
            if (new_node != nullptr) {
                current->links.push_back(new_node);
            }
            if (current->links.size() > config_.b) {
                std::vector<Node*> links = current->links;
                current->links.clear();
                uint64_t best_dim, best_side, best_count, best_p;
                for (uint64_t cur_dim = 0; cur_dim < config_.d; ++cur_dim) {
                    for (uint8_t cur_side = 0; cur_side < 2; ++cur_side) {
                        auto comp = [cur_dim, cur_side](const Node* lhs, const Node* rhs) {
                            if (cur_side == 0) {
                                return lhs->left[cur_dim] < rhs->left[cur_dim];
                            } else {
                                return lhs->right[cur_dim] < rhs->right[cur_dim];
                            }
                        };
                        std::sort(links.begin(), links.end(), comp);
                        std::vector<uint64_t> perimeters;
                        std::vector<int64_t> left;
                        std::vector<int64_t> right;
                        uint64_t cur_p = 0;
                        for (uint64_t count = 1; count <= links.size() - minimal_batch_; ++count) {
                            if (count == 1) {
                                left = links[links.size() - count]->left;
                                right = links[links.size() - count]->right;
                            } else {
                                for (uint64_t i = 0; i < config_.d; ++i) {
                                    if (links[links.size() - count]->left[i] < left[i]) {
                                        cur_p += left[i] - links[links.size() - count]->left[i];
                                        left[i] = links[links.size() - count]->left[i];
                                    }
                                    if (links[links.size() - count]->right[i] > right[i]) {
                                        cur_p += links[links.size() - count]->right[i] - right[i];
                                        right[i] = links[links.size() - count]->right[i];
                                    }
                                }
                            }
                            if (count >= minimal_batch_) {
                                perimeters.push_back(cur_p);
                            }
                        }
                        std::reverse(perimeters.begin(), perimeters.end());
                        cur_p = 0;
                        uint64_t cur_best_count, cur_best_p;
                        for (uint64_t count = 1; count <= links.size() - minimal_batch_; ++count) {
                            if (count == 1) {
                                left = links[count - 1]->left;
                                right = links[count - 1]->right;
                            } else {
                                for (uint64_t i = 0; i < config_.d; ++i) {
                                    if (links[count - 1]->left[i] < left[i]) {
                                        cur_p += left[i] - links[count - 1]->left[i];
                                        left[i] = links[count - 1]->left[i];
                                    }
                                    if (links[count - 1]->right[i] > right[i]) {
                                        cur_p += links[count - 1]->right[i] - right[i];
                                        right[i] = links[count - 1]->right[i];
                                    }
                                }
                            }
                            if (count >= minimal_batch_) {
                                if (count == minimal_batch_ || cur_best_p > cur_p + perimeters[count - minimal_batch_]) {
                                    cur_best_count = count;
                                    cur_best_p = cur_p + perimeters[count - minimal_batch_];
                                }
                            }
                        }
                        if (cur_dim == 0 || best_p > cur_best_p) {
                            best_p = cur_best_p;
                            best_count = cur_best_count;
                            best_side = cur_side;
                            best_dim = cur_dim;
                        }
                    }
                }
                auto comp = [best_dim, best_side](const Node* lhs, const Node* rhs) {
                    if (best_side == 0) {
                        return lhs->left[best_dim] < rhs->left[best_dim];
                    } else {
                        return lhs->right[best_dim] < rhs->right[best_dim];
                    }
                };
                std::sort(links.begin(), links.end(), comp);
                for (uint64_t ind = 0; ind < best_count; ++ind) {
                    if (ind == 0) {
                        current->left = links[ind]->left;
                        current->right = links[ind]->right;
                    } else {
                        for (uint64_t i = 0; i < config_.d; ++i) {
                            current->left[i] = std::min(current->left[i], links[ind]->left[i]);
                            current->right[i] = std::max(current->right[i], links[ind]->right[i]);
                        }
                    }
                    current->links.push_back(links[ind]);
                }
                auto new_node = new Node(
                    false,
                    {},
                    {},
                    {},
                    {}
                );
                for (uint64_t ind = best_count; ind < links.size(); ++ind) {
                    if (ind == best_count) {
                        new_node->left = links[ind]->left;
                        new_node->right = links[ind]->right;
                    } else {
                        for (uint64_t i = 0; i < config_.d; ++i) {
                            new_node->left[i] = std::min(new_node->left[i], links[ind]->left[i]);
                            new_node->right[i] = std::max(new_node->right[i], links[ind]->right[i]);
                        }
                    }
                    new_node->links.push_back(links[ind]);
                }
                return new_node;
            }
        }
        for (uint64_t i = 0; i < config_.d; ++i) {
            current->left[i] = std::min(current->left[i], new_point[i]);
            current->right[i] = std::max(current->right[i], new_point[i]);
        }
        return nullptr;
    }

private:
    RTreeConfig config_;
    uint64_t minimal_batch_;
    Node* root_ = nullptr;
};

RTree::RTree(const RTreeConfig& config) : impl_(std::make_unique<RTree::Impl>(config)) {
}

void RTree::Insert(const std::vector<int64_t>& point) {
    return impl_->Insert(point);
}

std::vector<int64_t> RTree::BestFirst(const std::vector<int64_t>& point) const {
    return impl_->BestFirst(point);
}

uint64_t RTree::GetLvl() const {
    return impl_->GetLvl();
}

RTree::~RTree() = default;

}