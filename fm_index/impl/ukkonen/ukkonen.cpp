#include "impl/ukkonen/ukkonen.h"

#include <cstddef>
#include <map>
#include <string>
#include <vector>

namespace fm_index::ukkonen {

namespace {

    struct Node {
        struct Edge {
            size_t pos;
            size_t len;
            Node* link;
        };

        std::map<char, Edge> edges;
        Node* suf_link = nullptr;
    };

    void Dfs(Node* node, size_t active_len, size_t max_len, std::vector<size_t>& suf_array) {
        // std::cout << "-> " << node << ' ' << active_len <<std::endl;
        if (node->edges.empty()) {
            suf_array.push_back(max_len - active_len);
        // std::cout << "<- " << node << ' ' << max_len - active_len << std::endl;
            return;
        }
        // for (auto & [sym, edge] : node->edges) {
        //     std::cout << "node " << node << " edge " << sym << ' ' << edge.pos << ' ' << edge.len << ' ' << edge.link << ' ' << max_len << std::endl;
        // }
        for (auto & [sym, edge] : node->edges) {
            size_t new_active_len = active_len;
            if (edge.len > 0) {
                new_active_len += edge.len;
            } else {
                new_active_len += max_len - edge.pos;
            }
            Dfs(edge.link, new_active_len, max_len, suf_array);
            delete edge.link;
        }
        // std::cout << "<- " << node << std::endl;
    }

} // namespace

std::vector<size_t> MakeSuffixArray(const std::string& s) {
    auto root = new Node();
    auto active_node = root;
    size_t active_pos = 0;
    size_t active_len = 0;
    for (size_t pos = 0, add_suffix = 1; pos < s.size(); ++pos, ++add_suffix) {
        Node* prev_mid_node = nullptr;
        while (add_suffix) {
            // std::cout << pos << ' ' << active_node << ' ' << active_len << ' ' << active_pos << ' ' << s[active_pos] << ' ' << add_suffix << std::endl;
            bool end = false;
            while (active_len != 0 && active_node->edges.contains(s[active_pos]) && active_node->edges[s[active_pos]].len != 0 && active_node->edges[s[active_pos]].len <= active_len) {
                auto new_active_node = active_node->edges[s[active_pos]].link;
                auto new_active_len = active_len - active_node->edges[s[active_pos]].len;
                auto new_active_pos = active_pos + active_node->edges[s[active_pos]].len;
                active_node = new_active_node;
                active_len = new_active_len;
                active_pos = new_active_pos;
            }
            if (active_node->edges.contains(s[active_pos])) {
                size_t previous_pos = active_node->edges[s[active_pos]].pos + active_len;
                if (s[pos] == s[previous_pos]) {
                    ++active_len;
                    if (prev_mid_node != nullptr) {
                        prev_mid_node->suf_link = active_node;
                    }
                    end = true;
                } else {
                    auto mid_node = new Node();
                    mid_node->edges[s[previous_pos]] = {
                        .pos = previous_pos,
                        .len = 0,
                        .link = active_node->edges[s[active_pos]].link,
                    };
                    if (active_node->edges[s[active_pos]].len > 0) {
                        mid_node->edges[s[previous_pos]].len = active_node->edges[s[active_pos]].len - active_len;
                    }
                    mid_node->edges[s[pos]] = {
                        .pos = pos,
                        .len = 0,
                        .link = new Node(),
                    };
                    if (prev_mid_node != nullptr) {
                        // std::cout << "new suf link " << prev_mid_node << ' ' << mid_node << std::endl;
                        prev_mid_node->suf_link = mid_node;
                    }
                    prev_mid_node = mid_node;
                    --add_suffix;
                    active_node->edges[s[active_pos]].len = active_len;
                    active_node->edges[s[active_pos]].link = mid_node;
                    if (active_node == root) {
                        ++active_pos;
                        if (active_len > 0) {
                            --active_len;
                        }
                    } else if (active_node->suf_link != nullptr) {
                        active_node = active_node->suf_link;
                    } else {
                        active_node = root;
                    }
                }
            } else {
                auto new_node = new Node();
                active_node->edges[s[active_pos]] = {
                    .pos = active_pos,
                    .len = 0,
                    .link = new_node,
                };
                if (prev_mid_node != nullptr) {
                    // std::cout << "new suf link " << prev_mid_node << ' ' << new_node << std::endl;
                    prev_mid_node->suf_link = active_node;
                }
                prev_mid_node = new_node;
                --add_suffix;
                if (active_node == root) {
                    ++active_pos;
                    if (active_len > 0) {
                        --active_len;
                    }
                } else if (active_node->suf_link != nullptr) {
                    active_node = active_node->suf_link;
                } else {
                    active_node = root;
                }
            }
            if (end) {
                break;
            }
        }
    }

    std::vector<size_t> suf_array;
    Dfs(root, 0, s.size(), suf_array);
    delete root;
    // std::cout << "MakeSuffixArray fin\n";
    return suf_array;
}

};  // namespace fm_index::ukkonen