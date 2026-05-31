#include "impl/index/index.h"

#include <impl/ukkonen/ukkonen.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace fm_index {

class FMIndex::Impl {
public:
    Impl(const std::string& s, uint64_t k) : k_(k) {
        auto suf_array = ukkonen::MakeSuffixArray(s);
        for (uint64_t i = 0; i < suf_array.size(); ++i) {
            auto bwt_ind = suf_array[i];
            if (bwt_ind == 0) {
                bwt_ind = s.size();
            }
            --bwt_ind;
            bwt_.push_back(s[bwt_ind]);

            if (!c_.contains(s[suf_array[i]])) {
                c_[s[suf_array[i]]] = i;
            }

            if (suf_array[i] % k == 0) {
                l_to_t_[i] = suf_array[i];
            }
        }

        for (auto & [sym, _] : c_) {
            uint64_t count = 0;
            std::vector<uint64_t> occ_for_sym;
            for (size_t i = 0; i < bwt_.size(); ++i) {
                if (bwt_[i] == sym) {
                    ++count;
                }
                if (i % k == 0) {
                    occ_for_sym.push_back(count);
                }
            }
            occ_[sym] = occ_for_sym;
        }
    }

    uint64_t Count(const std::string& request) const {
        auto [l, r] = GetSegment(request);
        return r + 1 - l;
    }

    std::vector<uint64_t> Locate(const std::string& request) const {
        auto [l, r] = GetSegment(request);
        std::vector<uint64_t> result;
        for (uint64_t ind = l; ind <= r; ++ind) {
            uint64_t pos = ind, step = 0;
            while (!l_to_t_.contains(pos)) {
                pos = Lf(pos);
                ++step;
            }
            result.push_back((l_to_t_.at(pos) + step) % bwt_.size());
        }
        return result;
    }

    ~Impl() = default;

private:
    std::pair<uint64_t, uint64_t> GetSegment(const std::string& request) const {
        if (!c_.contains(request.back())) {
            return {bwt_.size(), bwt_.size()};
        }
        uint64_t l = c_.at(request.back()), r = c_.at(request.back()) + Occ(request.back(), bwt_.size() - 1) - 1;
        for (size_t i = request.size() - 1; i > 0; --i) {
            char sym = request[i - 1];
            if (!c_.contains(sym)) {
                return {bwt_.size() + 1, bwt_.size()};
            }
            if (l > 0) {
                l = c_.at(sym) + Occ(sym, l - 1);
            } else {
                l = c_.at(sym);
            }
            r = c_.at(sym) + Occ(sym, r) - 1;
        }
        return {l, r};
    }

    uint64_t Occ(char c, uint64_t k) const {
        auto result = 0;
        if (occ_.contains(c)) {
            result = occ_.at(c)[k / k_];
        }
        for (size_t i = k + 1 - k % k_; i <= k; ++i) {
            if (bwt_[i] == c) {
                ++result;
            }
        }
        return result;
    }

    uint64_t Lf(uint64_t i) const {
        return c_.at(bwt_[i]) + Occ(bwt_[i], i) - 1;
    }

private:
    uint64_t k_;
    std::vector<char> bwt_;
    std::unordered_map<char, uint64_t> c_;
    std::unordered_map<uint64_t, uint64_t> l_to_t_;
    std::unordered_map<char, std::vector<uint64_t>> occ_;
};

FMIndex::FMIndex(const std::string& s, uint64_t k) : impl_(std::make_unique<FMIndex::Impl>(s, k)) {
}

uint64_t FMIndex::Count(const std::string& request) const {
    return impl_->Count(request);
}

std::vector<uint64_t> FMIndex::Locate(const std::string& request) const {
    return impl_->Locate(request);
}

FMIndex::~FMIndex() = default;

} // namespace fm_index