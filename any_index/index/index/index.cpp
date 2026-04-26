#include "index/index/index.h"

#include <strutext/morpho/models/rus_model.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/core/checked_delete.hpp>
#include <boost/locale.hpp>
#include <boost/regex.hpp>
#include <cassert>
#include <chrono>
#include <cmath>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "index/common/types.h"
#include "index/roaring/roaring_utils.h"

namespace invindex {

namespace {

using RussianPos = strutext::morpho::RussianPos;
using RussianPosSerializer = strutext::morpho::russian::PosSerializer;

class TokenId final : public lsm::IUserKey {
public:
    TokenId() = default;

    TokenId(const Token& feature_id, uint64_t block_id) : feature_id_(feature_id), block_id_(block_id) {}

    void Deserialize(const std::vector<char> &buffer) override {
        // std::cout << "Deserialize TokenId\n";
        std::memcpy(&block_id_, buffer.data(), sizeof(block_id_));
        feature_id_.resize(buffer.size() - sizeof(block_id_));
        std::memcpy(feature_id_.data(), buffer.data() + sizeof(block_id_), feature_id_.size());
    }

    std::vector<char> Serialize() const override {
        std::vector<char> buffer(GetSerializedSize());
        std::memcpy(buffer.data(), &block_id_, sizeof(block_id_));
        std::memcpy(buffer.data() + sizeof(block_id_), feature_id_.data(), feature_id_.size());
        return buffer;
    }

    uint64_t GetSerializedSize() const override {
        return feature_id_.size() + sizeof(block_id_);
    }

    std::strong_ordering operator<=>(const lsm::UserKey& token_id) const override {
        {
            auto cmp = feature_id_ <=> std::reinterpret_pointer_cast<TokenId>(token_id)->feature_id_;
            if (cmp != std::strong_ordering::equal) {
                return cmp;
            }
        }
        {
            auto cmp = block_id_ <=> std::reinterpret_pointer_cast<TokenId>(token_id)->block_id_;
            if (cmp != std::strong_ordering::equal) {
                return cmp;
            }
        }
        return std::strong_ordering::equal;
    }

public:
    Token feature_id_;
    uint64_t block_id_;
};

class Token2Id final : public lsm::IUserKey {
public:
    Token2Id() = default;

    Token2Id(const Token& feature1_id, const Token& feature2_id, uint64_t block_id) : feature1_id_(feature1_id), feature2_id_(feature2_id), block_id_(block_id) {}

    void Deserialize(const std::vector<char> &buffer) override {
        // std::cout << "Deserialize Token2Id\n";
        std::memcpy(&block_id_, buffer.data(), sizeof(block_id_));
        uint64_t feature1_size;
        std::memcpy(&feature1_size, buffer.data() + sizeof(block_id_), sizeof(block_id_));
        feature1_id_.resize(feature1_size);
        std::memcpy(feature1_id_.data(), buffer.data() + sizeof(block_id_), feature1_id_.size());
        feature2_id_.resize(buffer.size() - sizeof(block_id_) - feature1_size);
        std::memcpy(feature2_id_.data(), buffer.data() + sizeof(block_id_), feature2_id_.size());
    }

    std::vector<char> Serialize() const override {
        std::vector<char> buffer(GetSerializedSize());
        std::memcpy(buffer.data(), &block_id_, sizeof(block_id_));
        uint64_t feature1_size = feature1_id_.size();
        std::memcpy(buffer.data() + sizeof(block_id_), &feature1_size, sizeof(feature1_size));
        std::memcpy(buffer.data() + sizeof(block_id_) + sizeof(feature1_size), feature1_id_.data(), feature1_id_.size());
        std::memcpy(buffer.data() + sizeof(block_id_) + sizeof(feature1_size) + feature1_id_.size(), feature2_id_.data(), feature2_id_.size());
        return buffer;
    }

    uint64_t GetSerializedSize() const override {
        return feature1_id_.size() + feature2_id_.size() + sizeof(uint64_t) + sizeof(block_id_);
    }

    std::strong_ordering operator<=>(const lsm::UserKey& token_id) const override {
        {
            auto cmp = feature1_id_ <=> std::dynamic_pointer_cast<Token2Id>(token_id)->feature1_id_;
            if (cmp != std::strong_ordering::equal) {
                return cmp;
            }
        }
        {
            auto cmp = feature2_id_ <=> std::dynamic_pointer_cast<Token2Id>(token_id)->feature2_id_;
            if (cmp != std::strong_ordering::equal) {
                return cmp;
            }
        }
        {
            auto cmp = block_id_ <=> std::dynamic_pointer_cast<Token2Id>(token_id)->block_id_;
            if (cmp != std::strong_ordering::equal) {
                return cmp;
            }
        }
        return std::strong_ordering::equal;
    }

public:
    Token feature1_id_;
    Token feature2_id_;
    uint64_t block_id_;
};

class BitMap final : public lsm::IValue {
public:
    BitMap() = default;

    BitMap(const roaring::Roaring64Map& bitmap) : bitmap_(bitmap) {}

    void Deserialize(const std::vector<char> &buffer) override {
        // std::cout << "Deserialize BitMap\n";
        bitmap_ = roaring::Deserialize(buffer);
        // std::cout << "End Deserialize BitMap\n";
    }

    std::vector<char> Serialize() const override {
        return roaring::Serialize(bitmap_);
    }

    uint64_t GetSerializedSize() const override {
        return bitmap_.getSizeInBytes();
    }

    void Merge(const lsm::Value& value) override {
        bitmap_ |= std::dynamic_pointer_cast<BitMap>(value)->bitmap_;
    }

    void RunOptimize() override {
        bitmap_.runOptimize();
    }

    bool Empty() const override {
        return bitmap_.isEmpty();
    }

public:
    roaring::Roaring64Map bitmap_;
};

class PosMap final : public lsm::IValue {
public:
    PosMap() = default;

    PosMap(uint64_t doc_id, uint64_t pos_id) {
        posmap[doc_id] = {pos_id};
        bitmaps_size = posmap[doc_id].getSizeInBytes();
    }

    PosMap(const std::unordered_map<uint64_t, roaring::Roaring64Map>& pos_map) : posmap(pos_map) {
        bitmaps_size = 0;
        for (auto& [_, bitmap]: posmap) {
            bitmaps_size += bitmap.getSizeInBytes();
        }
    }

    void Deserialize(const std::vector<char> &buffer) override {
        // std::cout << "Deserialize PosMap\n";
        uint64_t shift = 0;
        bitmaps_size = 0;
        posmap.clear();
        while (shift < buffer.size()) {
            uint64_t doc_id;
            std::memcpy(&doc_id, buffer.data() + shift, sizeof(doc_id));
            shift += sizeof(doc_id);
            size_t bitmap_size;
            std::memcpy(&bitmap_size, buffer.data() + shift, sizeof(bitmap_size));
            shift += sizeof(bitmap_size);
            std::vector<char> bitmap_buffer(bitmap_size);
            std::memcpy(bitmap_buffer.data(), buffer.data() + shift, bitmap_size);
            shift += bitmap_size;
            posmap[doc_id] = roaring::Deserialize(bitmap_buffer);
            bitmaps_size += posmap[doc_id].getSizeInBytes();
        }
        // std::cout << "Deserialize PosMap END\n";
    }

    std::vector<char> Serialize() const override {
        std::vector<char> buffer(GetSerializedSize());
        uint64_t shift = 0;
        for (const auto& [doc_id, bitmap]: posmap) {
            std::memcpy(buffer.data() + shift, &doc_id, sizeof(doc_id));
            shift += sizeof(doc_id);
            auto bitmap_buffer = roaring::Serialize(bitmap);
            auto bitmap_size = bitmap_buffer.size();
            std::memcpy(buffer.data() + shift, &bitmap_size, sizeof(bitmap_size));
            shift += sizeof(bitmap_size);
            std::memcpy(buffer.data() + shift, bitmap_buffer.data(), bitmap_size);
            shift += bitmap_size;
        }
        return buffer;
    }

    uint64_t GetSerializedSize() const override {
        return bitmaps_size + 2 * posmap.size() * sizeof(uint64_t);
    }

    void Merge(const lsm::Value& value) override {
        // std::cout << "Merge op\n";
        for (auto& [doc_id, bitmap]: std::dynamic_pointer_cast<PosMap>(value)->posmap) {
            if (posmap.contains(doc_id)) {
                // std::cout << "contains" << std::endl;
                bitmaps_size -= posmap[doc_id].getSizeInBytes();
                posmap[doc_id] |= bitmap;
                posmap[doc_id].runOptimize();
            } else {
                // std::cout << "no contains" << std::endl;
                posmap[doc_id] = bitmap;
            }
            bitmaps_size += posmap[doc_id].getSizeInBytes();
        }
    }

    void RunOptimize() override {
        for (auto& [_, bitmap] : posmap) {
            bitmaps_size -= bitmap.getSizeInBytes();
            bitmap.runOptimize();
            bitmaps_size += bitmap.getSizeInBytes();
        }
    }

    bool Empty() const override {
        return posmap.empty();
    }

public:
    uint64_t bitmaps_size = 0;
    std::unordered_map<uint64_t, roaring::Roaring64Map> posmap;
};

class DocsStat final : public lsm::IValue {
public:
    DocsStat() = default;

    DocsStat(uint64_t doc_id) {
        in_doc_counts_[doc_id] = 1;
    }

    void Deserialize(const std::vector<char> &buffer) override {
        for (uint64_t shift = 0; shift + 2 * sizeof(uint64_t) <= buffer.size(); shift += 2 * sizeof(uint64_t)) {
            uint64_t doc_id;
            std::memcpy(&doc_id, buffer.data() + shift, sizeof(doc_id));
            uint64_t in_doc_count;
            std::memcpy(&in_doc_count, buffer.data() + shift + sizeof(doc_id), sizeof(in_doc_count));
            in_doc_counts_[doc_id] = in_doc_count;
        }
    }

    std::vector<char> Serialize() const override {
        std::vector<char> buffer(GetSerializedSize());
        uint64_t shift = 0;
        for (auto & [doc_id, in_doc_count] : in_doc_counts_) {
            std::memcpy(buffer.data() + shift, &doc_id, sizeof(doc_id));
            shift += sizeof(uint64_t);
            std::memcpy(buffer.data() + shift, &in_doc_count, sizeof(in_doc_count));
            shift += sizeof(uint64_t);
        }
        return buffer;
    }

    uint64_t GetSerializedSize() const override {
        return in_doc_counts_.size() * 2 * sizeof(uint64_t);
    }

    void Merge(const lsm::Value& value) override {
        for (auto & [doc_id, in_doc_count] : std::dynamic_pointer_cast<DocsStat>(value)->in_doc_counts_) {
            in_doc_counts_[doc_id] += in_doc_count;
        }
    }

    void RunOptimize() override {
    }

    bool Empty() const override {
        return in_doc_counts_.empty();
    }

public:
    std::unordered_map<uint64_t, uint64_t> in_doc_counts_;
};

class TFIDF final : public lsm::IValue {
public:
    TFIDF() = default;

    TFIDF(double idf, const std::vector<std::pair<double, uint64_t>>& tf) : idf_(idf), tf_(tf) {
    }

    void Deserialize(const std::vector<char> &buffer) override {
        uint64_t shift = 0;
        tf_.clear();
        for (; shift + sizeof(double) + sizeof(uint64_t) <= buffer.size(); shift += sizeof(double) + sizeof(uint64_t)) {
            double tf;
            std::memcpy(&tf, buffer.data() + shift, sizeof(tf));
            uint64_t doc_id;
            std::memcpy(&doc_id, buffer.data() + shift + sizeof(tf), sizeof(doc_id));
            tf_.push_back({tf, doc_id});
        }
        std::memcpy(&idf_, buffer.data() + shift, sizeof(idf_));
    }

    std::vector<char> Serialize() const override {
        std::vector<char> buffer(GetSerializedSize());
        uint64_t shift = 0;
        for (auto & [tf, doc_id] : tf_) {
            std::memcpy(buffer.data() + shift, &tf, sizeof(tf));
            shift += sizeof(tf);
            std::memcpy(buffer.data() + shift, &doc_id, sizeof(doc_id));
            shift += sizeof(doc_id);
        }
        std::memcpy(buffer.data() + shift, &idf_, sizeof(idf_));
        return buffer;
    }

    uint64_t GetSerializedSize() const override {
        return sizeof(idf_) + tf_.size() * (sizeof(double) + sizeof(uint64_t));
    }

    void Merge(const lsm::Value& value) override {
    }

    void RunOptimize() override {
    }

    bool Empty() const override {
        return tf_.empty();
    }

public:
    double idf_ = 0;
    std::vector<std::pair<double, uint64_t>> tf_;
};

template<typename IndexKey, typename IndexValue>
class InvertedIndexImpl : public IInvertedIndex<IndexKey, IndexValue> {
   public:
    InvertedIndexImpl(const IndexConfig& config) : config_(config) {
        lsm_ = lsm::MakeIndexLsm(config_.index_dir, std::make_pair(std::static_pointer_cast<lsm::IUserKey>(std::make_shared<IndexKey>()), std::static_pointer_cast<lsm::IValue>(std::make_shared<IndexValue>())), config_.lsm_options);
    }

    void Put(const IndexKey& index_key, const IndexValue& index_value) {
        lsm_->Put(std::static_pointer_cast<lsm::IUserKey>(std::make_shared<IndexKey>(index_key)), std::static_pointer_cast<lsm::IValue>(std::make_shared<IndexValue>(index_value)));
    }

    std::optional<IndexValue> GetDocId(IndexKey index_key) const override {
        // std::cout << "GetDocId\n";
        auto index_value = lsm_->Get(std::static_pointer_cast<lsm::IUserKey>(std::make_shared<IndexKey>(index_key)));
        if (index_value.has_value()) {
            return *std::dynamic_pointer_cast<IndexValue>(index_value.value());
        } else {
            return std::nullopt;
        }
    }

    std::shared_ptr<IStream<std::pair<IndexKey, IndexValue>>> Scan() const override {
        return std::make_shared<IndexScanner>(lsm_->Scan(), std::make_pair(IndexKey(), IndexValue()));
    }

private:
    class IndexScanner : public IStream<std::pair<IndexKey, IndexValue>> {
    public:
        IndexScanner(const std::shared_ptr<IStream<std::pair<lsm::UserKey, lsm::Value>>>& lsm_scanner, const std::pair<IndexKey, IndexValue>& example) : lsm_scanner_(lsm_scanner), example_(example) {
        }

        std::optional<std::pair<IndexKey, IndexValue>> Next() {
            auto object = lsm_scanner_->Next();
            if (object.has_value()) {
                example_ = {*std::dynamic_pointer_cast<IndexKey>(object->first), *std::dynamic_pointer_cast<IndexValue>(object->second)};
                return example_;
            } else {
                return std::nullopt;
            }
        }

    private:
        std::shared_ptr<IStream<std::pair<lsm::UserKey, lsm::Value>>> lsm_scanner_;
        std::pair<IndexKey, IndexValue> example_;
    };

   private:
    IndexConfig config_;
    std::unique_ptr<lsm::ILSM> lsm_;
};

static const std::regex kDateAttributePatternt(R"(^\$[A-Za-z_]+:\d{4}-\d{2}-\d{2}$)");

std::string FilterText(const std::string& text) {
    if (std::regex_match(text, kDateAttributePatternt)) {
        return text;
    }
    auto is_rus = [](char32_t ch) { return (ch >= U'А' && ch <= U'Я') || (ch >= U'а' && ch <= U'я') || ch == U'Ё' || ch == U'ё'; };
    std::u32string in = boost::locale::conv::utf_to_utf<char32_t>(text), out;
    for (size_t i = 0; i < in.size(); ++i) {
        if (is_rus(in[i])) {
            out.push_back(in[i]);
        } else {
            out.push_back(U' ');
        }
    }
    return boost::locale::conv::utf_to_utf<char>(out);
}

std::vector<std::string> GetMainForms(const std::string& word, const std::shared_ptr<RussianMorpher>& russian_morpher, std::locale loc) {
    strutext::morpho::MorphologistBase::LemList lemm_list;
    russian_morpher->Analize(boost::locale::to_lower(word, loc), lemm_list);
    if (lemm_list.empty()) {
        return {boost::locale::to_lower(word, loc)};
    }
    bool skip = true;
    std::vector<std::string> main_forms;
    for (const auto& lemma : lemm_list) {
        if (skip) {
            RussianPos::Ptr pos = RussianPosSerializer::Deserialize(lemma.attr_);
            if (pos->GetPosTag() > RussianPos::PREDICATE_PS) {
                break;
            }
            skip = false;
        }
        Token main_form;
        russian_morpher->GenMainForm(lemma.id_, main_form);
        main_forms.push_back(main_form);
        break;
    }
    return main_forms;
}

struct PositionalIndexStateConfig {
    uint16_t doc_block_size;
    std::string work_dir;
    lsm::GranularLsmOptions lsm_options;
};

class PositionalIndexState final : public IIndexState {
public:
    PositionalIndexState(const PositionalIndexStateConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher) : doc_block_size_(config.doc_block_size), russian_morpher_(russian_morpher) {
        IndexConfig one_word_index_config = {
            .index_dir = config.work_dir + "/one_word",
            .lsm_options = config.lsm_options
        };
        one_word_index_ = std::make_shared<InvertedIndexImpl<TokenId, BitMap>>(one_word_index_config);
        IndexConfig biword_index_config = {
            .index_dir = config.work_dir + "/biword",
            .lsm_options = config.lsm_options
        };
        biword_index_ = std::make_shared<InvertedIndexImpl<Token2Id, PosMap>>(biword_index_config);
        doc_id_ = 0;
    }

    IndexType GetIndexType() const override {
        return IndexType::IT_POSITIONAL;
    }

    void StartDoc() override {
        pos_id_ = 0;
    }

    void AddToken(const Token& token) override {
        // std::cout << "AddToken\n";
        one_word_index_->Put(TokenId(token, doc_id_ / doc_block_size_), BitMap({doc_id_ % doc_block_size_}));
        if (pos_id_ > 0) {
            biword_index_->Put(Token2Id(prev_token_, token, doc_id_ / doc_block_size_), PosMap(doc_id_ % doc_block_size_, pos_id_ - 1));
        }
        // std::cout << "BiWord END\n";
        prev_token_ = token;
        ++pos_id_;
        // std::cout << "AddToken END\n";
    }

    void FinishDoc() override {
        ++doc_id_;
    }

    void Finish() override {
    }

    std::vector<uint64_t> Search(const std::string& query) const override {
        auto text_line_reader = TextLineReader(query);
        std::vector<std::string> tokens;
        auto word = text_line_reader.GetWord();
        while (word.has_value()) {
            auto main_forms = GetMainForms(word.value(), russian_morpher_, loc_);
            if (!main_forms.empty()) {
                tokens.push_back(main_forms[0]);
            }
            word = text_line_reader.GetWord();
        }
        std::cout << "Search:\n";
        for (auto & token: tokens) {
            std::cout << token << '\n';
        }
        // std::cout << doc_id_ << ' ' << doc_block_size_ << std::endl;

        std::vector<uint64_t> result;
        for (size_t doc_block_index = 0; doc_block_index <= (doc_id_ - 1) / doc_block_size_ + 1; ++doc_block_index) {
            if (tokens.size() == 1) {
                auto doc_ids = one_word_index_->GetDocId(TokenId(tokens[0], doc_block_index));
                if (doc_ids.has_value()) {
                    for (const auto& doc_id : doc_ids->bitmap_) {
                        result.push_back(doc_id + doc_block_size_ * doc_block_index);
                    }
                }
            } else {
                bool skip = false;
                std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>> posvecs;
                for (size_t token_index = 0; token_index < tokens.size(); token_index += 2) {
                    if (token_index + 1 == tokens.size()) {
                        --token_index;
                    }
                    auto ans = biword_index_->GetDocId(Token2Id(tokens[token_index], tokens[token_index + 1], doc_block_index));
                    if (ans.has_value()) {
                        for (auto & [doc_id, bitmap] : ans->posmap) {
                            for (const auto& pos : bitmap) {
                                posvecs[doc_id].push_back({pos + 1, token_index});
                            }
                        }
                    } else {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
                for (auto & [doc_id, posvec] : posvecs) {
                    std::sort(posvec.begin(), posvec.end());
                    uint64_t lvl0 = 0, lvl1 = 0;
                    for (auto & [pos, token_index]: posvec) {
                        if (pos % 2) {
                            if (tokens.size() % 2 && lvl0 + 2 == tokens.size() && lvl0 == token_index) {
                                result.push_back(doc_id);
                                break;
                            } else {
                                lvl0 = 0;
                            }
                            if (lvl1 == token_index) {
                                lvl1 += 2;
                                if (lvl1 + 1 == tokens.size()) {
                                    --lvl1;
                                }
                                if (lvl1 == tokens.size()) {
                                    result.push_back(doc_id);
                                    break;
                                }
                            } else {
                                lvl1 = 0;
                            }
                        } else {
                            if (tokens.size() % 2 && lvl1 + 2 == tokens.size() && lvl1 == token_index) {
                                result.push_back(doc_id);
                                break;
                            } else {
                                lvl1 = 0;
                            }
                            if (lvl0 == token_index) {
                                lvl0 += 2;
                                if (lvl0 + 1 == tokens.size()) {
                                    --lvl0;
                                }
                                if (lvl0 == tokens.size()) {
                                    result.push_back(doc_id);
                                    break;
                                }
                            } else {
                                lvl0 = 0;
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

private:
    class TextLineReader {
       public:
        TextLineReader(const std::string& line) {
            std::string line_2 = line;
            boost::algorithm::trim(line_2, loc_);
            iss_ = std::istringstream(FilterText(line));
        }

        std::optional<std::string> GetWord() {
            std::string word;
            if (iss_ >> word) {
                return word;
            } else {
                return std::nullopt;
            }
        }

       private:
        std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
        std::istringstream iss_;
    };

private:
    std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
    uint64_t doc_id_ = 0;
    uint64_t doc_block_size_;
    uint64_t pos_id_ = 0;
    Token prev_token_;
    std::shared_ptr<InvertedIndexImpl<TokenId, BitMap>> one_word_index_;
    std::shared_ptr<InvertedIndexImpl<Token2Id, PosMap>> biword_index_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
};

struct RankedIndexStateConfig {
    std::string work_dir;
    lsm::GranularLsmOptions lsm_options;
    uint64_t start_doc_count = 2;
    uint64_t doc_mul = 2;
    uint64_t lvl_count = 3;
};

class TFIDFIndexState final : public IIndexState {
public:
    TFIDFIndexState(const RankedIndexStateConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher) : russian_morpher_(russian_morpher), config_(config) {
        tiered_index_.resize(config_.lvl_count);
        for (uint64_t lvl = 0; lvl < config_.lvl_count; ++lvl) {
            IndexConfig index_config = {
                .index_dir = config.work_dir + "/tiered_index_" + std::to_string(lvl),
                .lsm_options = config.lsm_options
            };
            tiered_index_[lvl] = std::make_shared<InvertedIndexImpl<TokenId, TFIDF>>(index_config);
        }
        doc_count_ = 0;
        IndexConfig index_config = {
            .index_dir = config.work_dir + "/doc_stat",
            .lsm_options = config.lsm_options
        };
        stat_ = std::make_shared<InvertedIndexImpl<TokenId, DocsStat>>(index_config);
    }

    IndexType GetIndexType() const override {
        return IndexType::IT_POSITIONAL;
    }

    void StartDoc() override {
    }

    void AddToken(const Token& token) override {
        stat_->Put(TokenId(token, 0), DocsStat(doc_count_));
    }

    void FinishDoc() override {
        ++doc_count_;
    }

    void Finish() override {
        auto stat_scanner = stat_->Scan();
        auto object = stat_scanner->Next();
        while (object.has_value()) {
            double idf = std::log2(static_cast<double>(doc_count_) / static_cast<double>(object->second.in_doc_counts_.size()));
            std::vector<std::pair<double, uint64_t>> tf;
            for (auto & [doc_id, in_doc_count] : object->second.in_doc_counts_) {
                tf.push_back({1. + std::log2(in_doc_count), doc_id});
            }
            std::sort(tf.begin(), tf.end());
            for (uint64_t lvl = 0, docs = config_.start_doc_count; lvl < config_.lvl_count; ++lvl, docs *= config_.doc_mul) {
                auto end = tf.begin() + docs;
                if (docs > tf.size()) {
                    end = tf.end();
                }
                auto tfidf = TFIDF(idf, std::vector<std::pair<double, uint64_t>>(tf.begin(), end));
                tiered_index_[lvl]->Put(object->first, tfidf);
            }
            object = stat_scanner->Next();
        }
        stat_scanner = nullptr;
    }

    std::vector<uint64_t> Search(const std::string& query) const override {
        uint64_t k = 0, ind = 0;
        while ('0' <= query[ind] && query[ind] <= '9') {
            k *= 10;
            k += query[ind] - '0';
            ++ind;
        }

        auto text_line_reader = TextLineReader(query);
        std::vector<std::string> tokens;
        auto word = text_line_reader.GetWord();
        while (word.has_value()) {
            auto main_forms = GetMainForms(word.value(), russian_morpher_, loc_);
            if (!main_forms.empty()) {
                tokens.push_back(main_forms[0]);
            }
            word = text_line_reader.GetWord();
        }
        std::cout << "Search:\n";
        for (auto & token: tokens) {
            std::cout << token << '\n';
        }

        std::unordered_map<std::string, uint64_t> count;
        for (auto & token: tokens) {
            ++count[token];
        }
        std::vector<double> tf_query_vec;
        tokens.clear();
        for (auto &[token, c] : count) {
            tf_query_vec.push_back(1 + std::log2(c));
            tokens.push_back(token);
        }
        
        if (k != 0) {
            for (uint64_t lvl = 0, docs = config_.start_doc_count; lvl + 1 < config_.lvl_count; ++lvl, docs *= config_.doc_mul) {
                if (docs < k) {
                    continue;
                }
                auto result = GetRankDocs(k, tokens, tf_query_vec, tiered_index_[lvl]);
                if (result.empty()) {
                    continue;
                }
                return result;
            }
        }
        return GetRankDocs(0, tokens, tf_query_vec, tiered_index_.back());
    }

private:
    class TextLineReader {
       public:
        TextLineReader(const std::string& line) {
            std::string line_2 = line;
            boost::algorithm::trim(line_2, loc_);
            iss_ = std::istringstream(FilterText(line));
        }

        std::optional<std::string> GetWord() {
            std::string word;
            if (iss_ >> word) {
                return word;
            } else {
                return std::nullopt;
            }
        }

       private:
        std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
        std::istringstream iss_;
    };

    std::vector<uint64_t> GetRankDocs(uint64_t k, const std::vector<std::string>& tokens, const std::vector<double> tf_query_vec, const std::shared_ptr<InvertedIndexImpl<TokenId, TFIDF>>& index) const {
        std::unordered_map<uint64_t, double> doc_rels;
        for (size_t i = 0; i < tokens.size(); ++i) {
            auto object = index->GetDocId(TokenId(tokens[i], 0));
            if (object.has_value()) {
                for (auto & [tf, doc_id] : object->tf_) {
                    doc_rels[doc_id] += tf * object->idf_ * tf_query_vec[i] * object->idf_;
                }
            }
        }
        if (k != 0 && doc_rels.size() < k) {
            return {};
        }
        std::vector<std::pair<double, uint64_t>> sorted_doc_rels;
        for (auto & [doc_id, rel] : doc_rels) {
            sorted_doc_rels.push_back({rel, doc_id});
        }
        sort(sorted_doc_rels.rbegin(), sorted_doc_rels.rend());
        std::vector<uint64_t> result;
        for (auto & [rel, doc_id] : sorted_doc_rels) {
            result.push_back(doc_id);
            if (k != 0 && result.size() == k) {
                break;
            }
        }
        return result;
    }

private:
    std::shared_ptr<InvertedIndexImpl<TokenId, DocsStat>> stat_;

private:
    std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
    uint64_t doc_count_ = 0;
    std::vector<std::shared_ptr<InvertedIndexImpl<TokenId, TFIDF>>> tiered_index_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
    RankedIndexStateConfig config_;
};

class BM25IndexState final : public IIndexState {
public:
    BM25IndexState(const RankedIndexStateConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher) : russian_morpher_(russian_morpher), config_(config) {
        tiered_index_.resize(config_.lvl_count);
        for (uint64_t lvl = 0; lvl < config_.lvl_count; ++lvl) {
            IndexConfig index_config = {
                .index_dir = config.work_dir + "/tiered_index_" + std::to_string(lvl),
                .lsm_options = config.lsm_options
            };
            tiered_index_[lvl] = std::make_shared<InvertedIndexImpl<TokenId, TFIDF>>(index_config);
        }
        doc_count_ = 0;
        IndexConfig index_config = {
            .index_dir = config.work_dir + "/doc_stat",
            .lsm_options = config.lsm_options
        };
        stat_ = std::make_shared<InvertedIndexImpl<TokenId, DocsStat>>(index_config);
    }

    IndexType GetIndexType() const override {
        return IndexType::IT_POSITIONAL;
    }

    void StartDoc() override {
        doc_size_ = 0;
    }

    void AddToken(const Token& token) override {
        stat_->Put(TokenId(token, 0), DocsStat(doc_count_));
        ++doc_size_;
    }

    void FinishDoc() override {
        ++doc_count_;
        doc_sizes_.push_back(doc_size_);
        sum_doc_size_ += doc_size_;
    }

    void Finish() override {
        auto avgdl = static_cast<double>(sum_doc_size_) / doc_count_;
        auto stat_scanner = stat_->Scan();
        auto object = stat_scanner->Next();
        while (object.has_value()) {
            double idf = std::log2(static_cast<double>(doc_count_) / static_cast<double>(object->second.in_doc_counts_.size()));
            std::vector<std::pair<double, uint64_t>> tf;
            for (auto & [doc_id, in_doc_count] : object->second.in_doc_counts_) {
                auto short_tf = 1. + std::log2(in_doc_count);
                tf.push_back({(short_tf * (k_1_ + 1)) / (short_tf + k_1_ * (1 - b_ + b_ * doc_sizes_[doc_id] / avgdl)), doc_id});
            }
            std::sort(tf.begin(), tf.end());
            for (uint64_t lvl = 0, docs = config_.start_doc_count; lvl < config_.lvl_count; ++lvl, docs *= config_.doc_mul) {
                auto end = tf.begin() + docs;
                if (docs > tf.size()) {
                    end = tf.end();
                }
                auto tfidf = TFIDF(idf, std::vector<std::pair<double, uint64_t>>(tf.begin(), end));
                tiered_index_[lvl]->Put(object->first, tfidf);
            }
            object = stat_scanner->Next();
        }
        stat_scanner = nullptr;
    }

    std::vector<uint64_t> Search(const std::string& query) const override {
        uint64_t k = 0, ind = 0;
        while ('0' <= query[ind] && query[ind] <= '9') {
            k *= 10;
            k += query[ind] - '0';
            ++ind;
        }

        auto text_line_reader = TextLineReader(query);
        std::vector<std::string> tokens;
        auto word = text_line_reader.GetWord();
        while (word.has_value()) {
            auto main_forms = GetMainForms(word.value(), russian_morpher_, loc_);
            if (!main_forms.empty()) {
                tokens.push_back(main_forms[0]);
            }
            word = text_line_reader.GetWord();
        }
        std::cout << "Search:\n";
        for (auto & token: tokens) {
            std::cout << token << '\n';
        }
        
        if (k != 0) {
            for (uint64_t lvl = 0, docs = config_.start_doc_count; lvl + 1 < config_.lvl_count; ++lvl, docs *= config_.doc_mul) {
                if (docs < k) {
                    continue;
                }
                auto result = GetRankDocs(k, tokens, tiered_index_[lvl]);
                if (result.empty()) {
                    continue;
                }
                return result;
            }
        }
        return GetRankDocs(0, tokens, tiered_index_.back());
    }

private:
    class TextLineReader {
       public:
        TextLineReader(const std::string& line) {
            std::string line_2 = line;
            boost::algorithm::trim(line_2, loc_);
            iss_ = std::istringstream(FilterText(line));
        }

        std::optional<std::string> GetWord() {
            std::string word;
            if (iss_ >> word) {
                return word;
            } else {
                return std::nullopt;
            }
        }

       private:
        std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
        std::istringstream iss_;
    };

    std::vector<uint64_t> GetRankDocs(uint64_t k, const std::vector<std::string>& tokens, const std::shared_ptr<InvertedIndexImpl<TokenId, TFIDF>>& index) const {
        std::unordered_map<uint64_t, double> doc_rels;
        for (size_t i = 0; i < tokens.size(); ++i) {
            auto object = index->GetDocId(TokenId(tokens[i], 0));
            if (object.has_value()) {
                for (auto & [tf, doc_id] : object->tf_) {
                    doc_rels[doc_id] += tf * object->idf_;
                }
            }
        }
        if (k != 0 && doc_rels.size() < k) {
            return {};
        }
        std::vector<std::pair<double, uint64_t>> sorted_doc_rels;
        for (auto & [doc_id, rel] : doc_rels) {
            sorted_doc_rels.push_back({rel, doc_id});
        }
        sort(sorted_doc_rels.rbegin(), sorted_doc_rels.rend());
        std::vector<uint64_t> result;
        for (auto & [rel, doc_id] : sorted_doc_rels) {
            result.push_back(doc_id);
            if (k != 0 && result.size() == k) {
                break;
            }
        }
        return result;
    }

private:
    std::shared_ptr<InvertedIndexImpl<TokenId, DocsStat>> stat_;
    std::vector<uint64_t> doc_sizes_;
    uint64_t doc_size_ = 0;
    uint64_t sum_doc_size_ = 0;

private:
    double k_1_ = 2.0;
    double b_ = 0.75;
    std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
    uint64_t doc_count_ = 0;
    std::vector<std::shared_ptr<InvertedIndexImpl<TokenId, TFIDF>>> tiered_index_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
    RankedIndexStateConfig config_;
};

class IndexerImpl : public IIndexer {
   public:
    IndexerImpl(const IndexerConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher) : config_(config), russian_morpher_(russian_morpher) {
    }

    void Process() override {
        BuildDocList();
        BuildIndex();
    }

    std::vector<std::string> GetDocList() const override { return docs_; }

    std::shared_ptr<const IIndexState> GetIndexState() const override { return index_state_; }

   private:
    void BuildDocList() {
        for (const std::filesystem::directory_entry& e : std::filesystem::recursive_directory_iterator(config_.docbase_dir, std::filesystem::directory_options::skip_permission_denied)) {
            if (e.is_regular_file()) {
                docs_.push_back(e.path().string());
            }
        }
    }

    class DocReader {
       public:
        DocReader(const std::string& path) : file_(path), iss_() {}

        std::optional<std::string> GetWord() {
            std::string word;
            if (iss_ >> word) {
                return word;
            } else {
                std::string line;
                while (std::getline(file_, line)) {
                    boost::algorithm::trim(line, loc_);
                    line = FilterText(line);
                    if (!line.empty()) {
                        iss_ = std::istringstream(line);
                        return GetWord();
                    }
                }
                return std::nullopt;
            }
        }

       private:
        std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
        std::ifstream file_;
        std::istringstream iss_;
    };

    std::vector<Token> GetTokens(const std::string& word) {
        auto tokens = GetMainForms(word, russian_morpher_, loc_);
        return tokens;
    }

    void BuildIndex() {
        if (config_.index_type == IT_POSITIONAL) {
            PositionalIndexStateConfig index_config = {
                .doc_block_size = config_.doc_block_size,
                .work_dir = config_.work_dir,
                .lsm_options = config_.lsm_options
            };
            index_state_ = std::make_shared<PositionalIndexState>(index_config, russian_morpher_);
        } else if (config_.index_type == IT_RANKED_TFIDF) {
            RankedIndexStateConfig index_config = {
                .work_dir = config_.work_dir,
                .lsm_options = config_.lsm_options,
            };
            index_config.lvl_count = 1;
            uint64_t docs_count = index_config.start_doc_count;
            while (docs_count < docs_.size()) {
                docs_count *= index_config.doc_mul;
                ++index_config.lvl_count;
            }
            index_state_ = std::make_shared<TFIDFIndexState>(index_config, russian_morpher_);
        } else if (config_.index_type == IT_RANKED_BM25) {
            RankedIndexStateConfig index_config = {
                .work_dir = config_.work_dir,
                .lsm_options = config_.lsm_options,
            };
            index_config.lvl_count = 1;
            uint64_t docs_count = index_config.start_doc_count;
            while (docs_count < docs_.size()) {
                docs_count *= index_config.doc_mul;
                ++index_config.lvl_count;
            }
            index_state_ = std::make_shared<BM25IndexState>(index_config, russian_morpher_);
        }
        for (uint32_t doc_id = 0; doc_id < docs_.size(); ++doc_id) {
            IndexDoc(doc_id);
        }
        index_state_->Finish();
    }

    void IndexDoc(uint32_t doc_id) {
        std::cout << "indexes doc " << doc_id << ": " << docs_[doc_id] << '\n';
        auto doc_reader = DocReader(docs_[doc_id]);

        std::optional<std::string> word;
        index_state_->StartDoc();
        while ((word = doc_reader.GetWord())) {
            for (const auto& token : GetTokens(*word)) {
                index_state_->AddToken(token);
            }
        }
        index_state_->FinishDoc();
    }

   private:
    std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
    IndexerConfig config_;
    std::vector<std::string> docs_;
    std::shared_ptr<IIndexState> index_state_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
};

}  // namespace

std::unique_ptr<IIndexer> MakeIndexer(const IndexerConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher) { return std::make_unique<IndexerImpl>(config, russian_morpher); }

}  // namespace invindex