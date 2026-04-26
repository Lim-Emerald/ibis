#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <index/common/morpher.h>
#include <index/common/utils.h>
#include <index/index/index.h>

#include <cassert>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "index/common/types.h"

namespace invindex {
namespace {

static const std::string kPrefixDocbasePath = "/home/lim/HSE/Projects/IBIS/docbase";
static const std::string kLangDictPath = "/home/lim/HSE/Projects/IBIS/lang_dict";

std::vector<std::string> PrintResponse(const std::string& query, const std::vector<uint64_t>& doc_ids, const std::vector<std::string>& docs) {
    std::vector<std::string> result;
    std::cout << "---\n";
    std::cout << "Запрос: " << query << '\n';
    if (doc_ids.empty()) {
        std::cout << "Документы не найдены\n";
    } else {
        std::cout << "Найдены документы:\n";
        for (const auto& ind : doc_ids) {
            std::cout << docs[ind] << '\n';
            result.push_back(docs[ind].substr(kPrefixDocbasePath.size() + 7));
        }
    }
    return result;
}

static std::vector<std::string> docs;
static std::shared_ptr<const IIndexState> index_state;

void ResetIndex() { index_state = nullptr; }

TEST(SmallVecRankIndex, Build) {
    std::filesystem::remove_all("small_index");
    std::filesystem::create_directory("small_index");
    ResetIndex();
    lsm::GranularLsmOptions lsm_options;
    // lsm_options.frame_size = 256;
    // lsm_options.bloom_filter_size = 16 * 1024;
    // lsm_options.buffer_pool_size = 16 * 1024;
    // lsm_options.memtable_bytes = 16 * 1024;
    // lsm_options.max_sstable_size = 64 * 1024;
    auto indexer_config = IndexerConfig();
    indexer_config.lsm_options = lsm_options;
    indexer_config.docbase_dir = kPrefixDocbasePath + "/small";
    indexer_config.work_dir = "small_index";
    indexer_config.index_type = IndexType::IT_RANKED_TFIDF;
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto indexer = MakeIndexer(indexer_config, russian_morpher);
    indexer->Process();
    docs = indexer->GetDocList();
    index_state = indexer->GetIndexState();
}

TEST(SmallVecRankIndex, OneToken) {
    std::vector<std::pair<Token, std::vector<std::string>>> queries = {
        {"деньги",
         {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt", "Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt",
          "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt", "Mireckiy_Arhivarius_RuLit_Me.txt",
          "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt",
          "Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt"}},
        {"спокойствие",
         {"Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt",
          "Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt"}},
        {"электроэнергия", {}},
        {"за", {}},
        {"под", {}},
    };

    for (auto& [token, expected_docs] : queries) {
        auto doc_ids = index_state->Search(token);
        auto result = PrintResponse(token, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

TEST(SmallVecRankIndex, Phrases) {
    std::vector<std::pair<Token, std::vector<std::string>>> queries = {
        {"Истекая кровью",
         {"Mireckiy_Arhivarius_RuLit_Me.txt", "Poselyagin_Ya-vyzhivu_RuLit_Net.txt",
          "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt",
          "Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt"}},
        {"насколько это возможно",
         {"_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt", "Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt",
         "samyjj_bogatyjj_chelovek_v_vavilone.u.txt",
          "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt", "Mireckiy_Arhivarius_RuLit_Me.txt",
          "Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt", "Anatolevich_Zelyonyy-dom_1_Velikan-na-polyane-ili-pervye-uroki-ekologicheskoy-etiki_RuLit_Net.txt",
          "Poselyagin_Ya-vyzhivu_RuLit_Net.txt"}},
        {"3 делаю много довольно смелых заявлений",
         {"Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt", "Anatolevich_Zelyonyy-dom_1_Velikan-na-polyane-ili-pervye-uroki-ekologicheskoy-etiki_RuLit_Net.txt",
          "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt"}},
    };

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = index_state->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

TEST(SmallBM25RankIndex, Build) {
    std::filesystem::remove_all("small_index");
    std::filesystem::create_directory("small_index");
    ResetIndex();
    lsm::GranularLsmOptions lsm_options;
    // lsm_options.frame_size = 256;
    // lsm_options.bloom_filter_size = 16 * 1024;
    // lsm_options.buffer_pool_size = 16 * 1024;
    // lsm_options.memtable_bytes = 16 * 1024;
    // lsm_options.max_sstable_size = 64 * 1024;
    auto indexer_config = IndexerConfig();
    indexer_config.lsm_options = lsm_options;
    indexer_config.docbase_dir = kPrefixDocbasePath + "/small";
    indexer_config.work_dir = "small_index";
    indexer_config.index_type = IndexType::IT_RANKED_BM25;
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto indexer = MakeIndexer(indexer_config, russian_morpher);
    indexer->Process();
    docs = indexer->GetDocList();
    index_state = indexer->GetIndexState();
}

TEST(SmallBM25RankIndex, OneToken) {
    std::vector<std::pair<Token, std::vector<std::string>>> queries = {
        {"2 деньги", {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt", "Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt"}},
        {"2 спокойствие",
         {"Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt",
          "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt"}},
        {"электроэнергия", {}},
        {"за", {}},
        {"под", {}},
    };

    for (auto& [token, expected_docs] : queries) {
        auto doc_ids = index_state->Search(token);
        auto result = PrintResponse(token, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

TEST(SmallBM25RankIndex, Phrases) {
    std::vector<std::pair<Token, std::vector<std::string>>> queries = {
        {"Истекая кровью",
         {"Mireckiy_Arhivarius_RuLit_Me.txt", "Poselyagin_Ya-vyzhivu_RuLit_Net.txt", "Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt",
          "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt"}},
        {"4 насколько это возможно",
         {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt", "Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt",
          "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt", "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt"}},
        {"1 делаю много довольно смелых заявлений", {"Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt"}},
    };

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = index_state->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

}  // namespace
}  // namespace invindex
