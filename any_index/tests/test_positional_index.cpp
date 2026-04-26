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

void ResetIndex() {
    index_state = nullptr;
}

TEST(SmallPositionalIndex, Build) {
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
    indexer_config.index_type = IndexType::IT_POSITIONAL;
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto indexer = MakeIndexer(indexer_config, russian_morpher);
    indexer->Process();
    docs = indexer->GetDocList();
    index_state = indexer->GetIndexState();
}

TEST(SmallPositionalIndex, OneToken) {
    std::vector<std::pair<Token, std::vector<std::string>>> queries = {
        {"деньги",
         {"Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt", "Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt",
          "Mireckiy_Arhivarius_RuLit_Me.txt", "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt",
          "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt", "samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
        {"спокойствие",
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

TEST(SmallPositionalIndex, Phrases) {
    std::vector<std::pair<Token, std::vector<std::string>>> queries = {
        {"Истекая кровью",
         {"Mireckiy_Arhivarius_RuLit_Me.txt"}},
        {"Кровью истекая", {}},
        {"насколько это возможно",
         {"Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt"}},
        {"делаю много довольно смелых заявлений", {"Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt"}},
        {"друзья возобновили прерванную накануне беседу с «духом Гоголя»", {"_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt"}},
        {"прерванную накануне беседу с «духом Гоголя» друзья возобновили", {}},
    };

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = index_state->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

// TEST(SmallPositionalIndex, YourQuery) {
//     std::cout << "---\n";
//     std::cout << "Введите запрос:\n";
//     std::string query;
//     while (std::getline(std::cin, query)) {
//         auto doc_ids = index_state->Search(query);
//         PrintResponse(query, doc_ids, docs);
//         std::cout << "---\n";
//         std::cout << "Введите запрос:\n";
//     }
//     std::cout << "---\n";
//     std::cout << "Остановка обработки запросов\n";
//     std::cout << "---\n";
// }

// TEST(LargeInvertedIndex, Build) {
//     std::filesystem::remove_all("large_index");
//     std::filesystem::create_directory("large_index");
//     ResetIndex();
//     lsm::GranularLsmOptions lsm_options;
//     index_config = IndexConfig();
//     index_config.lsm_options = lsm_options;
//     index_config.use_word_dictionary = false;
//     CrawlerConfig config;
//     config.work_dir = "large_index";
//     config.docbase_dir = kPrefixDocbasePath + "/large";
//     config.index_config = index_config;
//     auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

//     auto crawler = MakeCrawler(config, russian_morpher);
//     crawler->Process();

//     word_dictionary = crawler->GetWordDictionary();
//     index = crawler->GetInvertedIndex();
//     docs = crawler->GetDocList();
//     index_config.doc_count = docs.size();
// }

// TEST(LargeInvertedIndex, YourQuery) {
//     auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
//     auto search_engine = MakeSearchEngine(index_config, index, word_dictionary, russian_morpher);

//     std::cout << "---\n";
//     std::cout << "Введите запрос:\n";
//     std::string query;
//     while (std::getline(std::cin, query)) {
//         auto doc_ids = search_engine->Search(query);
//         PrintResponse(query, doc_ids, docs);
//         std::cout << "---\n";
//         std::cout << "Введите запрос:\n";
//     }
//     std::cout << "---\n";
//     std::cout << "Остановка обработки запросов\n";
//     std::cout << "---\n";
// }

}  // namespace
}  // namespace invindex
