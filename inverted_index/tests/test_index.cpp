#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <index/common/morpher.h>
#include <index/common/utils.h>
#include <index/invindex/index.h>

#include <cassert>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "index/common/types.h"

namespace invindex {
namespace {

static const std::string kPrefixDocbasePath = "/home/lim/HSE/Projects/IBIS/inverted_index/tests/docbase";
static const std::string kLangDictPath = "/home/lim/HSE/Projects/IBIS/inverted_index/tests/lang_dict";

std::vector<std::string> PrintResponse(const std::string& query, const PostingList& doc_ids, const std::vector<std::string>& docs) {
    std::vector<std::string> result;
    std::cout << "---\n";
    std::cout << "Запрос: " << query << '\n';
    if (doc_ids.isEmpty()) {
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

static IndexConfig index_config;
static std::vector<std::string> docs;
static std::shared_ptr<const IInvertedIndex> index;
static std::shared_ptr<const lsm::ILSM<lsm::DefaultValue>> word_dictionary;

void ResetIndex() {
    docs = {};
    index = nullptr;
}

TEST(SmallInvertedIndex, Build) {
    std::filesystem::remove_all("small_index");
    std::filesystem::create_directory("small_index");
    ResetIndex();
    lsm::GranularLsmOptions lsm_options;
    lsm_options.frame_size = 256;
    lsm_options.bloom_filter_size = 16 * 1024;
    lsm_options.buffer_pool_size = 16 * 1024;
    lsm_options.memtable_bytes = 16 * 1024;
    lsm_options.max_sstable_size = 64 * 1024;
    index_config = IndexConfig();
    index_config.lsm_options = lsm_options;
    index_config.use_word_dictionary = true;
    // index_config.doc_block_size = 1;
    CrawlerConfig config;
    config.index_config = index_config;
    config.work_dir = "small_index";
    config.docbase_dir = kPrefixDocbasePath + "/small";
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto crawler = MakeCrawler(config, russian_morpher);
    crawler->Process();

    word_dictionary = crawler->GetWordDictionary();
    index = crawler->GetInvertedIndex();
    docs = crawler->GetDocList();
    index_config.doc_count = docs.size();
}

TEST(SmallInvertedIndex, OneToken) {
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
        Token special_token = "^" + token + "$";
        PostingList doc_ids;
        if (index_config.use_word_dictionary) {
            auto s_feature_id = word_dictionary->Get(lsm::UserKey(special_token.begin(), special_token.end()));
            if (s_feature_id) {
                doc_ids = index->GetDocId(TokenId{0, DeserializeUint32(*s_feature_id)}.Serialize());
            }
        } else {
            doc_ids = index->GetDocId(TokenWithBlockId{0, special_token}.Serialize());
        }
        auto result = PrintResponse(token, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

TEST(SmallInvertedIndex, SimpleSearch) {
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
    auto search_engine = MakeSearchEngine(index_config, index, word_dictionary, russian_morpher);

    std::vector<std::pair<std::string, std::vector<std::string>>> queries = {
        {"ВАВИЛОН", {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
        {"( заработать & деньги )",
         {"Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt", "Mireckiy_Arhivarius_RuLit_Me.txt", "samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
        {"( следы & кетчупа & с & горчицей )", {"Mireckiy_Arhivarius_RuLit_Me.txt"}},
        {"( Парадоксальный & способ & жить & счастливо)", {"Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt"}},
        {"( Начните & пополнять & кошелек )", {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}}};

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = search_engine->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

TEST(SmallInvertedIndex, PrefixSearch) {
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
    auto search_engine = MakeSearchEngine(index_config, index, word_dictionary, russian_morpher);

    lsm::UserKey a = {0, 1, 2};
    lsm::UserKey b = {1, 1};

    std::vector<std::pair<std::string, std::vector<std::string>>> queries = {
        {"куш*", {"Poselyagin_Ya-vyzhivu_RuLit_Net.txt"}},
        {"Вави*", {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
        {"сза*",
         {"Poselyagin_Ya-vyzhivu_RuLit_Net.txt", "Mireckiy_Arhivarius_RuLit_Me.txt", "Anatolevich_Zelyonyy-dom_1_Velikan-na-polyane-ili-pervye-uroki-ekologicheskoy-etiki_RuLit_Net.txt",
          "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt"}}};

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = search_engine->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

TEST(SmallInvertedIndex, DateSearch) {
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
    auto search_engine = MakeSearchEngine(index_config, index, word_dictionary, russian_morpher);

    lsm::UserKey a = {0, 1, 2};
    lsm::UserKey b = {1, 1};

    std::vector<std::pair<std::string, std::vector<std::string>>> queries = {
        {"{ 2010-01-02 $creation_date * }",
         {"Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt", "Poselyagin_Ya-vyzhivu_RuLit_Net.txt",
          "Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt", "Mireckiy_Arhivarius_RuLit_Me.txt",
          "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt"}},
        {"{ * $creation_date 2010-01-01 }",
         {"Anatolevich_Zelyonyy-dom_1_Velikan-na-polyane-ili-pervye-uroki-ekologicheskoy-etiki_RuLit_Net.txt",
          "Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt", "samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
        {"{ 1970-01-01 $creation_date 2000-01-01 }",
         {"Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt", "samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
        {"[ 2010-01-01 : 2026-03-15 ]",
         {"Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt", "samyjj_bogatyjj_chelovek_v_vavilone.u.txt",
          "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt"}},
        {"[ 2025-01-01 : 2025-09-01 ]",
         {"Grey_Muzhchiny-s-Marsa-zhenshchiny-s-Venery-Kak-dumat-effektivnee-Praktiki-dlya-razvitiya-vashego-mozga_RuLit_Me.txt", "samyjj_bogatyjj_chelovek_v_vavilone.u.txt",
          "_Shevkunov_Nesvyatyie_svyatyie_i_drugie_rasskazyi_RuLit_Net.txt"}}};

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = search_engine->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

// TEST(SmallInvertedIndex, YourQuery) {
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

TEST(SmallInvertedIndex, BuildKGram) {
    std::filesystem::remove_all("small_index_kgram");
    std::filesystem::create_directory("small_index_kgram");
    ResetIndex();
    lsm::GranularLsmOptions lsm_options;
    index_config = IndexConfig();
    index_config.lsm_options = lsm_options;
    index_config.use_k_gram = 6;
    index_config.use_word_dictionary = false;
    CrawlerConfig config;
    config.work_dir = "small_index_kgram";
    config.docbase_dir = kPrefixDocbasePath + "/small";
    config.index_config = index_config;
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto crawler = MakeCrawler(config, russian_morpher);
    crawler->Process();

    word_dictionary = crawler->GetWordDictionary();
    index = crawler->GetInvertedIndex();
    docs = crawler->GetDocList();
    index_config.doc_count = docs.size();
}

TEST(SmallInvertedIndex, WildcardSearch) {
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
    auto search_engine = MakeSearchEngine(index_config, index, word_dictionary, russian_morpher);

    std::vector<std::pair<std::string, std::vector<std::string>>> queries = {{"ВАВ*он", {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
                                                                             {"авток*фа", {"Elrod_Magiya-utra-Kak-pervyy-chas-dnya-opredelyaet-vash-uspeh_RuLit_Me.txt"}},
                                                                             {"*^игра$*^здесь*", {"Mireckiy_Arhivarius_RuLit_Me.txt"}},
                                                                             {"*^под$*", {}}};

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = search_engine->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

// TEST(SmallInvertedIndex, YourWildcardQuery) {
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

TEST(LargeInvertedIndex, Build) {
    std::filesystem::remove_all("large_index");
    std::filesystem::create_directory("large_index");
    ResetIndex();
    lsm::GranularLsmOptions lsm_options;
    index_config = IndexConfig();
    index_config.lsm_options = lsm_options;
    index_config.use_word_dictionary = false;
    CrawlerConfig config;
    config.work_dir = "large_index";
    config.docbase_dir = kPrefixDocbasePath + "/large";
    config.index_config = index_config;
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto crawler = MakeCrawler(config, russian_morpher);
    crawler->Process();

    word_dictionary = crawler->GetWordDictionary();
    index = crawler->GetInvertedIndex();
    docs = crawler->GetDocList();
    index_config.doc_count = docs.size();
}

TEST(LargeInvertedIndex, SimpleSearch) {
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
    auto search_engine = MakeSearchEngine(index_config, index, word_dictionary, russian_morpher);

    std::vector<std::pair<std::string, std::vector<std::string>>> queries = {
        {"эквестрия", {"Avtor_neizvesten_Follaut_Ekvestriya_RuLit_Net.txt"}},
        {"( Гарри & Поттер )",
         {"garri-potter-i-dary-smerti_RuLit_Net_114101.txt", "garri_potter_i_kubok_ognya.u.txt", "garri-potter-i-filosofskij-kamen-garri-potter-1_RuLit_Net_103173.txt",
          "Rouling_Garri-Potter-perevod-Marii-Spivak-_3_Garri-Potter-i-uznik-Azkabana_RuLit_Me.txt"}},
        {"крестраж", {"garri-potter-i-dary-smerti_RuLit_Net_114101.txt"}},
        {"( она & была & пленницей & викинга )", {"plennitsa_vikinga.u.txt"}}};

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = search_engine->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

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
