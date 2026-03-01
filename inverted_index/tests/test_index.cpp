#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <index/common/morpher.h>
#include <index/common/types.h>
#include <index/invindex/index.h>
#include <index/lsm/lsm.h>
#include <index/lsm/sstable.h>
#include <sys/stat.h>

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

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

static std::vector<std::string> docs;
static std::shared_ptr<const IInvertedIndex> index;

void ResetIndex() {
    docs = {};
    index = nullptr;
}

TEST(SmallInvertedIndex, Build) {
    ResetIndex();
    lsm::GranularLsmOptions options;
    options.frame_size = 1024;
    options.buffer_pool_size = 4096;
    options.memtable_bytes = 1024;
    options.max_sstable_size = 4096;
    options.bloom_filter_size = 1024;
    auto index_builder = MakeInvertedIndexBuilder("test_index", options);
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto crawler = MakeCrawler(kPrefixDocbasePath + "/small", index_builder, russian_morpher);
    crawler->Process();

    docs = crawler->GetDocList();
    index = index_builder->GetInvertedIndex();
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
        {"электроэнергия", {}}};

    for (auto& [token, expected_docs] : queries) {
        auto doc_ids = index->GetDocId(token);
        PrintResponse(token, doc_ids, docs);
    }
    std::cout << "---\n";
}

TEST(SmallInvertedIndex, SearchQuery) {
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
    auto search_engine = MakeSearchEngine(index, russian_morpher);

    std::vector<std::pair<std::string, std::vector<std::string>>> queries = {
        {"заработать деньги", {"Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt", "Mireckiy_Arhivarius_RuLit_Me.txt", "samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}},
        {"следы кетчупа с горчицей", {"Mireckiy_Arhivarius_RuLit_Me.txt"}},
        {"Парадоксальный способ жить счастливо", {"Menson_Tonkoe-iskusstvo-pofigizma-Paradoksalnyy-sposob-zhit-schastlivo_RuLit_Me.txt"}},
        {"Начните пополнять кошелек", {"samyjj_bogatyjj_chelovek_v_vavilone.u.txt"}}};

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = search_engine->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

// TEST(SmallInvertedIndex, YourQuery) {
//     auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
//     auto search_engine = MakeSearchEngine(index, russian_morpher);

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
    ResetIndex();
    lsm::GranularLsmOptions options;
    options.frame_size = 4096;
    options.buffer_pool_size = 256 * 1024;
    options.memtable_bytes = 256 * 1024;
    options.max_sstable_size = 512 * 1024;
    options.bloom_filter_size = 256 * 1024;
    auto index_builder = MakeInvertedIndexBuilder("test_index", options);
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");

    auto crawler = MakeCrawler(kPrefixDocbasePath + "/large", index_builder, russian_morpher);
    crawler->Process();

    docs = crawler->GetDocList();
    index = index_builder->GetInvertedIndex();
}

TEST(LargeInvertedIndex, SearchQuery) {
    auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
    auto search_engine = MakeSearchEngine(index, russian_morpher);

    std::vector<std::pair<std::string, std::vector<std::string>>> queries = {
        {"эквестрия", {"Avtor_neizvesten_Follaut_Ekvestriya_RuLit_Net.txt"}},
        {"Гарри Поттер",
         {"garri-potter-i-dary-smerti_RuLit_Net_114101.txt", "garri_potter_i_kubok_ognya.u.txt", "garri-potter-i-filosofskij-kamen-garri-potter-1_RuLit_Net_103173.txt",
          "Rouling_Garri-Potter-perevod-Marii-Spivak-_3_Garri-Potter-i-uznik-Azkabana_RuLit_Me.txt"}},
        {"крестраж", {"garri-potter-i-dary-smerti_RuLit_Net_114101.txt"}},
        {"она была пленницей викинга", {"plennitsa_vikinga.u.txt"}}};

    for (auto& [query, expected_docs] : queries) {
        auto doc_ids = search_engine->Search(query);
        auto result = PrintResponse(query, doc_ids, docs);
        ASSERT_EQ(result, expected_docs);
    }
    std::cout << "---\n";
}

// TEST(LargeInvertedIndex, YourQuery) {
//     auto russian_morpher = MakeRussianMorpher(kLangDictPath + "/rus_dict.bin");
//     auto search_engine = MakeSearchEngine(index, russian_morpher);

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
