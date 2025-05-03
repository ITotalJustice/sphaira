#include "i18n.hpp"
#include "fs.hpp"
#include "log.hpp"
#include <yyjson.h>
#include <vector>
#include <unordered_map>

namespace sphaira::i18n {
namespace {

std::vector<u8> g_i18n_data;
yyjson_doc* json;
yyjson_val* root;
std::unordered_map<std::string, std::string> g_tr_cache;

std::string get_internal(std::string_view str) {
    const std::string kkey = {str.data(), str.length()};

    if (auto it = g_tr_cache.find(kkey); it != g_tr_cache.end()) {
        return it->second;
    }

    // add default entry
    const auto it = g_tr_cache.emplace(kkey, kkey).first;

    if (!json || !root) {
        log_write("no json or root\n");
        return kkey;
    }

    auto key = yyjson_obj_getn(root, str.data(), str.length());
    if (!key) {
        log_write("\tfailed to find key: [%s]\n", kkey.c_str());
        return kkey;
    }

    auto val = yyjson_get_str(key);
    auto val_len = yyjson_get_len(key);
    if (!val || !val_len) {
        log_write("\tfailed to get value: [%s]\n", kkey.c_str());
        return kkey;
    }

    // update entry in cache
    const std::string ret = {val, val_len};
    g_tr_cache.insert_or_assign(it, kkey, ret);
    return ret;
}

} // namespace

bool init(long index) {
    g_tr_cache.clear();
    R_TRY_RESULT(romfsInit(), false);
    ON_SCOPE_EXIT( romfsExit() );

    u64 languageCode;
    SetLanguage setLanguage = SetLanguage_ENGB;
    std::string lang_name = "en";

    switch (index) {
        case 0: // auto
            if (R_SUCCEEDED(setGetSystemLanguage(&languageCode))) {
                setMakeLanguage(languageCode, &setLanguage);
            }
            break;

        case 1: setLanguage = SetLanguage_ENGB; break; // "English"
        case 2: setLanguage = SetLanguage_JA; break; // "Japanese"
        case 3: setLanguage = SetLanguage_FR; break; // "French"
        case 4: setLanguage = SetLanguage_DE; break; // "German"
        case 5: setLanguage = SetLanguage_IT; break; // "Italian"
        case 6: setLanguage = SetLanguage_ES; break; // "Spanish"
        case 7: setLanguage = SetLanguage_ZHCN; break; // "Chinese"
        case 8: setLanguage = SetLanguage_KO; break; // "Korean"
        case 9: setLanguage = SetLanguage_NL; break; // "Dutch"
        case 10: setLanguage = SetLanguage_PT; break; // "Portuguese"
        case 11: setLanguage = SetLanguage_RU; break; // "Russian"
        case 12: lang_name = "se"; break; // "Swedish"
        case 13: lang_name = "vi"; break; // "Vietnamese"
    }

    switch (setLanguage) {
        case SetLanguage_JA: lang_name = "ja"; break;
        case SetLanguage_FR: lang_name = "fr"; break;
        case SetLanguage_DE: lang_name = "de"; break;
        case SetLanguage_IT: lang_name = "it"; break;
        case SetLanguage_ES: lang_name = "es"; break;
        case SetLanguage_ZHCN: lang_name = "zh"; break;
        case SetLanguage_KO: lang_name = "ko"; break;
        case SetLanguage_NL: lang_name = "nl"; break;
        case SetLanguage_PT: lang_name = "pt"; break;
        case SetLanguage_RU: lang_name = "ru"; break;
        case SetLanguage_ZHTW: lang_name = "zh"; break;
        default: break;
    }

    const fs::FsPath sdmc_path = "/config/sphaira/i18n/" + lang_name + ".json";
    const fs::FsPath romfs_path = "romfs:/i18n/" + lang_name + ".json";
    fs::FsPath path = sdmc_path;

    // try and load override translation first
    Result rc = fs::FsNativeSd().read_entire_file(path, g_i18n_data);
    if (R_FAILED(rc)) {
        path = romfs_path;
        rc = fs::FsStdio().read_entire_file(path, g_i18n_data);
    }

    if (R_SUCCEEDED(rc)) {
        json = yyjson_read((const char*)g_i18n_data.data(), g_i18n_data.size(), YYJSON_READ_ALLOW_TRAILING_COMMAS|YYJSON_READ_ALLOW_COMMENTS|YYJSON_READ_ALLOW_INVALID_UNICODE);
        if (json) {
            root = yyjson_doc_get_root(json);
            if (root) {
                log_write("opened json: %s\n", path.s);
                return true;
            } else {
                log_write("failed to find root\n");
            }
        } else {
            log_write("failed open json\n");
        }
    } else {
        log_write("failed to read file\n");
    }

    return false;
}

void exit() {
    if (json) {
        yyjson_doc_free(json);
        json = nullptr;
    }
    g_i18n_data.clear();
}

std::string get(std::string_view str) {
    return get_internal(str);
}

} // namespace sphaira::i18n

namespace literals {

std::string operator"" _i18n(const char* str, size_t len) {
    return sphaira::i18n::get_internal({str, len});
}

} // namespace literals
