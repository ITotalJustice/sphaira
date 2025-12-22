#include "ui/menus/ghdl.hpp"
#include "ui/menus/homebrew.hpp"

#include "ui/sidebar.hpp"
#include "swkbd.hpp"
#include "ui/option_box.hpp"
#include "ui/popup_list.hpp"
#include "ui/progress_box.hpp"
#include "ui/error_box.hpp"
#include "ui/nvg_util.hpp"

#include "log.hpp"
#include "app.hpp"
#include "fs.hpp"
#include "defines.hpp"
#include "image.hpp"
#include "download.hpp"
#include "i18n.hpp"
#include "yyjson_helper.hpp"
#include "threaded_file_transfer.hpp"

#include <minIni.h>
#include <dirent.h>
#include <cstring>
#include <string>

namespace sphaira::ui::menu::gh {
namespace {

constexpr auto CACHE_PATH = "/switch/sphaira/cache/github";

auto GenerateApiUrl(const Entry& e) {
    if (e.tag.empty()) {
        return "https://api.github.com/repos/" + e.owner + "/" + e.repo + "/releases";
    } else if (e.tag == "latest") {
        return "https://api.github.com/repos/" + e.owner + "/" + e.repo + "/releases/latest";
    } else {
        return "https://api.github.com/repos/" + e.owner + "/" + e.repo + "/releases/tags/" + e.tag;
    }
}

auto apiBuildAssetCache(const std::string& url) -> fs::FsPath {
    fs::FsPath path;
    std::snprintf(path, sizeof(path), "%s/%u.json", CACHE_PATH, crc32Calculate(url.data(), url.size()));
    return path;
}

void from_json(yyjson_val* json, AssetEntry& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(name);
        JSON_SET_STR(path);
        JSON_SET_STR(pre_install_message);
        JSON_SET_STR(post_install_message);
    );
}

void from_json(const fs::FsPath& path, Entry& e) {
    JSON_INIT_VEC_FILE(path, nullptr, nullptr);
    JSON_OBJ_ITR(
        JSON_SET_STR(url);
        JSON_SET_STR(owner);
        JSON_SET_STR(repo);
        JSON_SET_STR(tag);
        JSON_SET_STR(pre_install_message);
        JSON_SET_STR(post_install_message);
        JSON_SET_ARR_OBJ(assets);
        JSON_SET_STR(direct_url);
    );
}

void from_json(yyjson_val* json, GhApiAsset& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(name);
        JSON_SET_STR(content_type);
        JSON_SET_UINT(size);
        JSON_SET_UINT(download_count);
        JSON_SET_STR(updated_at);
        JSON_SET_STR(browser_download_url);
    );
}

void from_json(yyjson_val* json, GhApiEntry& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(tag_name);
        JSON_SET_STR(name);
        JSON_SET_STR(published_at);
        JSON_SET_BOOL(prerelease);
        JSON_SET_ARR_OBJ(assets);
    );
}

void from_json(const fs::FsPath& path, std::vector<GhApiEntry>& e) {
    JSON_INIT_VEC_FILE(path, nullptr, nullptr);
    if (yyjson_is_arr(json)) {
        JSON_ARR_ITR(e);
    } else {
        e.resize(1);
        from_json(json, e[0]);
    }
}

auto DownloadApp(ProgressBox* pbox, const GhApiAsset& gh_asset, const AssetEntry* entry) -> Result {
    static const fs::FsPath temp_file{"/switch/sphaira/cache/github/ghdl.temp"};

    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());
    ON_SCOPE_EXIT(fs.DeleteFile(temp_file));

    R_UNLESS(!gh_asset.browser_download_url.empty(), Result_GhdlEmptyAsset);

    // 2. download the asset
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer(i18n::Reorder("Downloading ", gh_asset.name));
        log_write("starting download: %s\n", gh_asset.browser_download_url.c_str());

        const auto result = curl::Api().ToFile(
            curl::Url{gh_asset.browser_download_url},
            curl::Path{temp_file},
            curl::OnProgress{pbox->OnDownloadProgressCallback()}
        );

        R_UNLESS(result.success, Result_GhdlFailedToDownloadAsset);
    }

    fs::FsPath root_path{"/"};
    if (entry && !entry->path.empty()) {
        root_path = entry->path;
    }

    // 3. extract the zip / file
    if (gh_asset.content_type.find("zip") != gh_asset.content_type.npos) {
        log_write("found zip\n");
        R_TRY(thread::TransferUnzipAll(pbox, temp_file, &fs, root_path));
    } else {
        fs.CreateDirectoryRecursivelyWithPath(root_path);
        fs.DeleteFile(root_path);
        R_TRY(fs.RenameFile(temp_file, root_path));
    }

    log_write("success\n");
    R_SUCCEED();
}

auto DownloadReleaseJsonJson(ProgressBox* pbox, const std::string& url, std::vector<GhApiEntry>& out) -> Result {
    // 1. download the json
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Downloading json"_i18n);
        log_write("starting download\n");

        const auto path = apiBuildAssetCache(url);

        const auto result = curl::Api().ToFile(
            curl::Url{url},
            curl::Path{path},
            curl::OnProgress{pbox->OnDownloadProgressCallback()},
            curl::Flags{curl::Flag_Cache},
            curl::Header{
                { "Accept", "application/vnd.github+json" },
            }
        );

        R_UNLESS(result.success, Result_GhdlFailedToDownloadAssetJson);
        from_json(result.path, out);
    }

    R_UNLESS(!out.empty(), Result_GhdlEmptyAsset);
    R_SUCCEED();
}

constexpr s64 MAX_DIRECT_LINK_SIZE = 20 * 1024 * 1024; // 20MB soft limit
constexpr fs::FsPath DIRECT_LINK_TEMP{"/switch/sphaira/cache/github/direct_link.zip"};
constexpr fs::FsPath DIRECT_LINKS_JSON{"/config/sphaira/github/direct_links.json"};

void SaveUrlToHistory(const std::string& url) {
    fs::FsNativeSd fs;
    fs.CreateDirectoryRecursively("/config/sphaira/github");

    // Read existing entries from direct_links.json
    std::vector<std::string> urls;
    auto json_in = yyjson_read_file(DIRECT_LINKS_JSON, YYJSON_READ_NOFLAG, nullptr, nullptr);
    if (json_in) {
        auto root = yyjson_doc_get_root(json_in);
        if (yyjson_is_arr(root)) {
            size_t idx, max;
            yyjson_val* val;
            yyjson_arr_foreach(root, idx, max, val) {
                if (yyjson_is_obj(val)) {
                    auto direct_url_val = yyjson_obj_get(val, "direct_url");
                    if (direct_url_val && yyjson_is_str(direct_url_val)) {
                        urls.emplace_back(yyjson_get_str(direct_url_val));
                    }
                }
            }
        }
        yyjson_doc_free(json_in);
    }

    // Check if URL already exists
    for (const auto& existing : urls) {
        if (existing == url) {
            App::Notify("URL already saved"_i18n);
            return;
        }
    }

    // Add new URL
    urls.emplace_back(url);

    // Write updated array to direct_links.json
    auto json_out = yyjson_mut_doc_new(nullptr);
    auto arr = yyjson_mut_arr(json_out);
    yyjson_mut_doc_set_root(json_out, arr);

    for (const auto& u : urls) {
        auto obj = yyjson_mut_arr_add_obj(json_out, arr);
        yyjson_mut_obj_add_str(json_out, obj, "direct_url", u.c_str());
    }

    if (yyjson_mut_write_file(DIRECT_LINKS_JSON, json_out, YYJSON_WRITE_PRETTY, nullptr, nullptr)) {
        App::Notify("URL saved!"_i18n);
    }

    yyjson_mut_doc_free(json_out);
}

void AskToSaveUrl(const std::string& url) {
    App::Push<OptionBox>(
        "Save URL to history?"_i18n,
        "No"_i18n, "Yes"_i18n, 1, [url](auto op_index){
            if (op_index && *op_index) {
                SaveUrlToHistory(url);
            }
        }
    );
}

void DoDirectLinkDownload(const std::string& url) {
    App::Push<ProgressBox>(0, "Downloading..."_i18n, "", [url](auto pbox) -> Result {
        fs::FsNativeSd fs;
        R_TRY(fs.GetFsOpenResult());

        // Download the file
        pbox->NewTransfer("Downloading..."_i18n);
        const auto result = curl::Api().ToFile(
            curl::Url{url},
            curl::Path{DIRECT_LINK_TEMP},
            curl::OnProgress{pbox->OnDownloadProgressCallback()}
        );
        R_UNLESS(result.success, Result_GhdlFailedToDownloadAsset);

        // Extract the ZIP
        pbox->NewTransfer("Extracting..."_i18n);
        R_TRY(thread::TransferUnzipAll(pbox, DIRECT_LINK_TEMP, &fs, "/"));

        R_SUCCEED();
    }, [url](Result rc){
        App::PushErrorBox(rc, "Download failed!"_i18n);

        if (R_SUCCEEDED(rc)) {
            homebrew::SignalChange();

            // Ask whether to delete the ZIP
            App::Push<OptionBox>(
                "Download and extract completed!\nDelete ZIP file?"_i18n,
                "Keep"_i18n, "Delete"_i18n, 1, [url](auto op_index){
                    if (op_index && *op_index) {
                        fs::FsNativeSd fs;
                        fs.DeleteFile(DIRECT_LINK_TEMP);
                    }

                    // Ask to save URL to history
                    AskToSaveUrl(url);
                }
            );
        }
    });
}

void DownloadDirectLink() {
    std::string url;
    if (R_FAILED(swkbd::ShowText(url, "Enter ZIP URL", "https://", "https://")) || url.empty()) {
        return;
    }

    // Validate URL ends with .zip
    if (url.size() < 4 || strcasecmp(url.c_str() + url.size() - 4, ".zip") != 0) {
        App::Push<OptionBox>("URL must end with .zip"_i18n, "OK"_i18n);
        return;
    }

    // Check file size via HEAD request
    const auto head_result = curl::Api().ToMemory(
        curl::Url{url},
        curl::Flags{curl::Flag_NoBody}
    );

    if (head_result.success) {
        auto it = head_result.header.Find("content-length");
        if (it != head_result.header.m_map.end()) {
            s64 size = std::atoll(it->second.c_str());
            if (size > MAX_DIRECT_LINK_SIZE) {
                // File is larger than 20MB - warn user
                char msg[256];
                std::snprintf(msg, sizeof(msg),
                    "File is %.1f MB (limit: 20 MB)\nLarge files may cause issues.\nForce download?",
                    (double)size / (1024.0 * 1024.0));

                App::Push<OptionBox>(msg, "Cancel"_i18n, "Force"_i18n, 0, [url](auto op_index){
                    if (op_index && *op_index) {
                        DoDirectLinkDownload(url);
                    }
                });
                return;
            }
        }
    }

    // Size OK or unknown - proceed with download
    DoDirectLinkDownload(url);
}

} // namespace

Menu::Menu(u32 flags) : MenuBase{"GitHub"_i18n, flags} {
    fs::FsNativeSd().CreateDirectoryRecursively(CACHE_PATH);

    this->SetActions(
        std::make_pair(Button::A, Action{"Download"_i18n, [this](){
            if (m_entries.empty()) {
                return;
            }

            DownloadEntries(GetEntry());
        }}),

        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }}),

        std::make_pair(Button::Y, Action{"Direct Link"_i18n, [this](){
            DownloadDirectLink();
        }})
    );

    const Vec4 v{75, GetY() + 1.f + 42.f, 1220.f-45.f*2, 60};
    m_list = std::make_unique<List>(1, 8, m_pos, v);
}

Menu::~Menu() {
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_index, m_entries.size(), [this](bool touch, auto i) {
        if (touch && m_index == i) {
            FireAction(Button::A);
        } else {
            App::PlaySoundEffect(SoundEffect::Focus);
            SetIndex(i);
        }
    });
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    if (m_entries.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Empty..."_i18n.c_str());
        return;
    }

    constexpr float text_xoffset{15.f};

    m_list->Draw(vg, theme, m_entries.size(), [this](auto* vg, auto* theme, auto& v, auto i) {
        const auto& [x, y, w, h] = v;
        auto& e = m_entries[i];

        auto text_id = ThemeEntryID_TEXT;
        if (m_index == i) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, theme, 4.f, v);
        } else {
            if (i != m_entries.size() - 1) {
                gfx::drawRect(vg, x, y + h, w, 1.f, theme->GetColour(ThemeEntryID_LINE_SEPARATOR));
            }
        }

        nvgSave(vg);
        nvgIntersectScissor(vg, x + text_xoffset, y, w-(x+text_xoffset+50), h);
            gfx::drawTextArgs(vg, x + text_xoffset, y + (h / 2.f), 20.f, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(text_id), "%s By %s", e.repo.c_str(), e.owner.c_str());
        nvgRestore(vg);

        if (!e.tag.empty()) {
            gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f), 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "version: %s"_i18n.c_str(), e.tag.c_str());
        }
    });
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
    if (m_entries.empty()) {
        Scan();
    }
}

void Menu::SetIndex(s64 index) {
    m_index = index;
    if (!m_index) {
        m_list->SetYoff(0);
    }

    SetTitleSubHeading(m_entries[m_index].json_path);
    UpdateSubheading();
}

void Menu::LoadDirectLinksJson() {
    auto json_in = yyjson_read_file(DIRECT_LINKS_JSON, YYJSON_READ_NOFLAG, nullptr, nullptr);
    if (!json_in) {
        return;
    }
    ON_SCOPE_EXIT(yyjson_doc_free(json_in));

    auto root = yyjson_doc_get_root(json_in);
    if (!yyjson_is_arr(root)) {
        return;
    }

    size_t idx, max;
    yyjson_val* val;
    yyjson_arr_foreach(root, idx, max, val) {
        if (!yyjson_is_obj(val)) {
            continue;
        }

        auto direct_url_val = yyjson_obj_get(val, "direct_url");
        if (!direct_url_val || !yyjson_is_str(direct_url_val)) {
            continue;
        }

        Entry entry{};
        entry.direct_url = yyjson_get_str(direct_url_val);

        // Extract filename from URL for display
        auto pos = entry.direct_url.rfind('/');
        if (pos != std::string::npos && pos + 1 < entry.direct_url.size()) {
            entry.repo = entry.direct_url.substr(pos + 1);
            // Remove .zip extension for cleaner display
            if (entry.repo.size() > 4) {
                auto ext_pos = entry.repo.rfind(".zip");
                if (ext_pos != std::string::npos && ext_pos == entry.repo.size() - 4) {
                    entry.repo = entry.repo.substr(0, ext_pos);
                }
            }
        } else {
            entry.repo = "Direct Link";
        }
        entry.owner = "Direct";
        entry.json_path = DIRECT_LINKS_JSON;

        m_entries.emplace_back(entry);
    }
}

void Menu::Scan() {
    m_entries.clear();

    // load from romfs first
    if (R_SUCCEEDED(romfsInit())) {
        LoadEntriesFromPath("romfs:/github/");
        romfsExit();
    }

    // then load custom entries
    LoadEntriesFromPath("/config/sphaira/github/");

    // load saved direct links from array file
    LoadDirectLinksJson();

    Sort();
    SetIndex(0);
}

void Menu::LoadEntriesFromPath(const fs::FsPath& path) {
    auto dir = opendir(path);
    if (!dir) {
        return;
    }
    ON_SCOPE_EXIT(closedir(dir));

    while (auto d = readdir(dir)) {
        if (d->d_name[0] == '.') {
            continue;
        }

        if (d->d_type != DT_REG) {
            continue;
        }

        const auto ext = std::strrchr(d->d_name, '.');
        if (!ext || strcasecmp(ext, ".json")) {
            continue;
        }

        Entry entry{};
        const auto full_path = fs::AppendPath(path, d->d_name);
        from_json(full_path, entry);

        // parse owner and author from url (if needed).
        if (!entry.url.empty()) {
            const auto s = entry.url.substr(std::strlen("https://github.com/"));
            const auto it = s.find('/');
            if (it != s.npos) {
                entry.owner = s.substr(0, it);
                entry.repo = s.substr(it + 1);
            }
        }

        // check that we have a owner and repo, OR a direct_url
        if ((entry.owner.empty() || entry.repo.empty()) && entry.direct_url.empty()) {
            continue;
        }

        // For direct_url entries without owner/repo, use filename as display name
        if (!entry.direct_url.empty() && entry.repo.empty()) {
            // Extract filename from URL for display
            auto pos = entry.direct_url.rfind('/');
            if (pos != std::string::npos && pos + 1 < entry.direct_url.size()) {
                entry.repo = entry.direct_url.substr(pos + 1);
                // Remove .zip extension for cleaner display
                if (entry.repo.size() > 4) {
                    auto ext_pos = entry.repo.rfind(".zip");
                    if (ext_pos != std::string::npos && ext_pos == entry.repo.size() - 4) {
                        entry.repo = entry.repo.substr(0, ext_pos);
                    }
                }
            } else {
                entry.repo = "Direct Link";
            }
            entry.owner = "Direct";
        }

        entry.json_path = full_path;
        m_entries.emplace_back(entry);
    }
}

void Menu::Sort() {
    const auto sorter = [this](Entry& lhs, Entry& rhs) -> bool {
        // handle fallback if multiple entries are added with the same name
        // used for forks of a project.
        // in the rare case of the user adding the same owner and repo,
        // fallback to the filepath, which *is* unqiue
        auto r = strcasecmp(lhs.repo.c_str(), rhs.repo.c_str());
        if (!r) {
            r = strcasecmp(lhs.owner.c_str(), rhs.owner.c_str());
            if (!r) {
                r = strcasecmp(lhs.json_path, rhs.json_path);
            }
        }
        return r < 0;
    };

    std::sort(m_entries.begin(), m_entries.end(), sorter);
}

void Menu::UpdateSubheading() {
    const auto index = m_entries.empty() ? 0 : m_index + 1;
    this->SetSubHeading(std::to_string(index) + " / " + std::to_string(m_entries.size()));
}

void DownloadEntries(const Entry& entry) {
    // Handle direct URL entries differently - skip GitHub API
    if (!entry.direct_url.empty()) {
        DoDirectLinkDownload(entry.direct_url);
        return;
    }

    // hack
    static std::vector<GhApiEntry> gh_entries;
    gh_entries = {};

    App::Push<ProgressBox>(0, "Downloading "_i18n, entry.repo, [entry](auto pbox) -> Result {
        return DownloadReleaseJsonJson(pbox, GenerateApiUrl(entry), gh_entries);
    }, [entry](Result rc){
        App::PushErrorBox(rc, "Failed to download json"_i18n);
        if (R_FAILED(rc) || gh_entries.empty()) {
            return;
        }

        PopupList::Items entry_items;
        for (const auto& e : gh_entries) {
            std::string str = " [" + e.published_at.substr(0, 10) + "]";

            if (!e.name.empty()) {
                str += " " + e.name;
            } else {
                str += " " + e.tag_name;
            }
            if (e.prerelease) {
                str += " (Pre-Release)";
            }

            entry_items.emplace_back(str);
        }

        App::Push<PopupList>("Select release to download for "_i18n + entry.repo, entry_items, [entry](auto op_index){
            if (!op_index) {
                return;
            }

            const auto& gh_entry = gh_entries[*op_index];
            const auto& assets = entry.assets;
            PopupList::Items asset_items;
            std::vector<const AssetEntry*> asset_ptr;
            std::vector<GhApiAsset> api_assets;
            bool using_name = false;

            for (auto&p : gh_entry.assets) {
                bool found = false;

                for (auto& e : assets) {
                    if (!e.name.empty()) {
                        using_name = true;
                    }

                    if (p.name.find(e.name) != p.name.npos) {
                        found = true;
                        asset_ptr.emplace_back(&e);
                        break;
                    }
                }

                if (!using_name || found) {
                    std::string str = " [" + p.updated_at.substr(0, 10) + "]" + " " + p.name;

                    asset_items.emplace_back(str);
                    api_assets.emplace_back(p);
                }
            }

            App::Push<PopupList>("Select asset to download for "_i18n + entry.repo, asset_items, [entry, api_assets, asset_ptr](auto op_index){
                if (!op_index) {
                    return;
                }

                const auto index = *op_index;
                const auto& asset_entry = api_assets[index];
                const AssetEntry* ptr{};
                auto pre_install_message = entry.pre_install_message;
                if (asset_ptr.size()) {
                    ptr = asset_ptr[index];
                    if (!ptr->pre_install_message.empty()) {
                        pre_install_message = ptr->pre_install_message;
                    }
                }

                const auto func = [entry, &asset_entry, ptr](){
                    App::Push<ProgressBox>(0, "Downloading "_i18n, entry.repo, [entry, &asset_entry, ptr](auto pbox) -> Result {
                        return DownloadApp(pbox, asset_entry, ptr);
                    }, [entry, ptr](Result rc){
                        homebrew::SignalChange();
                        App::PushErrorBox(rc, "Failed to download app!"_i18n);

                        if (R_SUCCEEDED(rc)) {
                            App::Notify(i18n::Reorder("Downloaded ", entry.repo));
                            auto post_install_message = entry.post_install_message;
                            if (ptr && !ptr->post_install_message.empty()) {
                                post_install_message = ptr->post_install_message;
                            }

                            if (!post_install_message.empty()) {
                                App::Push<OptionBox>(post_install_message, "OK"_i18n);
                            }
                        }
                    });
                };

                if (!pre_install_message.empty()) {
                    App::Push<OptionBox>(
                        pre_install_message,
                        "Back"_i18n, "Download"_i18n, 1, [entry, func](auto op_index){
                            if (op_index && *op_index) {
                                func();
                            }
                        }
                    );
                } else {
                    func();
                }
            });
        });
    });
}

bool Download(const std::string& url, const std::vector<AssetEntry>& assets, const std::string& tag, const std::string& pre_install_message, const std::string& post_install_message) {
    Entry entry{};
    entry.url = url;
    entry.tag = tag;
    entry.assets = assets;
    entry.pre_install_message = pre_install_message;
    entry.post_install_message = post_install_message;

    // parse owner and author from url (if needed).
    if (!entry.url.empty()) {
        const auto s = entry.url.substr(std::strlen("https://github.com/"));
        const auto it = s.find('/');
        if (it != s.npos) {
            entry.owner = s.substr(0, it);
            entry.repo = s.substr(it + 1);
        }
    }

    // check that we have a owner and repo
    if (entry.owner.empty() || entry.repo.empty()) {
        return false;
    }

    DownloadEntries(entry);
    return true;
}

} // namespace sphaira::ui::menu::gh
