#include "ui/menus/ghdl.hpp"
#include "ui/sidebar.hpp"
#include "ui/option_box.hpp"
#include "ui/popup_list.hpp"
#include "ui/progress_box.hpp"
#include "ui/error_box.hpp"

#include "log.hpp"
#include "app.hpp"
#include "ui/nvg_util.hpp"
#include "fs.hpp"
#include "defines.hpp"
#include "image.hpp"
#include "download.hpp"
#include "i18n.hpp"
#include "yyjson_helper.hpp"

#include <minIni.h>
#include <minizip/unzip.h>
#include <dirent.h>
#include <cstring>
#include <string>

namespace sphaira::ui::menu::gh {
namespace {

constexpr auto CACHE_PATH = "/switch/sphaira/cache/github";

auto GenerateApiUrl(const Entry& e) {
    return "https://api.github.com/repos/" + e.owner + "/" + e.repo + "/releases/latest";
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
    );
}

void from_json(const fs::FsPath& path, Entry& e) {
    JSON_INIT_VEC_FILE(path, nullptr, nullptr);
    JSON_OBJ_ITR(
        JSON_SET_STR(owner);
        JSON_SET_STR(repo);
        JSON_SET_ARR_OBJ(assets);
    );
}

void from_json(yyjson_val* json, GhApiAsset& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(name);
        JSON_SET_STR(content_type);
        JSON_SET_UINT(size);
        JSON_SET_UINT(download_count);
        JSON_SET_STR(browser_download_url);
    );
}

void from_json(const fs::FsPath& path, GhApiEntry& e) {
    JSON_INIT_VEC_FILE(path, nullptr, nullptr);
    JSON_OBJ_ITR(
        JSON_SET_STR(tag_name);
        JSON_SET_STR(name);
        JSON_SET_ARR_OBJ(assets);
    );
}

auto DownloadApp(ProgressBox* pbox, const GhApiAsset& gh_asset, const AssetEntry* entry) -> bool {
    static const fs::FsPath temp_file{"/switch/sphaira/cache/github/ghdl.temp"};
    constexpr auto chunk_size = 1024 * 512; // 512KiB

    fs::FsNativeSd fs;
    R_TRY_RESULT(fs.GetFsOpenResult(), false);
    ON_SCOPE_EXIT(fs.DeleteFile(temp_file));

    if (gh_asset.browser_download_url.empty()) {
        log_write("failed to find asset\n");
        return false;
    }

    // 2. download the asset
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Downloading "_i18n + gh_asset.name);
        log_write("starting download: %s\n", gh_asset.browser_download_url.c_str());

        if (!curl::Api().ToFile(
            curl::Url{gh_asset.browser_download_url},
            curl::Path{temp_file},
            curl::OnProgress{pbox->OnDownloadProgressCallback()}
        ).success){
            log_write("error with download\n");
            return false;
        }
    }

    fs::FsPath root_path{"/"};
    if (entry && !entry->path.empty()) {
        root_path = entry->path;
    }

    // 3. extract the zip / file
    if (gh_asset.content_type == "application/zip") {
        log_write("found zip\n");
        auto zfile = unzOpen64(temp_file);
        if (!zfile) {
            log_write("failed to open zip: %s\n", temp_file);
            return false;
        }
        ON_SCOPE_EXIT(unzClose(zfile));

        unz_global_info64 pglobal_info;
        if (UNZ_OK != unzGetGlobalInfo64(zfile, &pglobal_info)) {
            return false;
        }

        for (int i = 0; i < pglobal_info.number_entry; i++) {
            if (i > 0) {
                if (UNZ_OK != unzGoToNextFile(zfile)) {
                    log_write("failed to unzGoToNextFile\n");
                    return false;
                }
            }

            if (UNZ_OK != unzOpenCurrentFile(zfile)) {
                log_write("failed to open current file\n");
                return false;
            }
            ON_SCOPE_EXIT(unzCloseCurrentFile(zfile));

            unz_file_info64 info;
            fs::FsPath file_path;
            if (UNZ_OK != unzGetCurrentFileInfo64(zfile, &info, file_path, sizeof(file_path), 0, 0, 0, 0)) {
                log_write("failed to get current info\n");
                return false;
            }

            file_path = fs::AppendPath(root_path, file_path);

            Result rc;
            if (file_path[strlen(file_path) -1] == '/') {
                if (R_FAILED(rc = fs.CreateDirectoryRecursively(file_path, true)) && rc != FsError_PathAlreadyExists) {
                    log_write("failed to create folder: %s 0x%04X\n", file_path, rc);
                    return false;
                }
            } else {
                Result rc;
                if (R_FAILED(rc = fs.CreateFile(file_path, info.uncompressed_size, 0, true)) && rc != FsError_PathAlreadyExists) {
                    log_write("failed to create file: %s 0x%04X\n", file_path, rc);
                    return false;
                }

                FsFile f;
                if (R_FAILED(rc = fs.OpenFile(file_path, FsOpenMode_Write, &f))) {
                    log_write("failed to open file: %s 0x%04X\n", file_path, rc);
                    return false;
                }
                ON_SCOPE_EXIT(fsFileClose(&f));

                if (R_FAILED(rc = fsFileSetSize(&f, info.uncompressed_size))) {
                    log_write("failed to set file size: %s 0x%04X\n", file_path, rc);
                    return false;
                }

                std::vector<char> buf(chunk_size);
                u64 offset{};
                while (offset < info.uncompressed_size) {
                    const auto bytes_read = unzReadCurrentFile(zfile, buf.data(), buf.size());
                    if (bytes_read <= 0) {
                        log_write("failed to read zip file: %s\n", file_path.s);
                        return false;
                    }

                    if (R_FAILED(rc = fsFileWrite(&f, offset, buf.data(), bytes_read, FsWriteOption_None))) {
                        log_write("failed to write file: %s 0x%04X\n", file_path.s, rc);
                        return false;
                    }

                    pbox->UpdateTransfer(offset, info.uncompressed_size);
                    offset += bytes_read;
                }
            }
        }
    } else {
        fs.CreateDirectoryRecursivelyWithPath(root_path, true);
        fs.DeleteFile(root_path);
        if (R_FAILED(fs.RenameFile(temp_file, root_path, true))) {
            log_write("failed to rename file: %s -> %s\n", temp_file.s, root_path.s);
        }
    }

    log_write("success\n");
    return true;
}

auto DownloadAssetJson(ProgressBox* pbox, const std::string& url, GhApiEntry& out) -> bool {
    // 1. download the json
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Downloading json"_i18n);
        log_write("starting download\n");

        const auto path = apiBuildAssetCache(url);

        const auto result = curl::Api().ToFile(
            curl::Url{url},
            curl::Path{path},
            curl::OnProgress{pbox->OnDownloadProgressCallback()},
            curl::Header{
                { "Accept", "application/vnd.github+json" },
                { "if-none-match", curl::cache::etag_get(path) },
                { "if-modified-since", curl::cache::lmt_get(path) },
            }
        );

        if (!result.success) {
            log_write("json empty\n");
            return false;
        }

        curl::cache::etag_set(result.path, result.header);
        curl::cache::lmt_set(result.path, result.header);
        from_json(result.path, out);
    }

    return !out.assets.empty();
}

} // namespace

Menu::Menu() : MenuBase{"GitHub"_i18n} {
    fs::FsNativeSd().CreateDirectoryRecursively(CACHE_PATH);

    this->SetActions(
        std::make_pair(Button::R2, Action{[this](){
        }}),

        std::make_pair(Button::DOWN, Action{[this](){
            if (m_index < (m_entries.size() - 1)) {
                SetIndex(m_index + 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                if (m_index - m_index_offset >= 8) {
                    log_write("moved down\n");
                    m_index_offset++;
                }
            }
        }}),

        std::make_pair(Button::UP, Action{[this](){
            if (m_index != 0) {
                SetIndex(m_index - 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                if (m_index < m_index_offset ) {
                    log_write("moved up\n");
                    m_index_offset--;
                }
            }
        }}),

        std::make_pair(Button::A, Action{"Download"_i18n, [this](){
            if (m_entries.empty()) {
                return;
            }

            // hack
            static GhApiEntry gh_entry;
            gh_entry = {};

            App::Push(std::make_shared<ProgressBox>("Downloading "_i18n + GetEntry().repo, [this](auto pbox){
                return DownloadAssetJson(pbox, GenerateApiUrl(GetEntry()), gh_entry);
            }, [this](bool success){
                if (success) {
                    const auto& assets = GetEntry().assets;
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
                            asset_items.emplace_back(p.name);
                            api_assets.emplace_back(p);
                        }
                    }

                    App::Push(std::make_shared<PopupList>("Select asset to download for "_i18n + GetEntry().repo, asset_items, [this, api_assets, asset_ptr](auto op_index){
                        if (!op_index) {
                            return;
                        }

                        const auto index = *op_index;
                        const auto& asset_entry = api_assets[index];
                        const AssetEntry* ptr{};
                        if (asset_ptr.size()) {
                            ptr = asset_ptr[index];
                        }

                        App::Push(std::make_shared<ProgressBox>("Downloading "_i18n + GetEntry().repo, [this, &asset_entry, ptr](auto pbox){
                            return DownloadApp(pbox, asset_entry, ptr);
                        }, [this](bool success){
                            if (success) {
                                App::Notify("Downloaded "_i18n + GetEntry().name);
                            }
                        }, 2));
                    }));
                }
            }, 2));
        }}),

        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }})
    );
}

Menu::~Menu() {
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    const auto& text_col = theme->elements[ThemeEntryID_TEXT].colour;

    if (m_entries.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, text_col, "Empty..."_i18n.c_str());
        return;
    }

    const u64 SCROLL = m_index_offset;
    constexpr u64 max_entry_display = 8;
    const u64 entry_total = m_entries.size();

    // only draw scrollbar if needed
    if (entry_total > max_entry_display) {
        const auto scrollbar_size = 500.f;
        const auto sb_h = 1.f / (float)entry_total * scrollbar_size;
        const auto sb_y = SCROLL;
        gfx::drawRect(vg, SCREEN_WIDTH - 50, 100, 10, scrollbar_size, gfx::getColour(gfx::Colour::BLACK));
        gfx::drawRect(vg, SCREEN_WIDTH - 50+2, 102 + sb_h * sb_y, 10-4, sb_h + (sb_h * (max_entry_display - 1)) - 4, gfx::getColour(gfx::Colour::SILVER));
    }

    // constexpr Vec4 line_top{30.f, 86.f, 1220.f, 1.f};
    // constexpr Vec4 line_bottom{30.f, 646.f, 1220.f, 1.f};
    // constexpr Vec4 block{280.f, 110.f, 720.f, 60.f};
    constexpr Vec4 block{75.f, 110.f, 1220.f-45.f*2, 60.f};
    constexpr float text_xoffset{15.f};

    // todo: cleanup
    const float x = block.x;
    float y = GetY() + 1.f + 42.f;
    const float h = block.h;
    const float w = block.w;

    nvgSave(vg);
    nvgScissor(vg, GetX(), GetY(), GetW(), GetH());

    for (std::size_t i = m_index_offset; i < m_entries.size(); i++) {
        auto& e = m_entries[i];

        auto text_id = ThemeEntryID_TEXT;
        if (m_index == i) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, 4.f, theme->elements[ThemeEntryID_SELECTED_OVERLAY].colour, x, y, w, h, theme->elements[ThemeEntryID_SELECTED].colour);
        } else {
            if (i == m_index_offset) {
                gfx::drawRect(vg, x, y, w, 1.f, text_col);
            }
            gfx::drawRect(vg, x, y + h, w, 1.f, text_col);
        }

        nvgSave(vg);
        const auto txt_clip = std::min(GetY() + GetH(), y + h) - y;
        nvgScissor(vg, x + text_xoffset, y, w-(x+text_xoffset+50), txt_clip);
            gfx::drawTextArgs(vg, x + text_xoffset, y + (h / 2.f), 20.f, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->elements[text_id].colour, "%s By %s", e.repo.c_str(), e.owner.c_str());
        nvgRestore(vg);

        gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f), 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_MIDDLE, theme->elements[text_id].colour, GenerateApiUrl(e).c_str());

        y += h;
        if (!InYBounds(y)) {
            break;
        }
    }

    nvgRestore(vg);
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
    if (m_entries.empty()) {
        Scan();
    }
}

void Menu::SetIndex(std::size_t index) {
    m_index = index;
    if (!m_index) {
        m_index_offset = 0;
    }

    SetTitleSubHeading(m_entries[m_index].json_path);
    UpdateSubheading();
}

void Menu::Scan() {
    m_entries.clear();
    m_index = 0;
    m_index_offset = 0;

    // load from romfs first
    if (R_SUCCEEDED(romfsInit())) {
        LoadEntriesFromPath("romfs:/github/");
        romfsExit();
    }

    // then load custom entries
    LoadEntriesFromPath("/config/sphaira/github/");
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

        // check that we have a owner and repo
        if (entry.owner.empty() || entry.repo.empty()) {
            continue;
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

} // namespace sphaira::ui::menu::gh
