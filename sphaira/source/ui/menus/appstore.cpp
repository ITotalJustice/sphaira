#include "ui/menus/appstore.hpp"
#include "ui/sidebar.hpp"
#include "ui/popup_list.hpp"
#include "ui/progress_box.hpp"
#include "ui/option_box.hpp"

#include "download.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "app.hpp"
#include "ui/nvg_util.hpp"
#include "fs.hpp"
#include "yyjson_helper.hpp"
#include "swkbd.hpp"
#include "i18n.hpp"
#include "nro.hpp"

#include <minIni.h>
#include <string>
#include <cstring>
#include <yyjson.h>
#include <stb_image.h>
#include <minizip/unzip.h>
#include <mbedtls/md5.h>
#include <ranges>
#include <utility>

namespace sphaira::ui::menu::appstore {
namespace {

constexpr fs::FsPath REPO_PATH{"/switch/sphaira/cache/appstore/repo.json"};
constexpr fs::FsPath CACHE_PATH{"/switch/sphaira/cache/appstore"};
constexpr auto URL_BASE = "https://switch.cdn.fortheusers.org";
constexpr auto URL_JSON = "https://switch.cdn.fortheusers.org/repo.json";
constexpr auto URL_POST_FEEDBACK = "http://switchbru.com/appstore/feedback";
constexpr auto URL_GET_FEEDACK = "http://switchbru.com/appstore/feedback";

constexpr const char* FILTER_STR[] = {
    "All",
    "Games",
    "Emulators",
    "Tools",
    "Advanced",
    "Themes",
    "Legacy",
    "Misc",
};

constexpr const char* SORT_STR[] = {
    "Updated",
    "Downloads",
    "Size",
    "Alphabetical",
};

constexpr const char* ORDER_STR[] = {
    "Desc",
    "Asc",
};

auto BuildIconUrl(const Entry& e) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/packages/%s/icon.png", URL_BASE, e.name.c_str());
    return out;
}

auto BuildBannerUrl(const Entry& e) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/packages/%s/screen.png", URL_BASE, e.name.c_str());
    return out;
}

auto BuildZipUrl(const Entry& e) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/zips/%s.zip", URL_BASE, e.name.c_str());
    return out;
}

auto BuildIconCachePath(const Entry& e) -> fs::FsPath {
    fs::FsPath out;
    std::snprintf(out, sizeof(out), "%s/icons/%s.png", CACHE_PATH.s, e.name.c_str());
    return out;
}

auto BuildBannerCachePath(const Entry& e) -> fs::FsPath {
    fs::FsPath out;
    std::snprintf(out, sizeof(out), "%s/banners/%s.png", CACHE_PATH.s, e.name.c_str());
    return out;
}

#if 0
auto BuildScreensCachePath(const Entry& e, u8 num) -> fs::FsPath {
    fs::FsPath out;
    std::snprintf(out, sizeof(out), "%s/screens/%s%u.png", CACHE_PATH, e.name.c_str(), num+1);
    return out;
}
#endif

// use appstore path in order to maintain compat with appstore
auto BuildPackageCachePath(const Entry& e) -> fs::FsPath {
    return "/switch/appstore/.get/packages/" + e.name;
}

auto BuildInfoCachePath(const Entry& e) -> fs::FsPath {
    return BuildPackageCachePath(e) + "/info.json";
}

auto BuildManifestCachePath(const Entry& e) -> fs::FsPath {
    return BuildPackageCachePath(e) + "/manifest.install";
}

auto BuildFeedbackCachePath(const Entry& e) -> fs::FsPath {
    return BuildPackageCachePath(e) + "/feedback.json";
}

void from_json(yyjson_val* json, Entry& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(category);
        JSON_SET_STR(binary);
        JSON_SET_STR(updated);
        JSON_SET_STR(name);
        JSON_SET_STR(license);
        JSON_SET_STR(title);
        JSON_SET_STR(url);
        JSON_SET_STR(description);
        JSON_SET_STR(author);
        JSON_SET_STR(changelog);
        JSON_SET_UINT(screens);
        JSON_SET_UINT(extracted);
        JSON_SET_STR(version);
        JSON_SET_UINT(filesize);
        JSON_SET_STR(details);
        JSON_SET_UINT(app_dls);
        JSON_SET_STR(md5);
    );
}

void from_json(const fs::FsPath& path, std::vector<appstore::Entry>& e) {
    yyjson_read_err err;
    JSON_INIT_VEC_FILE(path, nullptr, &err);
    JSON_OBJ_ITR(
        JSON_SET_ARR_OBJ2(packages, e);
    );
}

auto ParseManifest(std::span<const char> view) -> ManifestEntries {
    ManifestEntries entries;
    // auto view = std::string_view{manifest_data.data(), manifest_data.size()};

    for (const auto line : std::views::split(view, '\n')) {
        if (line.size() <= 3) {
            continue;
        }

        ManifestEntry entry{};
        entry.command = line[0];
        std::strncpy(entry.path, line.data() + 3, line.size() - 3);
        entries.emplace_back(entry);
    }

    return entries;
}

auto LoadAndParseManifest(const Entry& e) -> ManifestEntries {
    const auto path = BuildManifestCachePath(e);

    std::vector<u8> data;
    if (R_FAILED(fs::FsNativeSd().read_entire_file(path, data))) {
        return {};
    }

    return ParseManifest(std::span{(const char*)data.data(), data.size()});
}

auto EntryLoadImageFile(fs::Fs& fs, const fs::FsPath& path, LazyImage& image) -> bool {
    // already have the image
    if (image.image) {
        // log_write("warning, tried to load image: %s when already loaded\n", path);
        return true;
    }
    auto vg = App::GetVg();

    std::vector<u8> image_buf;
    if (R_FAILED(fs.read_entire_file(path, image_buf))) {
        log_write("failed to load image from file: %s\n", path.s);
    } else {
        int channels_in_file;
        auto buf = stbi_load_from_memory(image_buf.data(), image_buf.size(), &image.w, &image.h, &channels_in_file, 4);
        if (buf) {
            ON_SCOPE_EXIT(stbi_image_free(buf));
            std::memcpy(image.first_pixel, buf, sizeof(image.first_pixel));
            image.image = nvgCreateImageRGBA(vg, image.w, image.h, 0, buf);
        }
    }

    if (!image.image) {
        log_write("failed to load image from file: %s\n", path.s);
        return false;
    } else {
        // log_write("loaded image from file: %s\n", path);
        return true;
    }
}

auto EntryLoadImageFile(const fs::FsPath& path, LazyImage& image) -> bool {
    if (!strncasecmp("romfs:/", path, 7)) {
        fs::FsStdio fs;
        return EntryLoadImageFile(fs, path, image);
    } else {
        fs::FsNativeSd fs;
        return EntryLoadImageFile(fs, path, image);
    }
}

void DrawIcon(NVGcontext* vg, const LazyImage& l, const LazyImage& d, float x, float y, float w, float h, bool rounded = true, float scale = 1.0) {
    const auto& i = l.image ? l : d;

    const float iw = (float)i.w / scale;
    const float ih = (float)i.h / scale;
    float ix = x;
    float iy = y;
    bool rounded_image = rounded;

    if (w > iw) {
        ix = x + abs((w - iw) / 2);
    } else if (w < iw) {
        ix = x - abs((w - iw) / 2);
    }
    if (h > ih) {
        iy = y + abs((h - ih) / 2);
    } else if (h < ih) {
        iy = y - abs((h - ih) / 2);
    }

    bool crop = false;
    if (iw < w || ih < h) {
        rounded_image = false;
        gfx::drawRect(vg, x, y, w, h, nvgRGB(i.first_pixel[0], i.first_pixel[1], i.first_pixel[2]), rounded ? 5 : 0);
    }
    if (iw > w || ih > h) {
        crop = true;
        nvgSave(vg);
        nvgIntersectScissor(vg, x, y, w, h);
    }

    gfx::drawImage(vg, ix, iy, iw, ih, i.image, rounded_image ? 5 : 0);
    if (crop) {
        nvgRestore(vg);
    }
}

void DrawIcon(NVGcontext* vg, const LazyImage& l, const LazyImage& d, Vec4 vec, bool rounded = true, float scale = 1.0) {
    DrawIcon(vg, l, d, vec.x, vec.y, vec.w, vec.h, rounded, scale);
}

auto AppDlToStr(u32 value) -> std::string {
    auto str = std::to_string(value);
    u32 inc = 3;
    for (u32 i = inc; i < str.size(); i += inc) {
        str.insert(str.cend() - i , ',');
        inc++;
    }
    return str;
}

void ReadFromInfoJson(Entry& e) {
    const auto info_path = BuildInfoCachePath(e);

    yyjson_read_err err;
    auto doc = yyjson_read_file(info_path, YYJSON_READ_NOFLAG, nullptr, &err);
    if (doc) {
        const auto root = yyjson_doc_get_root(doc);
        const auto version = yyjson_obj_get(root, "version");
        if (version) {
            if (!std::strcmp(yyjson_get_str(version), e.version.c_str())) {
                e.status = EntryStatus::Installed;
            } else {
                e.status = EntryStatus::Update;
                log_write("info.json said %s needs update: %s vs %s\n", e.name.c_str(), yyjson_get_str(version), e.version.c_str());
            }
        }
        // log_write("got info for: %s\n", e.name.c_str());
        yyjson_doc_free(doc);
    }
}

// this ignores ShouldExit() as leaving somthing in a half
// deleted state is a bad idea :)
auto UninstallApp(ProgressBox* pbox, const Entry& entry) -> bool {
    const auto manifest = LoadAndParseManifest(entry);
    fs::FsNativeSd fs;

    if (manifest.empty()) {
        if (entry.binary.empty()) {
            return false;
        }
        fs.DeleteFile(entry.binary);
    } else {
        for (auto& e : manifest) {
            pbox->NewTransfer(e.path);

            const auto safe_buf = fs::AppendPath("/", e.path);
            // this will handle read only files, ie, hbmenu.nro
            if (R_FAILED(fs.DeleteFile(safe_buf))) {
                log_write("failed to delete file: %s\n", safe_buf.s);
            } else {
                log_write("deleted file: %s\n", safe_buf.s);
                // todo: delete empty directories!
                // fs::delete_directory(safe_buf);
            }
        }
    }

    // remove directory, this will also delete manifest and info
    const auto dir = BuildPackageCachePath(entry);
    pbox->NewTransfer("Removing "_i18n + dir);
    if (R_FAILED(fs.DeleteDirectoryRecursively(dir))) {
        log_write("failed to delete folder: %s\n", dir.s);
    } else {
        log_write("deleted: %s\n", dir.s);
    }
    return true;
}

// this is called by ProgressBox on a seperate thread
// it has 4 main steps
// 1. download the zip
// 2. md5 check the zip
// 3. parse manifest and unzip everything to placeholder
// 4. move everything from placeholder to normal location
auto InstallApp(ProgressBox* pbox, const Entry& entry) -> bool {
    static const fs::FsPath zip_out{"/switch/sphaira/cache/appstore/temp.zip"};
    constexpr auto chunk_size = 1024 * 512; // 512KiB

    fs::FsNativeSd fs;
    R_TRY_RESULT(fs.GetFsOpenResult(), false);

    // 1. download the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Downloading "_i18n + entry.title);
        log_write("starting download\n");

        const auto url = BuildZipUrl(entry);
        if (!curl::Api().ToFile(
            curl::Url{url},
            curl::Path{zip_out},
            curl::OnProgress{pbox->OnDownloadProgressCallback()}
        ).success) {
            log_write("error with download\n");
            return false;
        }
    }

    ON_SCOPE_EXIT(fs.DeleteFile(zip_out));

    // 2. md5 check the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Checking MD5"_i18n);
        log_write("starting md5 check\n");

        FsFile f;
        if (R_FAILED(fs.OpenFile(zip_out, FsOpenMode_Read, &f))) {
            return false;
        }
        ON_SCOPE_EXIT(fsFileClose(&f));

        s64 size;
        if (R_FAILED(fsFileGetSize(&f, &size))) {
            return false;
        }

        mbedtls_md5_context ctx;
        mbedtls_md5_init(&ctx);
        ON_SCOPE_EXIT(mbedtls_md5_free(&ctx));

        if (mbedtls_md5_starts_ret(&ctx)) {
            log_write("failed to start ret\n");
        }

        std::vector<u8> chunk(chunk_size);
        s64 offset{};
        while (offset < size) {
            if (pbox->ShouldExit()) {
                return false;
            }

            u64 bytes_read;
            if (R_FAILED(fsFileRead(&f, offset, chunk.data(), chunk.size(), 0, &bytes_read))) {
                log_write("failed to read file offset: %zd size: %zd\n", offset, size);
                return false;
            }

            if (mbedtls_md5_update_ret(&ctx, chunk.data(), bytes_read)) {
                log_write("failed to update ret\n");
                return false;
            }

            offset += bytes_read;
            pbox->UpdateTransfer(offset, size);
        }

        u8 md5_out[16];
        if (mbedtls_md5_finish_ret(&ctx, (u8*)md5_out)) {
            return false;
        }

        // convert md5 to hex string
        char md5_str[sizeof(md5_out) * 2 + 1];
        for (u32 i = 0; i < sizeof(md5_out); i++) {
            std::sprintf(md5_str + i * 2, "%02x", md5_out[i]);
        }

        if (strncasecmp(md5_str, entry.md5.data(), entry.md5.length())) {
            log_write("bad md5: %.*s vs %.*s\n", 32, md5_str, 32, entry.md5.c_str());
            return false;
        }
    }

    // 3. extract the zip
    if (!pbox->ShouldExit()) {
        auto zfile = unzOpen64(zip_out);
        if (!zfile) {
            log_write("failed to open zip: %s\n", zip_out.s);
            return false;
        }
        ON_SCOPE_EXIT(unzClose(zfile));

        // get manifest
        if (UNZ_END_OF_LIST_OF_FILE == unzLocateFile(zfile, "manifest.install", 0)) {
            log_write("failed to find manifest.install\n");
            return false;
        }

        ManifestEntries new_manifest;
        const auto old_manifest = LoadAndParseManifest(entry);
        {
            if (UNZ_OK != unzOpenCurrentFile(zfile)) {
                log_write("failed to open current file\n");
                return false;
            }
            ON_SCOPE_EXIT(unzCloseCurrentFile(zfile));

            unz_file_info64 info;
            if (UNZ_OK != unzGetCurrentFileInfo64(zfile, &info, 0, 0, 0, 0, 0, 0)) {
                log_write("failed to get current info\n");
                return false;
            }

            std::vector<char> manifest_data(info.uncompressed_size);
            if ((int)info.uncompressed_size != unzReadCurrentFile(zfile, manifest_data.data(), manifest_data.size())) {
                log_write("failed to read manifest file\n");
                return false;
            }

            new_manifest = ParseManifest(manifest_data);
            if (new_manifest.empty()) {
                log_write("manifest is empty!\n");
                return false;
            }
        }

        const auto unzip_to = [pbox, &fs, zfile](const fs::FsPath& inzip, fs::FsPath output) -> bool {
            pbox->NewTransfer(inzip);

            if (UNZ_END_OF_LIST_OF_FILE == unzLocateFile(zfile, inzip, 0)) {
                log_write("failed to find %s\n", inzip.s);
                return false;
            }

            if (UNZ_OK != unzOpenCurrentFile(zfile)) {
                log_write("failed to open current file\n");
                return false;
            }
            ON_SCOPE_EXIT(unzCloseCurrentFile(zfile));

            unz_file_info64 info;
            if (UNZ_OK != unzGetCurrentFileInfo64(zfile, &info, 0, 0, 0, 0, 0, 0)) {
                log_write("failed to get current info\n");
                return false;
            }

            if (output[0] != '/') {
                output = fs::AppendPath("/", output);
            }

            // create directories
            fs.CreateDirectoryRecursivelyWithPath(output);

            Result rc;
            if (R_FAILED(rc = fs.CreateFile(output, info.uncompressed_size, 0)) && rc != FsError_PathAlreadyExists) {
                log_write("failed to create file: %s 0x%04X\n", output.s, rc);
                return false;
            }

            FsFile f;
            if (R_FAILED(rc = fs.OpenFile(output, FsOpenMode_Write, &f))) {
                log_write("failed to open file: %s 0x%04X\n", output.s, rc);
                return false;
            }
            ON_SCOPE_EXIT(fsFileClose(&f));

            if (R_FAILED(rc = fsFileSetSize(&f, info.uncompressed_size))) {
                log_write("failed to set file size: %s 0x%04X\n", output.s, rc);
                return false;
            }

            std::vector<char> buf(chunk_size);
            u64 offset{};
            while (offset < info.uncompressed_size) {
                if (pbox->ShouldExit()) {
                    return false;
                }

                const auto bytes_read = unzReadCurrentFile(zfile, buf.data(), buf.size());
                if (bytes_read <= 0) {
                    log_write("failed to read zip file: %s\n", inzip.s);
                    return false;
                }

                if (R_FAILED(rc = fsFileWrite(&f, offset, buf.data(), bytes_read, FsWriteOption_None))) {
                    log_write("failed to write file: %s 0x%04X\n", output.s, rc);
                    return false;
                }

                pbox->UpdateTransfer(offset, info.uncompressed_size);
                offset += bytes_read;
            }

            return true;
        };

        // unzip manifest and info
        if (!unzip_to("info.json", BuildInfoCachePath(entry))) {
            return false;
        }
        if (!unzip_to("manifest.install", BuildManifestCachePath(entry))) {
            return false;
        }

        for (auto& new_entry : new_manifest) {
            if (pbox->ShouldExit()) {
                return false;
            }

            switch (new_entry.command) {
                case 'E': // both are the same?
                case 'U':
                    break;

                case 'G': { // checks if file exists, if not, extract
                    if (fs.FileExists(fs::AppendPath("/", new_entry.path))) {
                        continue;
                    }
                }   break;

                default:
                    log_write("bad command: %c\n", new_entry.command);
                    continue;
            }

            if (!unzip_to(new_entry.path, new_entry.path)) {
                return false;
            }
        }

        // finally finally, remove files no longer in the manifest
        for (auto& old_entry : old_manifest) {
            bool found = false;
            for (auto& new_entry : new_manifest) {
                if (!std::strcmp(old_entry.path, new_entry.path)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                const auto safe_buf = fs::AppendPath("/", old_entry.path);
                // std::strcat(safe_buf, old_entry.path);
                if (R_FAILED(fs.DeleteFile(safe_buf))) {
                    log_write("failed to delete: %s\n", safe_buf.s);
                } else {
                    log_write("deleted file: %s\n", safe_buf.s);
                }
            }
        }
    }

    log_write("finished install :)\n");
    return true;
}

// case-insensitive version of str.find()
auto FindCaseInsensitive(std::string_view base, std::string_view term) -> bool {
    const auto it = std::search(base.cbegin(), base.cend(), term.cbegin(), term.cend(), [](char a, char b){
        return std::toupper(a) == std::toupper(b);
    });
    return it != base.cend();
}

} // namespace

EntryMenu::EntryMenu(Entry& entry, const LazyImage& default_icon, Menu& menu)
: MenuBase{entry.title}
, m_entry{entry}
, m_default_icon{default_icon}
, m_menu{menu} {
    this->SetActions(
        std::make_pair(Button::DPAD_DOWN | Button::RS_DOWN, Action{[this](){
            if (m_index < (m_options.size() - 1)) {
                SetIndex(m_index + 1);
                App::PlaySoundEffect(SoundEffect_Focus);
            }
        }}),
        std::make_pair(Button::DPAD_UP | Button::RS_UP, Action{[this](){
            if (m_index != 0) {
                SetIndex(m_index - 1);
                App::PlaySoundEffect(SoundEffect_Focus);
            }
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto sidebar = std::make_shared<Sidebar>("Options"_i18n, Sidebar::Side::RIGHT);
            sidebar->Add(std::make_shared<SidebarEntryCallback>("More by Author"_i18n, [this](){
                m_menu.SetAuthor();
                SetPop();
            }, true));
            sidebar->Add(std::make_shared<SidebarEntryCallback>("Leave Feedback"_i18n, [this](){
                std::string out;
                if (R_SUCCEEDED(swkbd::ShowText(out)) && !out.empty()) {
                    const auto post = "name=" "switch_user" "&package=" + m_entry.name + "&message=" + out;
                    const auto file = BuildFeedbackCachePath(m_entry);

                    curl::Api().ToAsync(
                        curl::Url{URL_POST_FEEDBACK},
                        curl::Path{file},
                        curl::Fields{post},
                        curl::StopToken{this->GetToken()},
                        curl::OnComplete{[](auto& result){
                            if (result.success) {
                                log_write("got feedback!\n");
                            } else {
                                log_write("failed to send feedback :(");
                            }
                        }
                    });
                }
            }, true));
            App::Push(sidebar);
        }}),
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }})
    );

    SetTitleSubHeading("by " + m_entry.author);

    m_details = std::make_shared<ScrollableText>(m_entry.details, 0, 374, 250, 768, 18);
    m_changelog = std::make_shared<ScrollableText>(m_entry.changelog, 0, 374, 250, 768, 18);

    m_show_changlog ^= 1;
    ShowChangelogAction();

    const auto path = BuildBannerCachePath(m_entry);
    const auto url = BuildBannerUrl(m_entry);
    m_banner.cached = EntryLoadImageFile(path, m_banner);

    // race condition if we pop the widget before the download completes
    curl::Api().ToFileAsync(
        curl::Url{url},
        curl::Path{path},
        curl::Flags{curl::Flag_Cache},
        curl::StopToken{this->GetToken()},
        curl::OnComplete{[this, path](auto& result){
            if (result.success) {
                if (result.code == 304) {
                    m_banner.cached = false;
                } else {
                    EntryLoadImageFile(path, m_banner);
                }
            }
        }
    });

    SetSubHeading(m_entry.binary);
    SetSubHeading(m_entry.description);
    UpdateOptions();

    // todo: see Draw()
    // const Vec4 v{75, 110, 370, 155};
    // const Vec2 pad{10, 10};
    // m_list = std::make_unique<List>(3, 3, v, pad);
}

EntryMenu::~EntryMenu() {
}

void EntryMenu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);

    m_detail_changelog->Update(controller, touch);
}

void EntryMenu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    constexpr Vec4 line_vec(30, 86, 1220, 646);
    constexpr Vec4 banner_vec(70, line_vec.y + 20, 848.f, 208.f);
    constexpr Vec4 icon_vec(968, line_vec.y + 30, 256, 150);
    constexpr Vec4 grid_vec(icon_vec.x - 50, line_vec.y + 1, line_vec.w, line_vec.h - line_vec.y - 1);

    // nvgSave(vg);
    // nvgScissor(vg, line_vec.x, line_vec.y, line_vec.w - line_vec.x, line_vec.h - line_vec.y); // clip
    // ON_SCOPE_EXIT(nvgRestore(vg));

    gfx::drawRect(vg, grid_vec, theme->GetColour(ThemeEntryID_GRID));
    DrawIcon(vg, m_banner, m_entry.image.image ? m_entry.image : m_default_icon, banner_vec, false);
    DrawIcon(vg, m_entry.image, m_default_icon, icon_vec);

    constexpr float text_start_x = icon_vec.x;// - 10;
    float text_start_y = 218 + line_vec.y;
    const float text_inc_y = 32;
    const float font_size = 20;

    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "version: %s"_i18n.c_str(), m_entry.version.c_str());
    text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "updated: %s"_i18n.c_str(), m_entry.updated.c_str());
    text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "category: %s"_i18n.c_str(), m_entry.category.c_str());
    text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "extracted: %.2f MiB"_i18n.c_str(), (double)m_entry.extracted / 1024.0);
    text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "app_dls: %s"_i18n.c_str(), AppDlToStr(m_entry.app_dls).c_str());
    text_start_y += text_inc_y;

    // todo: rewrite this mess and use list
    constexpr float mm = 0;//20;
    constexpr Vec4 block{968.f + mm, 110.f, 256.f - mm*2, 60.f};
    const float x = block.x;
    float y = 1.f + text_start_y + (text_inc_y * 3) ;
    const float h = block.h;
    const float w = block.w;

    for (s32 i = m_options.size() - 1; i >= 0; i--) {
        const auto& option = m_options[i];
        auto text_id = ThemeEntryID_TEXT;
        if (m_index == i) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, theme, 4.f, Vec4{x, y, w, h});
        }

        gfx::drawTextArgs(vg, x + w / 2, y + h / 2, 22, NVG_ALIGN_MIDDLE | NVG_ALIGN_CENTER, theme->GetColour(text_id), option.display_text.c_str());
        y -= block.h + 18;
    }

    m_detail_changelog->Draw(vg, theme);
}

void EntryMenu::ShowChangelogAction() {
    std::function<void()> func = std::bind(&EntryMenu::ShowChangelogAction, this);
    m_show_changlog ^= 1;

    if (m_show_changlog) {
        SetAction(Button::L, Action{"Details"_i18n, func});
        m_detail_changelog = m_changelog;
    } else {
        SetAction(Button::L, Action{"Changelog"_i18n, func});
        m_detail_changelog = m_details;
    }
}

void EntryMenu::UpdateOptions() {
    const auto launch = [this](){
        nro_launch(m_entry.binary);
    };

    const auto install = [this](){
        App::Push(std::make_shared<ProgressBox>(m_entry.image.image, "Downloading "_i18n, m_entry.title, [this](auto pbox){
            return InstallApp(pbox, m_entry);
        }, [this](bool success){
            if (success) {
                App::Notify("Downloaded "_i18n + m_entry.title);
                m_entry.status = EntryStatus::Installed;
                m_menu.SetDirty();
                UpdateOptions();
            }
        }, 2));
    };

    const auto uninstall = [this](){
        App::Push(std::make_shared<ProgressBox>(m_entry.image.image, "Uninstalling "_i18n, m_entry.title, [this](auto pbox){
            return UninstallApp(pbox, m_entry);
        }, [this](bool success){
            if (success) {
                App::Notify("Removed "_i18n + m_entry.title);
                m_entry.status = EntryStatus::Get;
                m_menu.SetDirty();
                UpdateOptions();
            }
        }, 2));
    };

    const Option install_option{"Install"_i18n, install};
    const Option update_option{"Update"_i18n, install};
    const Option launch_option{"Launch"_i18n, launch};
    const Option remove_option{"Remove"_i18n, "Completely remove "_i18n + m_entry.title + '?', uninstall};

    m_options.clear();
    switch (m_entry.status) {
        case EntryStatus::Get:
            m_options.emplace_back(install_option);
            break;
        case EntryStatus::Installed:
            if (!m_entry.binary.empty() && m_entry.binary != "none") {
                m_options.emplace_back(launch_option);
            }
            m_options.emplace_back(remove_option);
            break;
        case EntryStatus::Local:
            if (!m_entry.binary.empty() && m_entry.binary != "none") {
                m_options.emplace_back(launch_option);
            }
            m_options.emplace_back(update_option);
            break;
        case EntryStatus::Update:
            m_options.emplace_back(update_option);
            m_options.emplace_back(remove_option);
            break;
    }

    SetIndex(0);
}

void EntryMenu::SetIndex(s64 index) {
    m_index = index;
    const auto option = m_options[m_index];
    if (option.confirm_text.empty()) {
        SetAction(Button::A, Action{option.display_text, option.func});
    } else {
        SetAction(Button::A, Action{option.display_text, [this, option](){
            App::Push(std::make_shared<OptionBox>(option.confirm_text, "No"_i18n, "Yes"_i18n, 1, [this, option](auto op_index){
                if (op_index && *op_index) {
                    option.func();
                }
            }));
        }});
    }
}

Menu::Menu() : grid::Menu{"AppStore"_i18n} {
    fs::FsNativeSd fs;
    fs.CreateDirectoryRecursively("/switch/sphaira/cache/appstore/icons");
    fs.CreateDirectoryRecursively("/switch/sphaira/cache/appstore/banners");
    fs.CreateDirectoryRecursively("/switch/sphaira/cache/appstore/screens");

    this->SetActions(
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            if (m_is_author) {
                m_is_author = false;
                if (m_is_search) {
                    SetSearch(m_search_term);
                } else {
                    SetFilter();
                }

                SetIndex(m_entry_author_jump_back);
                if (m_entry_author_jump_back >= 9) {
                    m_list->SetYoff((((m_entry_author_jump_back - 9) + 3) / 3) * m_list->GetMaxY());
                } else {
                    m_list->SetYoff(0);
                }
            } else if (m_is_search) {
                m_is_search = false;
                SetFilter();
                SetIndex(m_entry_search_jump_back);
                if (m_entry_search_jump_back >= 9) {
                    m_list->SetYoff(0);
                    m_list->SetYoff((((m_entry_search_jump_back - 9) + 3) / 3) * m_list->GetMaxY());
                } else {
                    m_list->SetYoff(0);
                }
            } else {
                SetPop();
            }
        }}),
        std::make_pair(Button::A, Action{"Info"_i18n, [this](){
            if (m_entries_current.empty()) {
                // log_write("pushing A when empty: size: %zu count: %zu\n", repo_json.size(), m_entries_current.size());
                return;
            }
            App::Push(std::make_shared<EntryMenu>(m_entries[m_entries_current[m_index]], m_default_image, *this));
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("AppStore Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            SidebarEntryArray::Items filter_items;
            filter_items.push_back("All"_i18n);
            filter_items.push_back("Games"_i18n);
            filter_items.push_back("Emulators"_i18n);
            filter_items.push_back("Tools"_i18n);
            filter_items.push_back("Advanced"_i18n);
            filter_items.push_back("Themes"_i18n);
            filter_items.push_back("Legacy"_i18n);
            filter_items.push_back("Misc"_i18n);

            SidebarEntryArray::Items sort_items;
            sort_items.push_back("Updated"_i18n);
            sort_items.push_back("Downloads"_i18n);
            sort_items.push_back("Size"_i18n);
            sort_items.push_back("Alphabetical"_i18n);

            SidebarEntryArray::Items order_items;
            order_items.push_back("Descending"_i18n);
            order_items.push_back("Ascending"_i18n);

            SidebarEntryArray::Items layout_items;
            layout_items.push_back("List"_i18n);
            layout_items.push_back("Icon"_i18n);
            layout_items.push_back("Grid"_i18n);

            options->Add(std::make_shared<SidebarEntryArray>("Filter"_i18n, filter_items, [this](s64& index_out){
                m_filter.Set(index_out);
                SetFilter();
            }, m_filter.Get()));

            options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this](s64& index_out){
                m_sort.Set(index_out);
                SortAndFindLastFile();
            }, m_sort.Get()));

            options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this](s64& index_out){
                m_order.Set(index_out);
                SortAndFindLastFile();
            }, m_order.Get()));

            options->Add(std::make_shared<SidebarEntryArray>("Layout"_i18n, layout_items, [this](s64& index_out){
                m_layout.Set(index_out);
                OnLayoutChange();
            }, m_layout.Get()));

            options->Add(std::make_shared<SidebarEntryCallback>("Search"_i18n, [this](){
                std::string out;
                if (R_SUCCEEDED(swkbd::ShowText(out)) && !out.empty()) {
                    SetSearch(out);
                    log_write("got %s\n", out.c_str());
                }
            }));
        }})
    );

    m_repo_download_state = ImageDownloadState::Progress;
    curl::Api().ToFileAsync(
        curl::Url{URL_JSON},
        curl::Path{REPO_PATH},
        curl::Flags{curl::Flag_Cache},
        curl::StopToken{this->GetToken()},
        curl::OnComplete{[this](auto& result){
            if (result.success) {
                m_repo_download_state = ImageDownloadState::Done;
                if (HasFocus()) {
                    ScanHomebrew();
                }
            } else {
                m_repo_download_state = ImageDownloadState::Failed;
            }
        }
    });

    OnLayoutChange();
}

Menu::~Menu() {

}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_index, m_entries_current.size(), [this](bool touch, auto i) {
        if (touch && m_index == i) {
            FireAction(Button::A);
        } else {
            App::PlaySoundEffect(SoundEffect_Focus);
            SetIndex(i);
        }
    });
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    if (m_entries.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Loading..."_i18n.c_str());
        return;
    }

    if (m_entries_current.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Empty!"_i18n.c_str());
        return;
    }

    // max images per frame, in order to not hit io / gpu too hard.
    const int image_load_max = 2;
    int image_load_count = 0;

    m_list->Draw(vg, theme, m_entries_current.size(), [this, &image_load_count](auto* vg, auto* theme, auto v, auto pos) {
        const auto& [x, y, w, h] = v;
        const auto index = m_entries_current[pos];
        auto& e = m_entries[index];
        auto& image = e.image;

        // try and load cached image.
        if (image_load_count < image_load_max && !image.image && !image.tried_cache) {
            image.tried_cache = true;
            image.cached = EntryLoadImageFile(BuildIconCachePath(e), image);
            if (image.cached) {
                image_load_count++;
            }
        }

        // lazy load image
        if (!image.image || image.cached) {
            switch (image.state) {
                case ImageDownloadState::None: {
                    const auto path = BuildIconCachePath(e);
                    const auto url = BuildIconUrl(e);
                    image.state = ImageDownloadState::Progress;
                    curl::Api().ToFileAsync(
                        curl::Url{url},
                        curl::Path{path},
                        curl::Flags{curl::Flag_Cache},
                        curl::StopToken{this->GetToken()},
                        curl::OnComplete{[this, &image](auto& result) {
                            if (result.success) {
                                image.state = ImageDownloadState::Done;
                                // data hasn't changed
                                if (result.code == 304) {
                                    image.cached = false;
                                }
                            } else {
                                image.state = ImageDownloadState::Failed;
                                log_write("failed to download image\n");
                            }
                        }
                    });
                }   break;
                case ImageDownloadState::Progress: {

                }   break;
                case ImageDownloadState::Done: {
                    if (image_load_count < image_load_max) {
                        image.cached = false;
                        if (!EntryLoadImageFile(BuildIconCachePath(e), e.image)) {
                            image.state = ImageDownloadState::Failed;
                        } else {
                            image_load_count++;
                        }
                    }
                }   break;
                case ImageDownloadState::Failed: {
                }   break;
            }
        }

        const auto selected = pos == m_index;
        const auto image_vec = DrawEntryNoImage(vg, theme, m_layout.Get(), v, selected, e.title.c_str(), e.author.c_str(), e.version.c_str());

        const auto image_scale = 256.0 / image_vec.w;
        DrawIcon(vg, e.image, m_default_image, image_vec.x, image_vec.y, image_vec.w, image_vec.h, true, image_scale);
        // gfx::drawImage(vg, x + 20, y + 20, image_size, image_size_h, image.image ? image.image : m_default_image);

        // todo: fix position on non-grid layout.
        float i_size = 22;
        switch (e.status) {
            case EntryStatus::Get:
                gfx::drawImage(vg, x + w - 30.f, y + 110, i_size, i_size, m_get.image, 20);
                break;
            case EntryStatus::Installed:
                gfx::drawImage(vg, x + w - 30.f, y + 110, i_size, i_size, m_installed.image, 20);
                break;
            case EntryStatus::Local:
                gfx::drawImage(vg, x + w - 30.f, y + 110, i_size, i_size, m_local.image, 20);
                break;
            case EntryStatus::Update:
                gfx::drawImage(vg, x + w - 30.f, y + 110, i_size, i_size, m_update.image, 20);
                break;
        }
    });
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
    // log_write("saying we got focus base: size: %zu count: %zu\n", repo_json.size(), m_entries.size());

    if (!m_default_image.image) {
        if (R_SUCCEEDED(romfsInit())) {
            ON_SCOPE_EXIT(romfsExit());
            EntryLoadImageFile("romfs:/default.png", m_default_image);
            EntryLoadImageFile("romfs:/UPDATE.png", m_update);
            EntryLoadImageFile("romfs:/GET.png", m_get);
            EntryLoadImageFile("romfs:/LOCAL.png", m_local);
            EntryLoadImageFile("romfs:/INSTALLED.png", m_installed);
        }
    }

    if (m_entries.empty()) {
        // log_write("got focus with empty size: size: %zu count: %zu\n", repo_json.size(), m_entries.size());

        if (m_repo_download_state == ImageDownloadState::Done) {
            // log_write("is done: size: %zu count: %zu\n", repo_json.size(), m_entries.size());
            ScanHomebrew();
        }
    } else {
        if (m_dirty) {
            m_dirty = false;
            const auto& current_entry = m_entries[m_entries_current[m_index]];
            Sort();

            for (u32 i = 0; i < m_entries_current.size(); i++) {
                if (current_entry.name == m_entries[m_entries_current[i]].name) {
                    const auto index = i;
                    const auto row = m_list->GetRow();
                    const auto page = m_list->GetPage();
                    // guesstimate where the position is
                    if (index >= page) {
                        m_list->SetYoff((((index - page) + row) / row) * m_list->GetMaxY());
                    } else {
                        m_list->SetYoff(0);
                    }
                    SetIndex(i);
                    break;
                }
            }
        }
    }
}

void Menu::SetIndex(s64 index) {
    m_index = index;
    if (!m_index) {
        m_list->SetYoff(0);
    }

    this->SetSubHeading(std::to_string(m_index + 1) + " / " + std::to_string(m_entries_current.size()));
}

void Menu::ScanHomebrew() {
    from_json(REPO_PATH, m_entries);

    fs::FsNativeSd fs;
    if (R_FAILED(fs.GetFsOpenResult())) {
        log_write("failed to open sd card in appstore scan\n");
        return;
    }

    // pre-allocate the max size, can shrink later if needed
    for (auto& index : m_entries_index) {
        index.reserve(m_entries.size());
    }

    for (u32 i = 0; i < m_entries.size(); i++) {
        auto& e = m_entries[i];

        m_entries_index[Filter_All].push_back(i);

        if (e.category == std::string_view{"game"}) {
            m_entries_index[Filter_Games].push_back(i);
        } else if (e.category == std::string_view{"emu"}) {
            m_entries_index[Filter_Emulators].push_back(i);
        } else if (e.category == std::string_view{"tool"}) {
            m_entries_index[Filter_Tools].push_back(i);
        } else if (e.category == std::string_view{"advanced"}) {
            m_entries_index[Filter_Advanced].push_back(i);
        } else if (e.category == std::string_view{"theme"}) {
            m_entries_index[Filter_Themes].push_back(i);
        } else if (e.category == std::string_view{"legacy"}) {
            m_entries_index[Filter_Legacy].push_back(i);
        } else {
            m_entries_index[Filter_Misc].push_back(i);
        }

        // fwiw, this is how N stores update info
        e.updated_num = std::atoi(e.updated.c_str()); // day
        e.updated_num += std::atoi(e.updated.c_str() + 3) * 100; // month
        e.updated_num += std::atoi(e.updated.c_str() + 6) * 100 * 100; // year

        e.status = EntryStatus::Get;
        // if binary is present, check for it, if not avalible, report as not installed
        // if there is not a binary path, then we have to trust the info.json
        // this can result in applications being shown as installed even though they
        // are deleted, this includes sys-modules.
        if (e.binary.empty() || e.binary == "none") {
            ReadFromInfoJson(e);
        } else {
            if (fs.FileExists(e.binary)) {
                // first check the info.json
                ReadFromInfoJson(e);
                // if we get here, this means that we have the file, but not the .info file
                // report the file as locally installed to match hb-appstore.
                if (e.status == EntryStatus::Get) {
                    e.status = EntryStatus::Local;
                }
            }
        }

        e.image.state = ImageDownloadState::None;
        e.image.image = 0; // images are lazy loaded
    }

    for (auto& index : m_entries_index) {
        index.shrink_to_fit();
    }

    SetFilter();
    SetIndex(0);
    Sort();
}

void Menu::Sort() {
    // log_write("doing sort: size: %zu count: %zu\n", repo_json.size(), m_entries.size());

    const auto sort = m_sort.Get();
    const auto order = m_order.Get();
    const auto filter = m_filter.Get();

    // returns true if lhs should be before rhs
    const auto sorter = [this, sort, order](EntryMini _lhs, EntryMini _rhs) -> bool {
        const auto& lhs = m_entries[_lhs];
        const auto& rhs = m_entries[_rhs];

        // fallback to name compare if the updated num is the same
        if (lhs.status == EntryStatus::Update && !(rhs.status == EntryStatus::Update)) {
            return true;
        } else if (!(lhs.status == EntryStatus::Update) && rhs.status == EntryStatus::Update) {
            return false;
        } else if (lhs.status == EntryStatus::Installed && !(rhs.status == EntryStatus::Installed)) {
            return true;
        } else if (!(lhs.status == EntryStatus::Installed) && rhs.status == EntryStatus::Installed) {
            return false;
        } else if (lhs.status == EntryStatus::Local && !(rhs.status == EntryStatus::Local)) {
            return true;
        } else if (!(lhs.status == EntryStatus::Local) && rhs.status == EntryStatus::Local) {
            return false;
        } else {
            switch (sort) {
                case SortType_Updated: {
                    if (lhs.updated_num == rhs.updated_num) {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
                    } else if (order == OrderType_Descending) {
                        return lhs.updated_num > rhs.updated_num;
                    } else {
                        return lhs.updated_num < rhs.updated_num;
                    }
                } break;
                case SortType_Downloads: {
                    if (lhs.app_dls == rhs.app_dls) {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
                    } else if (order == OrderType_Descending) {
                        return lhs.app_dls > rhs.app_dls;
                    } else {
                        return lhs.app_dls < rhs.app_dls;
                    }
                } break;
                case SortType_Size: {
                    if (lhs.extracted == rhs.extracted) {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
                    } else if (order == OrderType_Descending) {
                        return lhs.extracted > rhs.extracted;
                    } else {
                        return lhs.extracted < rhs.extracted;
                    }
                } break;
                case SortType_Alphabetical: {
                    if (order == OrderType_Descending) {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
                    } else {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) > 0;
                    }
                } break;
            }

            std::unreachable();
        }
    };


    char subheader[128]{};
    std::snprintf(subheader, sizeof(subheader), "Filter: %s | Sort: %s | Order: %s"_i18n.c_str(), i18n::get(FILTER_STR[filter]).c_str(), i18n::get(SORT_STR[sort]).c_str(), i18n::get(ORDER_STR[order]).c_str());
    SetTitleSubHeading(subheader);

    std::sort(m_entries_current.begin(), m_entries_current.end(), sorter);
}

void Menu::SortAndFindLastFile() {
    const auto name = GetEntry().name;
    Sort();
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries_current.size(); i++) {
        if (name == GetEntry(i).name) {
            index = i;
            break;
        }
    }

    if (index >= 0) {
        const auto row = m_list->GetRow();
        const auto page = m_list->GetPage();
        // guesstimate where the position is
        if (index >= page) {
            m_list->SetYoff((((index - page) + row) / row) * m_list->GetMaxY());
        } else {
            m_list->SetYoff(0);
        }
        SetIndex(index);
    }
}

void Menu::SetFilter() {
    m_is_search = false;
    m_is_author = false;

    m_entries_current = m_entries_index[m_filter.Get()];
    SetIndex(0);
    Sort();
}

void Menu::SetSearch(const std::string& term) {
    if (!m_is_search) {
        m_entry_search_jump_back = m_index;
    }

    m_search_term = term;
    m_entries_index_search.clear();
    const auto query = m_search_term;

    for (u64 i = 0; i < m_entries.size(); i++) {
        const auto& e = m_entries[i];
        if (FindCaseInsensitive(e.title, query) || FindCaseInsensitive(e.author, query) || FindCaseInsensitive(e.description, query)) {
            m_entries_index_search.emplace_back(i);
        }
    }

    m_is_search = true;
    m_entries_current = m_entries_index_search;
    SetIndex(0);
    Sort();
}

void Menu::SetAuthor() {
    if (!m_is_author) {
        m_entry_author_jump_back = m_index;
    }

    m_author_term = m_entries[m_entries_current[m_index]].author;
    m_entries_index_author.clear();
    const auto query = m_author_term;

    for (u64 i = 0; i < m_entries.size(); i++) {
        const auto& e = m_entries[i];
        if (FindCaseInsensitive(e.author, query)) {
            m_entries_index_author.emplace_back(i);
        }
    }

    m_is_author = true;
    m_entries_current = m_entries_index_author;
    SetIndex(0);
    Sort();
}

void Menu::OnLayoutChange() {
    m_index = 0;
    grid::Menu::OnLayoutChange(m_list, m_layout.Get());
}

LazyImage::~LazyImage() {
    if (image) {
        nvgDeleteImage(App::GetVg(), image);
    }
}

} // namespace sphaira::ui::menu::appstore
