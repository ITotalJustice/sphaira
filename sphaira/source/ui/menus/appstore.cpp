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

#include <minIni.h>
#include <string>
#include <cstring>
#include <yyjson.h>
#include <nanovg/stb_image.h>
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

constexpr const char* INI_SECTION = "appstore";

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

#if 0
auto BuildInfoUrl(const Entry& e) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/packages/%s/info.json", URL_BASE, e.name.c_str());
    return out;
}
#endif

auto BuildBannerUrl(const Entry& e) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/packages/%s/screen.png", URL_BASE, e.name.c_str());
    return out;
}

#if 0
auto BuildScreensUrl(const Entry& e, u8 num) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/packages/%s/screen%u.png", URL_BASE, e.name.c_str(), num+1);
    return out;
}
#endif

auto BuildMainifestUrl(const Entry& e) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/packages/%s/manifest.install", URL_BASE, e.name.c_str());
    return out;
}

auto BuildZipUrl(const Entry& e) -> std::string {
    char out[0x100];
    std::snprintf(out, sizeof(out), "%s/zips/%s.zip", URL_BASE, e.name.c_str());
    return out;
}

auto BuildFeedbackUrl(std::span<u32> ids) -> std::string {
    std::string out{"https://wiiubru.com/feedback/messages?ids="};
    for (u32 i = 0; i < ids.size(); i++) {
        if (i != 0) {
            out.push_back(',');
        }
        out += std::to_string(ids[i]);
    }
    return out;
}

auto BuildIconCachePath(const Entry& e) -> fs::FsPath {
    fs::FsPath out;
    std::snprintf(out, sizeof(out), "%s/icons/%s.png", CACHE_PATH, e.name.c_str());
    return out;
}

auto BuildBannerCachePath(const Entry& e) -> fs::FsPath {
    fs::FsPath out;
    std::snprintf(out, sizeof(out), "%s/banners/%s.png", CACHE_PATH, e.name.c_str());
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

void EntryLoadImageFile(fs::Fs& fs, const fs::FsPath& path, LazyImage& image) {
    // already have the image
    if (image.image) {
        log_write("warning, tried to load image: %s when already loaded\n", path);
        return;
    }
    auto vg = App::GetVg();

    std::vector<u8> image_buf;
    if (R_FAILED(fs.read_entire_file(path, image_buf))) {
        image.state = ImageDownloadState::Failed;
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
        image.state = ImageDownloadState::Failed;
        log_write("failed to load image from file: %s\n", path);
    } else {
        // log_write("loaded image from file: %s\n", path);
    }
}

void EntryLoadImageFile(const fs::FsPath& path, LazyImage& image) {
    if (!strncasecmp("romfs:/", path, 7)) {
        fs::FsStdio fs;
        EntryLoadImageFile(fs, path, image);
    } else {
        fs::FsNativeSd fs;
        EntryLoadImageFile(fs, path, image);
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
        gfx::drawRect(vg, x, y, w, h, nvgRGB(i.first_pixel[0], i.first_pixel[1], i.first_pixel[2]), rounded);
    }
    if (iw > w || ih > h) {
        crop = true;
        nvgSave(vg);
        nvgScissor(vg, x, y, w, h);
    }
    if (rounded_image) {
        gfx::drawImageRounded(vg, ix, iy, iw, ih, i.image);
    } else {
        gfx::drawImage(vg, ix, iy, iw, ih, i.image);
    }
    if (crop) {
        nvgRestore(vg);
    }
}

void DrawIcon(NVGcontext* vg, const LazyImage& l, const LazyImage& d, Vec4 vec, bool rounded = true, float scale = 1.0) {
    DrawIcon(vg, l, d, vec.x, vec.y, vec.w, vec.h, rounded, scale);
}

auto ScrollHelperDown(u64& index, u64& start, u64 step, u64 max, u64 size) -> bool {
    if (size && index < (size - 1)) {
        if (index < (size - step)) {
            index = index + step;
            App::PlaySoundEffect(SoundEffect_Scroll);
        } else {
            index = size - 1;
            App::PlaySoundEffect(SoundEffect_Scroll);
        }
        if (index - start >= max) {
            log_write("moved down\n");
            start += step;
        }

        return true;
    }

    return false;
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
    const auto manifest_path = BuildManifestCachePath(e);

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
                log_write("failed to delete file: %s\n", safe_buf);
            } else {
                log_write("deleted file: %s\n", safe_buf);
                // todo: delete empty directories!
                // fs::delete_directory(safe_buf);
            }
        }
    }

    // remove directory, this will also delete manifest and info
    const auto dir = BuildPackageCachePath(entry);
    pbox->NewTransfer("Removing " + dir);
    if (R_FAILED(fs.DeleteDirectoryRecursively(dir))) {
        log_write("failed to delete folder: %s\n", dir);
    } else {
        log_write("deleted: %s\n", dir);
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
        pbox->NewTransfer("Downloading " + entry.title);
        log_write("starting download\n");

        const auto url = BuildZipUrl(entry);
        if (!DownloadFile(url, zip_out, "", [pbox](u32 dltotal, u32 dlnow, u32 ultotal, u32 ulnow){
            if (pbox->ShouldExit()) {
                return false;
            }
            pbox->UpdateTransfer(dlnow, dltotal);
            return true;
        })) {
            log_write("error with download\n");
            // push popup error box
            return false;
            // return appletEnterFatalSection();
        }
    }

    ON_SCOPE_EXIT(fs.DeleteFile(zip_out));

    // 2. md5 check the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Checking MD5");
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
            log_write("bad md5: %.*s vs %.*s\n", 32, md5_str, 32, entry.md5);
            return false;
        }
    }

    // 3. extract the zip
    if (!pbox->ShouldExit()) {
        auto zfile = unzOpen64(zip_out);
        if (!zfile) {
            log_write("failed to open zip: %s\n", zip_out);
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
                log_write("failed to find %s\n", inzip);
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
            fs.CreateDirectoryRecursivelyWithPath(output, true);

            Result rc;
            if (R_FAILED(rc = fs.CreateFile(output, info.uncompressed_size, 0, true)) && rc != FsError_ResultPathAlreadyExists) {
                log_write("failed to create file: %s 0x%04X\n", output, rc);
                return false;
            }

            FsFile f;
            if (R_FAILED(rc = fs.OpenFile(output, FsOpenMode_Write, &f))) {
                log_write("failed to open file: %s 0x%04X\n", output, rc);
                return false;
            }
            ON_SCOPE_EXIT(fsFileClose(&f));

            if (R_FAILED(rc = fsFileSetSize(&f, info.uncompressed_size))) {
                log_write("failed to set file size: %s 0x%04X\n", output, rc);
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
                    log_write("failed to read zip file: %s\n", inzip);
                    return false;
                }

                if (R_FAILED(rc = fsFileWrite(&f, offset, buf.data(), bytes_read, FsWriteOption_None))) {
                    log_write("failed to write file: %s 0x%04X\n", output, rc);
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
                if (R_FAILED(fs.DeleteFile(safe_buf, true))) {
                    log_write("failed to delete: %s\n", safe_buf);
                } else {
                    log_write("deleted file: %s\n", safe_buf);
                }
            }
        }
    }

    log_write("finished install :)\n");
    return true;
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

                    DownloadFileAsync(URL_POST_FEEDBACK, file, post, [](std::vector<u8>& data, bool success){
                        if (success) {
                            log_write("got feedback!\n");
                        } else {
                            log_write("failed to send feedback :(");
                        }
                    });
                }
            }, true));
            App::Push(sidebar);
        }}),
        // std::make_pair(Button::A, Action{m_entry.status == EntryStatus::Update ? "Update" : "Install", [this](){
        //     App::Push(std::make_shared<ProgressBox>("App Install", [this](auto pbox){
        //         InstallApp(pbox, m_entry);
        //     }, 2));
        // }}),
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }})
    );

    // SidebarEntryCallback
            // if (!m_entries_current.empty() && !GetEntry().url.empty()) {
            //     options->Add(std::make_shared<SidebarEntryCallback>("Show Release Page"))
            // }

    SetTitleSubHeading("by " + m_entry.author);

    // const char* very_long = "total@fedora:~/dev/switch/sphaira$ nxlink build/MinSizeRel/*.nro total@fedora:~/dev/switch/sphaira$ nxlink build/MinSizeRel/*.nro total@fedora:~/dev/switch/sphaira$ nxlink build/MinSizeRel/*.nro total@fedora:~/dev/switch/sphaira$ nxlink build/MinSizeRel/*.nro";

    m_details = std::make_shared<ScrollableText>(m_entry.details, 0, 374, 250, 768, 18);
    m_changelog = std::make_shared<ScrollableText>(m_entry.changelog, 0, 374, 250, 768, 18);

    m_show_changlog ^= 1;
    ShowChangelogAction();

    const auto path = BuildBannerCachePath(m_entry);
    const auto url = BuildBannerUrl(m_entry);

    if (fs::FsNativeSd().FileExists(path)) {
        EntryLoadImageFile(path, m_banner);
    }

    // race condition if we pop the widget before the download completes
    if (!m_banner.image) {
        DownloadFileAsync(url, path, "", [this, path](std::vector<u8>& data, bool success){
            if (success) {
                EntryLoadImageFile(path, m_banner);
            }
        }, nullptr, DownloadPriority::High);
    }

    // ignore screen shots, most apps don't have any sadly.
    #if 0
    m_screens.resize(m_entry.screens);

    for (u32 i = 0; i < m_screens.size(); i++) {
        path = BuildScreensCachePath(m_entry.name, i);
        url = BuildScreensUrl(m_entry.name, i);

        if (fs::file_exists(path.c_str())) {
            EntryLoadImageFile(path, m_screens[i]);
        } else {
            DownloadFileAsync(url.c_str(), path.c_str(), [this, i, path](std::vector<u8>& data, bool success){
                EntryLoadImageFile(path, m_screens[i]);
            }, nullptr, DownloadPriority::High);
        }
    }
    #endif

    SetSubHeading(m_entry.binary);
    SetSubHeading(m_entry.description);
    UpdateOptions();
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

    // nvgSave(vg);
    // nvgScissor(vg, line_vec.x, line_vec.y, line_vec.w - line_vec.x, line_vec.h - line_vec.y); // clip
    // ON_SCOPE_EXIT(nvgRestore(vg));

    DrawIcon(vg, m_banner, m_entry.image.image ? m_entry.image : m_default_icon, banner_vec, false);
    DrawIcon(vg, m_entry.image, m_default_icon, icon_vec);

    // gfx::drawImage(vg, icon_vec, m_entry.image.image);
    constexpr float text_start_x = icon_vec.x;// - 10;
    float text_start_y = 218 + line_vec.y;
    const float text_inc_y = 32;
    const float font_size = 20;

    // gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "%s", m_entry.name.c_str());
    // gfx::drawTextBox(vg, text_start_x - 20, text_start_y, font_size, icon_vec.w + 20*2, theme->elements[ThemeEntryID_TEXT].colour, m_entry.description.c_str(), NVG_ALIGN_CENTER);
    // text_start_y += text_inc_y * 2.0;

    // gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "author: %s", m_entry.author.c_str());
    // text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "version: %s", m_entry.version.c_str());
    text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "updated: %s", m_entry.updated.c_str());
    text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "category: %s", m_entry.category.c_str());
    text_start_y += text_inc_y;
    // gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "license: %s", m_entry.license.c_str());
    // text_start_y += text_inc_y;
    // gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "title: %s", m_entry.title.c_str());
    // text_start_y += text_inc_y;
    // gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "filesize: %.2f MiB", (double)m_entry.filesize / 1024.0);
    // text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "extracted: %.2f MiB", (double)m_entry.extracted / 1024.0);
    text_start_y += text_inc_y;
    gfx::drawTextArgs(vg, text_start_x, text_start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->elements[ThemeEntryID_TEXT].colour, "app_dls: %s", AppDlToStr(m_entry.app_dls).c_str());
    text_start_y += text_inc_y;

    // for (const auto& option : m_options) {
    const auto& text_col = theme->elements[ThemeEntryID_TEXT].colour;

    constexpr float mm = 0;//20;
    constexpr Vec4 block{968.f + mm, 110.f, 256.f - mm*2, 60.f};
    constexpr float text_xoffset{15.f};
    const float x = block.x;
    float y = 1.f + text_start_y + (text_inc_y * 3) ;
    const float h = block.h;
    const float w = block.w;

    for (s32 i = m_options.size() - 1; i >= 0; i--) {
        const auto& option = m_options[i];
        auto text_id = ThemeEntryID_TEXT;
        if (m_index == i) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, 4.f, theme->elements[ThemeEntryID_SELECTED_OVERLAY].colour, x, y, w, h, theme->elements[ThemeEntryID_SELECTED].colour);
        } else {
            // if (i == m_index_offset) {
                // gfx::drawRect(vg, x, y, w, 1.f, text_col);
            // }
            // gfx::drawRect(vg, x, y + h, w, 1.f, text_col);
        }

        gfx::drawTextArgs(vg, x + w / 2, y + h / 2, 22, NVG_ALIGN_MIDDLE | NVG_ALIGN_CENTER, theme->elements[ThemeEntryID_TEXT].colour, option.display_text.c_str());
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
        App::Push(std::make_shared<ProgressBox>("Installing "_i18n + m_entry.title, [this](auto pbox){
            return InstallApp(pbox, m_entry);
        }, [this](bool success){
            if (success) {
                m_entry.status = EntryStatus::Installed;
                m_menu.SetDirty();
                UpdateOptions();
            }
        }, 2));
    };

    const auto uninstall = [this](){
        App::Push(std::make_shared<ProgressBox>("Uninstalling "_i18n + m_entry.title, [this](auto pbox){
            return UninstallApp(pbox, m_entry);
        }, [this](bool success){
            if (success) {
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

void EntryMenu::SetIndex(std::size_t index) {
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

auto toLower(const std::string& str) -> std::string {
	std::string lower;
	std::transform(str.cbegin(), str.cend(), std::back_inserter(lower), tolower);
	return lower;
}

Menu::Menu(const std::vector<NroEntry>& nro_entries) : MenuBase{"AppStore"}, m_nro_entries{nro_entries} {
    fs::FsNativeSd fs;
    fs.CreateDirectoryRecursively("/switch/sphaira/cache/appstore/icons");
    fs.CreateDirectoryRecursively("/switch/sphaira/cache/appstore/banners");
    fs.CreateDirectoryRecursively("/switch/sphaira/cache/appstore/screens");

    // m_span = m_entries;

    this->SetActions(
        std::make_pair(Button::RIGHT, Action{[this](){
            if (m_entries_current.empty()) {
                return;
            }

            if (m_index < (m_entries_current.size() - 1) && (m_index + 1) % 3 != 0) {
                SetIndex(m_index + 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                log_write("moved right\n");
            }
        }}),
        std::make_pair(Button::LEFT, Action{[this](){
            if (m_entries_current.empty()) {
                return;
            }

            if (m_index != 0 && (m_index % 3) != 0) {
                SetIndex(m_index - 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                log_write("moved left\n");
            }
        }}),
        std::make_pair(Button::DOWN, Action{[this](){
            if (ScrollHelperDown(m_index, m_start, 3, 9, m_entries_current.size())) {
                SetIndex(m_index);
            }
        }}),
        std::make_pair(Button::UP, Action{[this](){
            if (m_entries_current.empty()) {
                return;
            }

            if (m_index >= 3) {
                SetIndex(m_index - 3);
                App::PlaySoundEffect(SoundEffect_Scroll);
                if (m_index < m_start ) {
                    // log_write("moved up\n");
                    m_start -= 3;
                }
            }
        }}),
        std::make_pair(Button::R2, Action{(u8)ActionType::HELD, [this](){
            if (ScrollHelperDown(m_index, m_start, 9, 9, m_entries_current.size())) {
                SetIndex(m_index);
            }
        }}),
        std::make_pair(Button::L2, Action{(u8)ActionType::HELD, [this](){
            if (m_entries.empty()) {
                return;
            }

            if (m_index >= 9) {
                SetIndex(m_index - 9);
                App::PlaySoundEffect(SoundEffect_Scroll);
                while (m_index < m_start) {
                    // log_write("moved up\n");
                    m_start -= 3;
                }
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
            order_items.push_back("Decending"_i18n);
            order_items.push_back("Ascending"_i18n);

            options->Add(std::make_shared<SidebarEntryArray>("Filter"_i18n, filter_items, [this, filter_items](std::size_t& index_out){
                SetFilter((Filter)index_out);
            }, (std::size_t)m_filter));

            options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this, sort_items](std::size_t& index_out){
                SetSort((SortType)index_out);
            }, (std::size_t)m_sort));

            options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this, order_items](std::size_t& index_out){
                SetOrder((OrderType)index_out);
            }, (std::size_t)m_order));

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
    #if 0
    DownloadMemoryAsync(URL_JSON, [this](std::vector<u8>& data, bool success){
        if (success) {
            repo_json = data;
            repo_json.push_back('\0');
            m_repo_download_state = ImageDownloadState::Done;
            if (HasFocus()) {
                ScanHomebrew();
            }
        } else {
            m_repo_download_state = ImageDownloadState::Failed;
        }
    });
    #else
    FsTimeStampRaw time_stamp{};
    u64 current_time{};
    bool download_file = false;
    if (R_SUCCEEDED(fs.GetFsOpenResult())) {
        fs.GetFileTimeStampRaw(REPO_PATH, &time_stamp);
        timeGetCurrentTime(TimeType_Default, &current_time);
    }

    // this fails if we don't have the file or on fw < 3.0.0
    if (!time_stamp.is_valid) {
        download_file = true;
    } else {
        // check the date, if older than 1day, then fetch new file
        // this relaxes the spam to their server, don't want to fetch repo
        // every time the user opens the app!
        const auto time_file = (time_t)time_stamp.created;
        const auto time_cur = (time_t)current_time;
        const auto tm_file = *gmtime(&time_file);
        const auto tm_cur = *gmtime(&time_cur);
        if (tm_cur.tm_yday > tm_file.tm_yday || tm_cur.tm_year > tm_file.tm_year) {
            log_write("repo.json expired, downloading new! cur_yday: %d file_yday: %d | cur_year: %d file_year: %d\n", tm_cur.tm_yday, tm_file.tm_yday, tm_cur.tm_year, tm_file.tm_year);
            download_file = true;
        } else {
            log_write("repo.json not expired! cur_yday: %d file_yday: %d | cur_year: %d file_year: %d\n", tm_cur.tm_yday, tm_file.tm_yday, tm_cur.tm_year, tm_file.tm_year);
            // time_file = (time_t)time_stamp.modified;
            // tm_file = *gmtime(&time_file);
            // log_write("repo.json not expired! cur_yday: %d file_yday: %d | cur_year: %d file_year: %d\n", tm_cur.tm_yday, tm_file.tm_yday, tm_cur.tm_year, tm_file.tm_year);
            // time_file = (time_t)time_stamp.accessed;
            // tm_file = *gmtime(&time_file);
            // log_write("repo.json not expired! cur_yday: %d file_yday: %d | cur_year: %d file_year: %d\n", tm_cur.tm_yday, tm_file.tm_yday, tm_cur.tm_year, tm_file.tm_year);
        }
    }

    // todo: remove me soon
    // download_file = true;

    if (download_file) {
        DownloadFileAsync(URL_JSON, REPO_PATH, "", [this](std::vector<u8>& data, bool success){
            if (success) {
                m_repo_download_state = ImageDownloadState::Done;
                if (HasFocus()) {
                    ScanHomebrew();
                }
            } else {
                m_repo_download_state = ImageDownloadState::Failed;
            }
        });
    } else {
        m_repo_download_state = ImageDownloadState::Done;
    }
    #endif

    m_filter = (Filter)ini_getl(INI_SECTION, "filter", m_filter, App::CONFIG_PATH);
    m_sort = (SortType)ini_getl(INI_SECTION, "sort", m_sort, App::CONFIG_PATH);
    m_order = (OrderType)ini_getl(INI_SECTION, "order", m_order, App::CONFIG_PATH);

    Sort();
}

Menu::~Menu() {

}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    if (m_entries.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, gfx::Colour::YELLOW, "Loading...");
        return;
    }

    if (m_entries_current.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, gfx::Colour::YELLOW, "Empty!");
        return;
    }

    const u64 SCROLL = m_start;
    const u64 max_entry_display = 9;
    const u64 nro_total = m_entries_current.size();
    const u64 cursor_pos = m_index;

    // only draw scrollbar if needed
    if (nro_total > max_entry_display) {
        const auto scrollbar_size = 500.f;
        const auto sb_h = 3.f / (float)nro_total * scrollbar_size;
        const auto sb_y = SCROLL / 3.f;
        gfx::drawRect(vg, SCREEN_WIDTH - 50, 100, 10, scrollbar_size, theme->elements[ThemeEntryID_GRID].colour);
        gfx::drawRect(vg, SCREEN_WIDTH - 50+2, 102 + sb_h * sb_y, 10-4, sb_h + (sb_h * 2) - 4, theme->elements[ThemeEntryID_TEXT_SELECTED].colour);
    }

    for (u64 i = 0, pos = SCROLL, y = 110, w = 370, h = 155; pos < nro_total && i < max_entry_display; y += h + 10) {
        for (u64 j = 0, x = 75; j < 3 && pos < nro_total && i < max_entry_display; j++, i++, pos++, x += w + 10) {
            const auto index = m_entries_current[pos];
            auto& e = m_entries[index];

            // lazy load image
            if (!e.image.image) {
                switch (e.image.state) {
                    case ImageDownloadState::None: {
                        const auto path = BuildIconCachePath(e);
                        if (fs::FsNativeSd().FileExists(path)) {
                            EntryLoadImageFile(path, e.image);
                        } else {
                            const auto url = BuildIconUrl(e);
                            e.image.state = ImageDownloadState::Progress;
                            DownloadFileAsync(url, path, "", [this, index](std::vector<u8>& data, bool success) {
                                if (success) {
                                    m_entries[index].image.state = ImageDownloadState::Done;
                                } else {
                                    m_entries[index].image.state = ImageDownloadState::Failed;
                                    log_write("failed to download image\n");
                                }
                            }, nullptr, DownloadPriority::High);
                        }
                    }   break;
                    case ImageDownloadState::Progress: {

                    }   break;
                    case ImageDownloadState::Done: {
                        EntryLoadImageFile(BuildIconCachePath(e), e.image);
                    }   break;
                    case ImageDownloadState::Failed: {
                    }   break;
                }
            }

            auto text_id = ThemeEntryID_TEXT;
            if (pos == cursor_pos) {
                text_id = ThemeEntryID_TEXT_SELECTED;
                gfx::drawRectOutline(vg, 4.f, theme->elements[ThemeEntryID_SELECTED_OVERLAY].colour, x, y, w, h, theme->elements[ThemeEntryID_SELECTED].colour);
            } else {
                DrawElement(x, y, w, h, ThemeEntryID_GRID);
            }

            constexpr double image_scale = 256.0 / 115.0;
            // const float image_size = 256 / image_scale;
            // const float image_size_h = 150 / image_scale;
            DrawIcon(vg, e.image, m_default_image, x + 20, y + 20, 115, 115, true, image_scale);
            // gfx::drawImage(vg, x + 20, y + 20, image_size, image_size_h, e.image.image ? e.image.image : m_default_image);

            nvgSave(vg);
            nvgScissor(vg, x, y, w - 30.f, h); // clip
            {
                const float font_size = 18;
                gfx::drawTextArgs(vg, x + 148, y + 45, font_size, NVG_ALIGN_LEFT, theme->elements[text_id].colour, e.title.c_str());
                gfx::drawTextArgs(vg, x + 148, y + 80, font_size, NVG_ALIGN_LEFT, theme->elements[text_id].colour, e.author.c_str());
                gfx::drawTextArgs(vg, x + 148, y + 115, font_size, NVG_ALIGN_LEFT, theme->elements[text_id].colour, e.version.c_str());
            }
            nvgRestore(vg);

            float i_size = 22;
            switch (e.status) {
                case EntryStatus::Get:
                    gfx::drawImageRounded(vg, x + w - 30.f, y + 110, i_size, i_size, m_get.image);
                    break;
                case EntryStatus::Installed:
                    gfx::drawImageRounded(vg, x + w - 30.f, y + 110, i_size, i_size, m_installed.image);
                    break;
                case EntryStatus::Local:
                    gfx::drawImageRounded(vg, x + w - 30.f, y + 110, i_size, i_size, m_local.image);
                    break;
                case EntryStatus::Update:
                    gfx::drawImageRounded(vg, x + w - 30.f, y + 110, i_size, i_size, m_update.image);
                    break;
            }
        }
    }
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
            // m_start = 0;
            // m_index = 0;
            log_write("\nold index: %zu start: %zu\n", m_index, m_start);
            // old index: 19 start: 12
            Sort();

            for (u32 i = 0; i < m_entries_current.size(); i++) {
                if (current_entry.name == m_entries[m_entries_current[i]].name) {
                    SetIndex(i);
                    if (i >= 9) {
                        m_start = (i - 9) / 3 * 3 + 3;
                    } else {
                        m_start = 0;
                    }
                    log_write("\nnew index: %zu start: %zu\n", m_index, m_start);
                    break;
                }
            }
        }
    }
}

void Menu::SetIndex(std::size_t index) {
    m_index = index;
    if (!m_index) {
        m_start = 0;
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

    SetFilter(Filter_All);
    SetIndex(0);
}

void Menu::Sort() {
    // log_write("doing sort: size: %zu count: %zu\n", repo_json.size(), m_entries.size());

    // returns true if lhs should be before rhs
    const auto sorter = [this](EntryMini _lhs, EntryMini _rhs) -> bool {
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
            switch (m_sort) {
                case SortType_Updated: {
                    if (lhs.updated_num == rhs.updated_num) {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
                    } else if (m_order == OrderType_Decending) {
                        return lhs.updated_num > rhs.updated_num;
                    } else {
                        return lhs.updated_num < rhs.updated_num;
                    }
                } break;
                case SortType_Downloads: {
                    if (lhs.app_dls == rhs.app_dls) {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
                    } else if (m_order == OrderType_Decending) {
                        return lhs.app_dls > rhs.app_dls;
                    } else {
                        return lhs.app_dls < rhs.app_dls;
                    }
                } break;
                case SortType_Size: {
                    if (lhs.extracted == rhs.extracted) {
                        return strcasecmp(lhs.name.c_str(), rhs.name.c_str()) < 0;
                    } else if (m_order == OrderType_Decending) {
                        return lhs.extracted > rhs.extracted;
                    } else {
                        return lhs.extracted < rhs.extracted;
                    }
                } break;
                case SortType_Alphabetical: {
                    if (m_order == OrderType_Decending) {
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
    std::snprintf(subheader, sizeof(subheader), "Sort: %s | Filter: %s | Order: %s", SORT_STR[m_sort], FILTER_STR[m_filter], ORDER_STR[m_order]);
    SetTitleSubHeading(subheader);

    std::sort(m_entries_current.begin(), m_entries_current.end(), sorter);
}

void Menu::SetFilter(Filter filter) {
    m_is_search = false;
    m_is_author = false;
    RemoveAction(Button::B);

    m_filter = filter;
    m_entries_current = m_entries_index[m_filter];
    ini_putl(INI_SECTION, "filter", m_filter, App::CONFIG_PATH);
    SetIndex(0);
    Sort();
}

void Menu::SetSort(SortType sort) {
    m_sort = sort;
    ini_putl(INI_SECTION, "sort", m_sort, App::CONFIG_PATH);
    SetIndex(0);
    Sort();
}

void Menu::SetOrder(OrderType order) {
    m_order = order;
    ini_putl(INI_SECTION, "order", m_order, App::CONFIG_PATH);
    SetIndex(0);
    Sort();
}

void Menu::SetSearch(const std::string& term) {
    if (!m_is_search) {
        m_entry_search_jump_back = m_index;
    }

    m_search_term = term;
    m_entries_index_search.clear();
    const auto query = toLower(m_search_term);
    const auto npos = std::string::npos;

    for (u64 i = 0; i < m_entries.size(); i++) {
        const auto& e = m_entries[i];
        if (toLower(e.title).find(query) != npos || toLower(e.author).find(query) != npos || toLower(e.details).find(query) != npos || toLower(e.description).find(query) != npos) {
            m_entries_index_search.emplace_back(i);
        }
    }

    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetFilter(m_filter);
        SetIndex(m_entry_search_jump_back);
        if (m_entry_search_jump_back >= 9) {
            m_start = (m_entry_search_jump_back - 9) / 3 * 3 + 3;
        } else {
            m_start = 0;
        }
    }});

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

    for (u64 i = 0; i < m_entries.size(); i++) {
        const auto& e = m_entries[i];
        if (e.author.find(m_author_term) != std::string::npos) {
            m_entries_index_author.emplace_back(i);
        }
    }

    SetAction(Button::B, Action{"Back"_i18n, [this](){
        if (m_is_search) {
            SetSearch(m_search_term);
        } else {
            SetFilter(m_filter);
        }
        SetIndex(m_entry_author_jump_back);
        if (m_entry_author_jump_back >= 9) {
            m_start = (m_entry_author_jump_back - 9) / 3 * 3 + 3;
        } else {
            m_start = 0;
        }
    }});

    m_is_author = true;
    m_entries_current = m_entries_index_author;
    SetIndex(0);
    Sort();
}

LazyImage::~LazyImage() {
    if (image) {
        nvgDeleteImage(App::GetVg(), image);
    }
}

} // namespace sphaira::ui::menu::appstore
