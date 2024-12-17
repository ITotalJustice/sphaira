#include "ui/menus/themezer.hpp"
#include "ui/progress_box.hpp"
#include "ui/option_box.hpp"
#include "ui/sidebar.hpp"

#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "download.hpp"
#include "ui/nvg_util.hpp"
#include "swkbd.hpp"
#include "i18n.hpp"

#include <minIni.h>
#include <nanovg/stb_image.h>
#include <cstring>
#include <minizip/unzip.h>
#include <yyjson.h>
#include "yyjson_helper.hpp"

namespace sphaira::ui::menu::themezer {
namespace {

// format is /themes/sphaira/Theme Name by Author/theme_name-type.nxtheme
constexpr fs::FsPath THEME_FOLDER{"/themes/sphaira/"};
constexpr auto CACHE_PATH = "/switch/sphaira/cache/themezer";
constexpr auto URL_BASE = "https://switch.cdn.fortheusers.org";

constexpr const char* REQUEST_TARGET[]{
    "ResidentMenu",
    "Entrance",
    "Flaunch",
    "Set",
    "Psl",
    "MyPage",
    "Notification"
};

constexpr const char* REQUEST_SORT[]{
    "downloads",
    "updated",
    "likes",
    "id"
};

constexpr const char* REQUEST_ORDER[]{
    "desc",
    "asc"
};

// https://api.themezer.net/?query=query($nsfw:Boolean,$target:String,$page:Int,$limit:Int,$sort:String,$order:String,$query:String,$creators:[String!]){themeList(nsfw:$nsfw,target:$target,page:$page,limit:$limit,sort:$sort,order:$order,query:$query,creators:$creators){id,creator{id,display_name},details{name,description},last_updated,dl_count,like_count,target,preview{original,thumb}}}&variables={"nsfw":false,"target":null,"page":1,"limit":10,"sort":"updated","order":"desc","query":null,"creators":["695065006068334622"]}
// https://api.themezer.net/?query=query($nsfw:Boolean,$page:Int,$limit:Int,$sort:String,$order:String,$query:String,$creators:[String!]){packList(nsfw:$nsfw,page:$page,limit:$limit,sort:$sort,order:$order,query:$query,creators:$creators){id,creator{id,display_name},details{name,description},last_updated,dl_count,like_count,themes{id,creator{display_name},details{name,description},last_updated,dl_count,like_count,target,preview{original,thumb}}}}&variables={"nsfw":false,"page":1,"limit":10,"sort":"updated","order":"desc","query":null,"creators":["695065006068334622"]}

// i know, this is cursed
auto apiBuildUrlListInternal(const Config& e, bool is_pack) -> std::string {
    std::string api = "https://api.themezer.net/?query=query";
    std::string fields = "{id,creator{id,display_name},details{name,description},last_updated,dl_count,like_count";
    const char* boolarr[2] = { "false", "true" };

    std::string cmd;
    std::string p0 = "$nsfw:Boolean,$page:Int,$limit:Int,$sort:String,$order:String";
    std::string p1 = "nsfw:$nsfw,page:$page,limit:$limit,sort:$sort,order:$order";
    std::string json = "\"nsfw\":"+std::string{boolarr[e.nsfw]}+",\"page\":"+std::to_string(e.page)+",\"limit\":"+std::to_string(e.limit)+",\"sort\":\""+std::string{REQUEST_SORT[e.sort_index]}+"\",\"order\":\""+std::string{REQUEST_ORDER[e.order_index]}+"\"";

    if (is_pack) {
        cmd = "packList";
        fields += ",themes{id,creator{display_name},details{name,description},last_updated,dl_count,like_count,target,preview{original,thumb}}";
    } else {
        cmd = "themeList";
        p0 += ",$target:String";
        p1 += ",target:$target";
        if (e.target_index < 7) {
            json += ",\"target\":\"" + std::string{REQUEST_TARGET[e.target_index]} + "\"";
        } else {
            json += ",\"target\":null";
        }
    }

    if (!e.creator.empty()) {
        p0 += ",$creators:[String!]";
        p1 += ",creators:$creators";
        json += ",\"creators\":[\"" + e.creator + "\"]";
    }

    if (!e.query.empty()) {
        p0 += ",$query:String";
        p1 += ",query:$query";
        json += ",\"query\":\"" + e.query + "\"";
    }

    return api+"("+p0+"){"+cmd+"("+p1+")"+fields+"}}&variables={"+json+"}";
}

auto apiBuildUrlDownloadInternal(const std::string& id, bool is_pack) -> std::string {
    char url[2048];
    std::snprintf(url, sizeof(url), "https://api.themezer.net/?query=query{download%s(id:\"%s\"){filename,url,mimetype}}", is_pack ? "Pack" : "Theme", id.c_str());
    return url;
    // https://api.themezer.net/?query=query{downloadPack(id:"11"){filename,url,mimetype}}
}

auto apiBuildUrlDownloadTheme(const ThemeEntry& e) -> std::string {
    return apiBuildUrlDownloadInternal(e.id, false);
}

auto apiBuildUrlDownloadPack(const PackListEntry& e) -> std::string {
    return apiBuildUrlDownloadInternal(e.id, true);
}

auto apiBuildFilePack(const PackListEntry& e) -> fs::FsPath {
    fs::FsPath path;
    std::snprintf(path, sizeof(path), "%s/%s By %s/", THEME_FOLDER, e.details.name.c_str(), e.creator.display_name.c_str());
    return path;
}

auto apiBuildUrlPack(const PackListEntry& e) -> std::string {
    char url[2048];
    std::snprintf(url, sizeof(url), "https://api.themezer.net/?query=query($id:String!){pack(id:$id){id,creator{display_name},details{name,description},last_updated,categories,dl_count,like_count,themes{id,details{name},layout{id,details{name}},categories,target,preview{original,thumb},last_updated,dl_count,like_count}}}&variables={\"id\":\"%s\"}", e.id.c_str());
    return url;
}

auto apiBuildUrlThemeList(const Config& e) -> std::string {
    return apiBuildUrlListInternal(e, false);
}

auto apiBuildUrlListPacks(const Config& e) -> std::string {
    return apiBuildUrlListInternal(e, true);
}

auto apiBuildIconCache(const ThemeEntry& e) -> fs::FsPath {
    fs::FsPath path;
    std::snprintf(path, sizeof(path), "%s/%s_thumb.jpg", CACHE_PATH, e.id.c_str());
    return path;
}

auto loadThemeImage(ThemeEntry& e) -> void {
    auto& image = e.preview.lazy_image;

    // already have the image
    if (e.preview.lazy_image.image) {
        // log_write("warning, tried to load image: %s when already loaded\n", path.c_str());
        return;
    }
    auto vg = App::GetVg();

    fs::FsNativeSd fs;
    std::vector<u8> image_buf;

    const auto path = apiBuildIconCache(e);
    if (R_FAILED(fs.read_entire_file(path, image_buf))) {
        e.preview.lazy_image.state = ImageDownloadState::Failed;
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

void from_json(yyjson_val* json, Creator& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(id);
        JSON_SET_STR(display_name);
    );
}

void from_json(yyjson_val* json, Details& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(name);
        JSON_SET_STR(description);
    );
}

void from_json(yyjson_val* json, Preview& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(original);
        JSON_SET_STR(thumb);
    );
}

void from_json(yyjson_val* json, DownloadPack& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(filename);
        JSON_SET_STR(url);
        JSON_SET_STR(mimetype);
    );
}

void from_json(yyjson_val* json, ThemeEntry& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(id);
        JSON_SET_OBJ(creator);
        JSON_SET_OBJ(details);
        JSON_SET_STR(last_updated);
        JSON_SET_UINT(dl_count);
        JSON_SET_UINT(like_count);
        JSON_SET_ARR_STR(categories);
        JSON_SET_STR(target);
        JSON_SET_OBJ(preview);
    );
}

void from_json(yyjson_val* json, PackListEntry& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(id);
        JSON_SET_OBJ(creator);
        JSON_SET_OBJ(details);
        JSON_SET_STR(last_updated);
        JSON_SET_ARR_STR(categories);
        JSON_SET_UINT(dl_count);
        JSON_SET_UINT(like_count);
        JSON_SET_ARR_OBJ(themes);
    );
}

void from_json(yyjson_val* json, Pagination& e) {
    JSON_OBJ_ITR(
        JSON_SET_UINT(page);
        JSON_SET_UINT(limit);
        JSON_SET_UINT(page_count);
        JSON_SET_UINT(item_count);
    );
}

void from_json(const std::vector<u8>& data, DownloadPack& e) {
    JSON_INIT_VEC(data, "data");
    // JSON_GET_OBJ("downloadTheme");
    JSON_GET_OBJ("downloadPack");
    JSON_OBJ_ITR(
        JSON_SET_STR(filename);
        JSON_SET_STR(url);
        JSON_SET_STR(mimetype);
    );
}

void from_json(const std::vector<u8>& data, PackList& e) {
    JSON_INIT_VEC(data, "data");
    JSON_OBJ_ITR(
        JSON_SET_ARR_OBJ(packList);
        JSON_SET_OBJ(pagination);
    );
}

auto InstallTheme(ProgressBox* pbox, const PackListEntry& entry) -> bool {
    static fs::FsPath zip_out{"/switch/sphaira/cache/themezer/temp.zip"};
    constexpr auto chunk_size = 1024 * 512; // 512KiB

    fs::FsNativeSd fs;
    R_TRY_RESULT(fs.GetFsOpenResult(), false);

    DownloadPack download_pack;

    // 1. download the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Downloading "_i18n + entry.details.name);
        log_write("starting download\n");

        const auto url = apiBuildUrlDownloadPack(entry);
        log_write("using url: %s\n", url.c_str());
        DownloadClearCache(url);
        const auto data = DownloadMemory(url, "", [pbox](u32 dltotal, u32 dlnow, u32 ultotal, u32 ulnow){
            if (pbox->ShouldExit()) {
                return false;
            }
            pbox->UpdateTransfer(dlnow, dltotal);
            return true;
        });

        if (data.empty()) {
            log_write("error with download: %s\n", url.c_str());
            // push popup error box
            return false;
        }

        from_json(data, download_pack);
    }

    // 2. download the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Downloading "_i18n + entry.details.name);
        log_write("starting download: %s\n", download_pack.url.c_str());

        DownloadClearCache(download_pack.url);
        if (!DownloadFile(download_pack.url, zip_out, "", [pbox](u32 dltotal, u32 dlnow, u32 ultotal, u32 ulnow){
            if (pbox->ShouldExit()) {
                return false;
            }
            pbox->UpdateTransfer(dlnow, dltotal);
            return true;
        })) {
            log_write("error with download\n");
            // push popup error box
            return false;
        }
    }

    ON_SCOPE_EXIT(fs.DeleteFile(zip_out));

    // create directories
    fs::FsPath dir_path;
    std::snprintf(dir_path, sizeof(dir_path), "%s/%s - By %s", THEME_FOLDER, entry.details.name.c_str(), entry.creator.display_name.c_str());
    fs.CreateDirectoryRecursively(dir_path);

    // 3. extract the zip
    if (!pbox->ShouldExit()) {
        auto zfile = unzOpen64(zip_out);
        if (!zfile) {
            log_write("failed to open zip: %s\n", zip_out);
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
            char name[512];
            if (UNZ_OK != unzGetCurrentFileInfo64(zfile, &info, name, sizeof(name), 0, 0, 0, 0)) {
                log_write("failed to get current info\n");
                return false;
            }

            const auto file_path = fs::AppendPath(dir_path, name);

            Result rc;
            if (R_FAILED(rc = fs.CreateFile(file_path, info.uncompressed_size, 0)) && rc != FsError_ResultPathAlreadyExists) {
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
                if (pbox->ShouldExit()) {
                    return false;
                }

                const auto bytes_read = unzReadCurrentFile(zfile, buf.data(), buf.size());
                if (bytes_read <= 0) {
                    // log_write("failed to read zip file: %s\n", inzip.c_str());
                    return false;
                }

                if (R_FAILED(rc = fsFileWrite(&f, offset, buf.data(), bytes_read, FsWriteOption_None))) {
                    log_write("failed to write file: %s 0x%04X\n", file_path, rc);
                    return false;
                }

                pbox->UpdateTransfer(offset, info.uncompressed_size);
                offset += bytes_read;
            }
        }
    }

    log_write("finished install :)\n");
    return true;
}

} // namespace

LazyImage::~LazyImage() {
    if (image) {
        nvgDeleteImage(App::GetVg(), image);
    }
}

Menu::Menu() : MenuBase{"Themezer"_i18n} {
    SetAction(Button::B, Action{"Back"_i18n, [this]{
        SetPop();
    }});

    this->SetActions(
        std::make_pair(Button::RIGHT, Action{[this](){
            const auto& page = m_pages[m_page_index];
            if (m_index < (page.m_packList.size() - 1) && (m_index + 1) % 3 != 0) {
                SetIndex(m_index + 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                log_write("moved right\n");
            }
        }}),
        std::make_pair(Button::LEFT, Action{[this](){
            if (m_index != 0 && (m_index % 3) != 0) {
                SetIndex(m_index - 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                log_write("moved left\n");
            }
        }}),
        std::make_pair(Button::DOWN, Action{[this](){
            const auto& page = m_pages[m_page_index];
            if (ScrollHelperDown(m_index, m_start, 3, 6, page.m_packList.size())) {
                SetIndex(m_index);
            }
        }}),
        std::make_pair(Button::A, Action{"Download"_i18n, [this](){
            App::Push(std::make_shared<OptionBox>(
                "Download theme?"_i18n,
                "Back"_i18n, "Download"_i18n, 1, [this](auto op_index){
                    if (op_index && *op_index) {
                        const auto& page = m_pages[m_page_index];
                        if (page.m_packList.size() && page.m_ready == PageLoadState::Done) {
                            const auto& entry = page.m_packList[m_index];
                            const auto url = apiBuildUrlDownloadPack(entry);

                            App::Push(std::make_shared<ProgressBox>("Installing "_i18n + entry.details.name, [this, &entry](auto pbox){
                                return InstallTheme(pbox, entry);
                            }, [this](bool success){
                                // if (success) {
                                //     m_entry.status = EntryStatus::Installed;
                                //     m_menu.SetDirty();
                                //     UpdateOptions();
                                // }
                            }, 2));
                        }
                    }
                }
            ));
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("Themezer Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            SidebarEntryArray::Items sort_items;
            sort_items.push_back("Downloads"_i18n);
            sort_items.push_back("Updated"_i18n);
            sort_items.push_back("Likes"_i18n);
            sort_items.push_back("ID"_i18n);

            SidebarEntryArray::Items order_items;
            order_items.push_back("Descending (down)"_i18n);
            order_items.push_back("Ascending (Up)"_i18n);

            options->Add(std::make_shared<SidebarEntryBool>("Nsfw"_i18n, m_nsfw.Get(), [this](bool& v_out){
                m_nsfw.Set(v_out);
                InvalidateAllPages();
            }, "Enabled"_i18n, "Disabled"_i18n));

            options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this, sort_items](std::size_t& index_out){
                if (m_sort.Get() != index_out) {
                    m_sort.Set(index_out);
                    InvalidateAllPages();
                }
            }, m_sort.Get()));

            options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this, order_items](std::size_t& index_out){
                if (m_order.Get() != index_out) {
                    m_order.Set(index_out);
                    InvalidateAllPages();
                }
            }, m_order.Get()));

            options->Add(std::make_shared<SidebarEntryCallback>("Page"_i18n, [this](){
                s64 out;
                if (R_SUCCEEDED(swkbd::ShowNumPad(out, "Enter Page Number", nullptr, -1, 3))) {
                    if (out < m_page_index_max) {
                        m_page_index = out;
                        PackListDownload();
                    } else {
                        log_write("invalid page number\n");
                        App::Notify("Bad Page"_i18n);
                    }
                }
            }));

            options->Add(std::make_shared<SidebarEntryCallback>("Search"_i18n, [this](){
                std::string out;
                if (R_SUCCEEDED(swkbd::ShowText(out)) && !out.empty()) {
                    m_search = out;
                }
            }));
        }}),
        std::make_pair(Button::UP, Action{[this](){
            if (m_index >= 3) {
                SetIndex(m_index - 3);
                App::PlaySoundEffect(SoundEffect_Scroll);
                if (m_index < m_start ) {
                    // log_write("moved up\n");
                    m_start -= 3;
                }
            }
        }}),
        std::make_pair(Button::R, Action{"Next Page"_i18n, [this](){
            m_page_index++;
            if (m_page_index >= m_page_index_max) {
                m_page_index = m_page_index_max - 1;
            } else {
                PackListDownload();
            }
        }}),
        std::make_pair(Button::L, Action{"Prev Page"_i18n, [this](){
            if (m_page_index) {
                m_page_index--;
                PackListDownload();
            }
        }})
    );

    m_page_index = 0;
    m_pages.resize(1);
    PackListDownload();
}

Menu::~Menu() {

}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    if (m_pages.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, gfx::Colour::YELLOW, "Empty!");
        return;
    }

    auto& page = m_pages[m_page_index];

    switch (page.m_ready) {
        case PageLoadState::None:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, gfx::Colour::YELLOW, "Not Ready...");
            return;
        case PageLoadState::Loading:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, gfx::Colour::YELLOW, "Loading");
            return;
        case PageLoadState::Done:
            break;
        case PageLoadState::Error:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, gfx::Colour::YELLOW, "Error loading page!");
            return;
    }

    const u64 SCROLL = m_start;
    const u64 max_entry_display = 9;
    const u64 nro_total = page.m_packList.size();// m_entries_current.size();
    const u64 cursor_pos = m_index;

    // only draw scrollbar if needed
    if (nro_total > max_entry_display) {
        const auto scrollbar_size = 500.f;
        const auto sb_h = 3.f / (float)(nro_total + 3) * scrollbar_size;
        const auto sb_y = SCROLL / 3.f;
        gfx::drawRect(vg, SCREEN_WIDTH - 50, 100, 10, scrollbar_size, theme->elements[ThemeEntryID_GRID].colour);
        gfx::drawRect(vg, SCREEN_WIDTH - 50+2, 102 + sb_h * sb_y, 10-4, sb_h + (sb_h * 2) - 4, theme->elements[ThemeEntryID_TEXT_SELECTED].colour);
    }

    nvgSave(vg);
    nvgScissor(vg, 30, 87, 1220 - 30, 646 - 87); // clip

    for (u64 i = 0, pos = SCROLL, y = 110, w = 350, h = 250; pos < nro_total && i < max_entry_display; y += h + 10) {
        for (u64 j = 0, x = 75; j < 3 && pos < nro_total && i < max_entry_display; j++, i++, pos++, x += w + 10) {
            const auto index = pos;
            auto& e = page.m_packList[index];

            auto text_id = ThemeEntryID_TEXT;
            if (pos == cursor_pos) {
                text_id = ThemeEntryID_TEXT_SELECTED;
                gfx::drawRectOutline(vg, 4.f, theme->elements[ThemeEntryID_SELECTED_OVERLAY].colour, x, y, w, h, theme->elements[ThemeEntryID_SELECTED].colour);
            } else {
                DrawElement(x, y, w, h, ThemeEntryID_GRID);
            }

            const float xoff = (350 - 320) / 2;
            const float yoff = (350 - 320) / 2;

            // lazy load image
            if (e.themes.size()) {
                auto& theme = e.themes[0];
                auto& image = e.themes[0].preview.lazy_image;
                if (!image.image) {
                    switch (image.state) {
                        case ImageDownloadState::None: {
                            const auto path = apiBuildIconCache(theme);
                            log_write("downloading theme!: %s\n", path);

                            if (fs::FsNativeSd().FileExists(path)) {
                                loadThemeImage(theme);
                            } else {
                                const auto url = theme.preview.thumb;
                                log_write("downloading url: %s\n", url.c_str());
                                image.state = ImageDownloadState::Progress;
                                DownloadFileAsync(url, path, "", [this, index, &image](std::vector<u8>& data, bool success) {
                                    if (success) {
                                        image.state = ImageDownloadState::Done;
                                        log_write("downloaded themezer image\n");
                                    } else {
                                        image.state = ImageDownloadState::Failed;
                                        log_write("failed to download image\n");
                                    }
                                }, nullptr, DownloadPriority::High);
                            }
                        }   break;
                        case ImageDownloadState::Progress: {

                        }   break;
                        case ImageDownloadState::Done: {
                            loadThemeImage(theme);
                        }   break;
                        case ImageDownloadState::Failed: {
                        }   break;
                    }
                } else {
                    gfx::drawImageRounded(vg, x + xoff, y, 320, 180, image.image);
                }
            }

            gfx::drawTextArgs(vg, x + xoff, y + 180 + 20, 18, NVG_ALIGN_LEFT, theme->elements[text_id].colour, "%s", e.details.name.c_str());
            gfx::drawTextArgs(vg, x + xoff, y + 180 + 55, 18, NVG_ALIGN_LEFT, theme->elements[text_id].colour, "%s", e.creator.display_name.c_str());
        }
    }

    nvgRestore(vg);
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
}

void Menu::InvalidateAllPages() {
    for (auto& e : m_pages) {
        e.m_packList.clear();
        e.m_ready = PageLoadState::None;
    }

    PackListDownload();
}

void Menu::PackListDownload() {
    const auto page_index = m_page_index + 1;
    char subheading[128];
    std::snprintf(subheading, sizeof(subheading), "Page %zu / %zu", m_page_index+1, m_page_index_max);
    SetSubHeading(subheading);

    m_index = 0;
    m_start = 0;

    // already downloaded
    if (m_pages[m_page_index].m_ready != PageLoadState::None) {
        return;
    }
    m_pages[m_page_index].m_ready = PageLoadState::Loading;

    Config config;
    config.page = page_index;
    config.SetQuery(m_search);
    config.sort_index = m_sort.Get();
    config.order_index = m_order.Get();
    config.nsfw = m_nsfw.Get();
    const auto packList_url = apiBuildUrlListPacks(config);
    const auto themeList_url = apiBuildUrlThemeList(config);

    log_write("\npackList_url: %s\n\n", packList_url.c_str());
    log_write("\nthemeList_url: %s\n\n", themeList_url.c_str());

    DownloadClearCache(packList_url);
    DownloadMemoryAsync(packList_url, "", [this, page_index](std::vector<u8>& data, bool success){
        log_write("got themezer data\n");
        if (!success) {
            auto& page = m_pages[page_index-1];
            page.m_ready = PageLoadState::Error;
            log_write("failed to get themezer data...\n");
            return;
        }

        PackList a;
        from_json(data, a);

        m_pages.resize(a.pagination.page_count);
        auto& page = m_pages[page_index-1];

        page.m_packList = a.packList;
        page.m_pagination = a.pagination;
        page.m_ready = PageLoadState::Done;
        m_page_index_max = a.pagination.page_count;

        char subheading[128];
        std::snprintf(subheading, sizeof(subheading), "Page %zu / %zu", m_page_index+1, m_page_index_max);
        SetSubHeading(subheading);

        log_write("a.pagination.page: %u\n", a.pagination.page);
        log_write("a.pagination.page_count: %u\n", a.pagination.page_count);
    }, nullptr, DownloadPriority::High);
}

} // namespace sphaira::ui::menu::themezer
