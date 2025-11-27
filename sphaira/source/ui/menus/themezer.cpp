#if 0
#include "ui/menus/themezer.hpp"
#include "ui/menus/ghdl.hpp"
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
#include "threaded_file_transfer.hpp"
#include "image.hpp"
#include "title_info.hpp"
#include "nro.hpp"

#include <minIni.h>
#include <stb_image.h>
#include <cstring>
#include <yyjson.h>
#include "yyjson_helper.hpp"

namespace sphaira::ui::menu::themezer {
namespace {

// format is /themes/sphaira/Theme Name by Author/theme_name-type.nxtheme
constexpr fs::FsPath THEME_FOLDER{"/themes/sphaira/"};
constexpr auto CACHE_PATH = "/switch/sphaira/cache/themezer";
constexpr auto URL_BASE = "https://switch.cdn.fortheusers.org";

constexpr const char* NRO_URL = "https://github.com/exelix11/SwitchThemeInjector";

constexpr const char* NRO_PATHS[]{
    "/switch/NXThemesInstaller.nro",
    "/switch/Switch_themes_Installer/NXThemesInstaller.nro",
};

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

auto GetNroPath() -> const char* {
    fs::FsNativeSd fs;
    for (auto& path : NRO_PATHS) {
        if (fs.FileExists(path)) {
            return path;
        }
    }

    return nullptr;
}

auto HasNro() -> bool {
    return GetNroPath() != nullptr;
}

// i know, this is cursed
// todo: send actual POST request rather than GET.
auto apiBuildUrlListInternal(const Config& e, bool is_pack) -> std::string {
    std::string api = "https://api.themezer.net/?query=query";
    // std::string fields = "{id,creator{id,display_name},details{name,description},last_updated,dl_count,like_count";
    std::string fields = "{id,creator{id,display_name},details{name}";
    const char* boolarr[2] = { "false", "true" };

    std::string cmd;
    std::string p0 = "$nsfw:Boolean,$page:Int,$limit:Int,$sort:String,$order:String";
    std::string p1 = "nsfw:$nsfw,page:$page,limit:$limit,sort:$sort,order:$order";
    std::string json = "\"nsfw\":"+std::string{boolarr[e.nsfw]}+",\"page\":"+std::to_string(e.page)+",\"limit\":"+std::to_string(e.limit)+",\"sort\":\""+std::string{REQUEST_SORT[e.sort_index]}+"\",\"order\":\""+std::string{REQUEST_ORDER[e.order_index]}+"\"";

    if (is_pack) {
        cmd = "packList";
        // fields += ",themes{id,creator{display_name},details{name,description},last_updated,dl_count,like_count,target,preview{original,thumb}}";
        fields += ",themes{id,preview{thumb}}";
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

    json = curl::EscapeString('{'+json+'}');

    return api+"("+p0+"){"+cmd+"("+p1+")"+fields+"}}&variables="+json;
}

auto apiBuildUrlDownloadInternal(const std::string& id, bool is_pack) -> std::string {
    char url[2048];
    std::snprintf(url, sizeof(url), "https://api.themezer.net/?query=query{download%s(id:\"%s\"){filename,url,mimetype}}", is_pack ? "Pack" : "Theme", id.c_str());
    return url;
    // https://api.themezer.net/?query=query{downloadPack(id:"11"){filename,url,mimetype}}
}

auto apiBuildUrlDownloadPack(const PackListEntry& e) -> std::string {
    return apiBuildUrlDownloadInternal(e.id, true);
}

auto apiBuildUrlListPacks(const Config& e) -> std::string {
    return apiBuildUrlListInternal(e, true);
}

auto apiBuildListPacksCache(const Config& e) -> fs::FsPath {
    fs::FsPath path;
    std::snprintf(path, sizeof(path), "%s/%u_page.json", CACHE_PATH, e.page);
    return path;
}

auto apiBuildIconCache(const ThemeEntry& e) -> fs::FsPath {
    fs::FsPath path;
    std::snprintf(path, sizeof(path), "%s/%s_thumb.jpg", CACHE_PATH, e.id.c_str());
    return path;
}

auto loadThemeImage(ThemeEntry& e) -> bool {
    auto& image = e.preview.lazy_image;

    // already have the image
    if (e.preview.lazy_image.image) {
        // log_write("warning, tried to load image: %s when already loaded\n", path.c_str());
        return true;
    }
    auto vg = App::GetVg();

    const auto path = apiBuildIconCache(e);
    TimeStamp ts;
    const auto data = ImageLoadFromFile(path, ImageFlag_JPEG);
    if (!data.data.empty()) {
        image.w = data.w;
        image.h = data.h;
        image.image = nvgCreateImageRGBA(vg, data.w, data.h, 0, data.data.data());
        log_write("\t[image load] time taken: %.2fs %zums\n", ts.GetSecondsD(), ts.GetMs());
    }

    if (!image.image) {
        log_write("failed to load image from file: %s\n", path.s);
        return false;
    } else {
        // log_write("loaded image from file: %s\n", path);
        return true;
    }
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
    );
}

void from_json(yyjson_val* json, Preview& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(thumb);
    );
}

void from_json(yyjson_val* json, ThemeEntry& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(id);
        JSON_SET_OBJ(preview);
    );
}

void from_json(yyjson_val* json, PackListEntry& e) {
    JSON_OBJ_ITR(
        JSON_SET_STR(id);
        JSON_SET_OBJ(creator);
        JSON_SET_OBJ(details);
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
    JSON_GET_OBJ("downloadPack");
    JSON_OBJ_ITR(
        JSON_SET_STR(filename);
        JSON_SET_STR(url);
        JSON_SET_STR(mimetype);
    );
}

void from_json(const fs::FsPath& path, PackList& e) {
    JSON_INIT_VEC_FILE(path, "data", nullptr);
    JSON_OBJ_ITR(
        JSON_SET_ARR_OBJ(packList);
        JSON_SET_OBJ(pagination);
    );
}

auto InstallTheme(ProgressBox* pbox, const PackListEntry& entry) -> Result {
    static const fs::FsPath zip_out{"/switch/sphaira/cache/themezer/temp.zip"};

    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    DownloadPack download_pack;

    // 1. download the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer(i18n::Reorder("Downloading ", entry.details.name));
        log_write("starting download\n");

        const auto url = apiBuildUrlDownloadPack(entry);
        log_write("using url: %s\n", url.c_str());
        const auto result = curl::Api().ToMemory(
            curl::Url{url},
            curl::OnProgress{pbox->OnDownloadProgressCallback()}
        );

        if (!result.success || result.data.empty()) {
            log_write("error with download: %s\n", url.c_str());
            R_THROW(Result_ThemezerFailedToDownloadThemeMeta);
        }

        from_json(result.data, download_pack);
    }

    // 2. download the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer(i18n::Reorder("Downloading ", entry.details.name));
        log_write("starting download: %s\n", download_pack.url.c_str());

        const auto result = curl::Api().ToFile(
            curl::Url{download_pack.url},
            curl::Path{zip_out},
            curl::OnProgress{pbox->OnDownloadProgressCallback()}
        );

        R_UNLESS(result.success, Result_ThemezerFailedToDownloadTheme);
    }

    ON_SCOPE_EXIT(fs.DeleteFile(zip_out));

    // replace invalid characters in the name.
    fs::FsPath name_buf{entry.details.name};
    title::utilsReplaceIllegalCharacters(name_buf, false);

    // replace invalid characters in the author.
    fs::FsPath author_buf{entry.creator.display_name};
    title::utilsReplaceIllegalCharacters(author_buf, false);

    // create directories.
    fs::FsPath dir_path;
    std::snprintf(dir_path, sizeof(dir_path), "%s/%s - By %s", THEME_FOLDER.s, name_buf.s, author_buf.s);
    fs.CreateDirectoryRecursively(dir_path);

    // 3. extract the zip
    std::vector<std::string> nxtheme_paths;
    if (!pbox->ShouldExit()) {
        R_TRY(thread::TransferUnzipAll(pbox, zip_out, &fs, dir_path, [&nxtheme_paths](const fs::FsPath& name, fs::FsPath& path){
            // just in case theme packs start adding invalid entries.
            if (!path.ends_with(".nxtheme")) {
                return false;
            }

            // store path for later.
            nxtheme_paths.emplace_back(path);
            return true;
        }));
    }

    // ensure that we actually downloaded the theme.
    // todo: add new error for this.
    R_UNLESS(!nxtheme_paths.empty(), Result_ThemezerFailedToDownloadTheme);

    // if we have nxtheme installed, prompt the user to install the theme now.
    if (HasNro()) {
        App::Push<OptionBox>(
            "Theme downloaded, install now?"_i18n,
            "Back"_i18n, "Install"_i18n, 1, [nxtheme_paths](auto op_index){
                if (op_index && *op_index) {
                    std::string args;

                    for (const auto& paths : nxtheme_paths) {
                        // add space between each arg.
                        if (!args.empty()) {
                            args += ' ';
                        }

                        // converts path to sdmc:/path.
                        args += nro_add_arg_file(paths);
                    }

                    log_write("themezer nro: %s\n", GetNroPath());
                    log_write("themezer args: %s\n", args.c_str());

                    // launch nro with args to the nxthemes.
                    const auto rc = nro_launch(GetNroPath(), args);
                    App::PushErrorBox(rc, "Failed to launch NXthemes_Installer.nro"_i18n);
                }
            }
        );
    }

    log_write("finished install :)\n");
    R_SUCCEED();
}

} // namespace

LazyImage::~LazyImage() {
    if (image) {
        nvgDeleteImage(App::GetVg(), image);
    }
}

Menu::Menu(u32 flags) : MenuBase{"Themezer"_i18n, flags} {
    fs::FsNativeSd().CreateDirectoryRecursively(CACHE_PATH);

    SetAction(Button::B, Action{"Back"_i18n, [this]{
        // if search is valid, then we are in search mode, return back to normal.
        if (!m_search.empty()) {
            m_search.clear();
            InvalidateAllPages();
        } else {
            SetPop();
        }
    }});

    this->SetActions(
        std::make_pair(Button::A, Action{"Download"_i18n, [this](){
            App::Push<OptionBox>(
                "Download theme?"_i18n,
                "Back"_i18n, "Download"_i18n, 1, [this](auto op_index){
                    if (op_index && *op_index) {
                        const auto& page = m_pages[m_page_index];
                        if (page.m_packList.size() && page.m_ready == PageLoadState::Done) {
                            const auto& entry = page.m_packList[m_index];
                            const auto url = apiBuildUrlDownloadPack(entry);

                            App::Push<ProgressBox>(entry.themes[0].preview.lazy_image.image, i18n::Reorder("Downloading ", entry.details.name), [this, &entry](auto pbox) -> Result {
                                return InstallTheme(pbox, entry);
                            }, [this, &entry](Result rc){
                                App::PushErrorBox(rc, "Failed to download theme"_i18n);

                                if (R_SUCCEEDED(rc)) {
                                    App::Notify(i18n::Reorder("Downloaded ", entry.details.name));
                                }
                            });
                        }
                    }
                }
            );
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            DisplayOptions();
        }}),
        std::make_pair(Button::R2, Action{"Next"_i18n, [this](){
            m_page_index++;
            if (m_page_index >= m_page_index_max) {
                m_page_index = m_page_index_max - 1;
            } else {
                PackListDownload();
            }
        }}),
        std::make_pair(Button::L2, Action{"Prev"_i18n, [this](){
            if (m_page_index) {
                m_page_index--;
                PackListDownload();
            }
        }})
    );

    const Vec4 v{75, 110, 350, 250};
    const Vec2 pad{10, 10};
    m_list = std::make_unique<List>(3, 6, m_pos, v, pad);

    m_page_index = 0;
    m_pages.resize(1);
    PackListDownload();
}

Menu::~Menu() {

}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);

    if (m_pages.empty()) {
        return;
    }

    const auto& page = m_pages[m_page_index];
    if (page.m_ready != PageLoadState::Done) {
        return;
    }

    m_list->OnUpdate(controller, touch, m_index, page.m_packList.size(), [this](bool touch, auto i) {
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

    if (m_pages.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Empty!"_i18n.c_str());
        return;
    }

    auto& page = m_pages[m_page_index];

    switch (page.m_ready) {
        case PageLoadState::None:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Not Ready..."_i18n.c_str());
            return;
        case PageLoadState::Loading:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Loading"_i18n.c_str());
            return;
        case PageLoadState::Done:
            break;
        case PageLoadState::Error:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Error loading page!"_i18n.c_str());
            return;
    }

    // max images per frame, in order to not hit io / gpu too hard.
    const int image_load_max = 2;
    int image_load_count = 0;

    m_list->Draw(vg, theme, page.m_packList.size(), [this, &page, &image_load_count](auto* vg, auto* theme, auto v, auto pos) {
        const auto& [x, y, w, h] = v;
        auto& e = page.m_packList[pos];

        auto text_id = ThemeEntryID_TEXT;
        const auto selected = pos == m_index;
        if (selected) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, theme, 4.f, v);
        } else {
            DrawElement(x, y, w, h, ThemeEntryID_GRID);
        }

        const float xoff = (350 - 320) / 2;

        // lazy load image
        if (e.themes.size()) {
            auto& theme = e.themes[0];
            auto& image = e.themes[0].preview.lazy_image;

            // try and load cached image.
            if (image_load_count < image_load_max && !image.image && !image.tried_cache) {
                image.tried_cache = true;
                image.cached = loadThemeImage(theme);
                if (image.cached) {
                    image_load_count++;
                }
            }

            if (!image.image || image.cached) {
                switch (image.state) {
                    case ImageDownloadState::None: {
                        const auto path = apiBuildIconCache(theme);
                        log_write("downloading theme!: %s\n", path.s);

                        const auto url = theme.preview.thumb;
                        log_write("downloading url: %s\n", url.c_str());
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
                        image.cached = false;
                        if (!loadThemeImage(theme)) {
                            image.state = ImageDownloadState::Failed;
                        } else {
                            image_load_count++;
                        }
                    }   break;
                    case ImageDownloadState::Failed: {
                    }   break;
                }
            }

            gfx::drawImage(vg, x + xoff, y, 320, 180, image.image ? image.image : App::GetDefaultImage(), 5);
        }

        const auto text_x = x + xoff;
        const auto text_clip_w = w - 30.f - xoff;
        const float font_size = 18;
        m_scroll_name.Draw(vg, selected, text_x, y + 180 + 20, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.details.name.c_str());
        m_scroll_author.Draw(vg, selected, text_x, y + 180 + 55, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.creator.display_name.c_str());
    });
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();

    if (!m_checked_for_nro) {
        m_checked_for_nro = true;

        // check if we have the nro, if not, then prompt the user to download from the appstore.
        if (!HasNro()) {
            App::Push<OptionBox>(
                "NXthemes_Installer.nro not found, download now?"_i18n,
                "Back"_i18n, "Download"_i18n, 1, [this](auto op_index){
                    if (op_index && *op_index) {
                        const gh::AssetEntry asset{
                            .name = "NXThemesInstaller.nro",
                            // same path as appstore
                            .path = "/switch/Switch_themes_Installer/NXThemesInstaller.nro",
                        };

                        gh::Download(NRO_URL, asset, "latest");
                    }
                }
            );
        }
    }
}

void Menu::InvalidateAllPages() {
    m_pages.clear();
    m_pages.resize(1);
    m_page_index = 0;
    PackListDownload();
}

void Menu::PackListDownload() {
    const auto page_index = m_page_index + 1;
    char subheading[128];
    std::snprintf(subheading, sizeof(subheading), "Page %zu / %zu"_i18n.c_str(), m_page_index+1, m_page_index_max);
    SetSubHeading(subheading);

    m_index = 0;
    m_list->SetYoff(0);

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
    const auto packlist_path = apiBuildListPacksCache(config);

    log_write("\npackList_url: %s\n\n", packList_url.c_str());

    curl::Api().ToFileAsync(
        curl::Url{packList_url},
        curl::Path{packlist_path},
        curl::Flags{curl::Flag_Cache},
        curl::StopToken{this->GetToken()},
        curl::OnComplete{[this, page_index](auto& result){
            App::SetBoostMode(true);
            ON_SCOPE_EXIT(App::SetBoostMode(false));

            log_write("got themezer data\n");
            if (!result.success) {
                auto& page = m_pages[page_index-1];
                page.m_ready = PageLoadState::Error;
                log_write("failed to get themezer data...\n");
                return;
            }

            PackList a;
            from_json(result.path, a);

            m_pages.resize(a.pagination.page_count);
            auto& page = m_pages[page_index-1];

            page.m_packList = a.packList;
            page.m_pagination = a.pagination;
            page.m_ready = PageLoadState::Done;
            m_page_index_max = a.pagination.page_count;

            char subheading[128];
            std::snprintf(subheading, sizeof(subheading), "Page %zu / %zu"_i18n.c_str(), m_page_index+1, m_page_index_max);
            SetSubHeading(subheading);

            log_write("a.pagination.page: %zu\n", a.pagination.page);
            log_write("a.pagination.page_count: %zu\n", a.pagination.page_count);
        }
    });
}

void Menu::DisplayOptions() {
    auto options = std::make_unique<Sidebar>("Themezer Options"_i18n, Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(std::move(options)));

    SidebarEntryArray::Items sort_items;
    sort_items.push_back("Downloads"_i18n);
    sort_items.push_back("Updated"_i18n);
    sort_items.push_back("Likes"_i18n);
    sort_items.push_back("ID"_i18n);

    SidebarEntryArray::Items order_items;
    order_items.push_back("Descending (down)"_i18n);
    order_items.push_back("Ascending (Up)"_i18n);

    options->Add<SidebarEntryBool>("Nsfw"_i18n, m_nsfw.Get(), [this](bool& v_out){
        m_nsfw.Set(v_out);
        InvalidateAllPages();
    });

    options->Add<SidebarEntryArray>("Sort"_i18n, sort_items, [this, sort_items](s64& index_out){
        if (m_sort.Get() != index_out) {
            m_sort.Set(index_out);
            InvalidateAllPages();
        }
    }, m_sort.Get());

    options->Add<SidebarEntryArray>("Order"_i18n, order_items, [this, order_items](s64& index_out){
        if (m_order.Get() != index_out) {
            m_order.Set(index_out);
            InvalidateAllPages();
        }
    }, m_order.Get());

    options->Add<SidebarEntryCallback>("Page"_i18n, [this](){
        s64 out;
        if (R_SUCCEEDED(swkbd::ShowNumPad(out, "Enter Page Number"_i18n.c_str(), nullptr, -1, 3))) {
            if (out < m_page_index_max) {
                m_page_index = out;
                PackListDownload();
            } else {
                log_write("invalid page number\n");
                App::Notify("Bad Page"_i18n);
            }
        }
    });

    options->Add<SidebarEntryCallback>("Search"_i18n, [this](){
        std::string out;
        if (R_SUCCEEDED(swkbd::ShowText(out)) && !out.empty()) {
            m_search = out;
            // PackListDownload();
            InvalidateAllPages();
        }
    });

    if (HasNro()) {
        options->Add<SidebarEntryCallback>("Launch NXthemes_Installer.nro"_i18n, [](){
            const auto rc = nro_launch(GetNroPath());
            App::PushErrorBox(rc, "Failed to launch NXthemes_Installer.nro"_i18n);
        });
    }
}

} // namespace sphaira::ui::menu::themezer
#endif
