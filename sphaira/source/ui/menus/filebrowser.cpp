#include "ui/menus/filebrowser.hpp"
#include "ui/menus/homebrew.hpp"
#include "ui/sidebar.hpp"
#include "ui/option_box.hpp"
#include "ui/popup_list.hpp"
#include "ui/progress_box.hpp"
#include "ui/error_box.hpp"
#include "ui/menus/file_viewer.hpp"

#include "log.hpp"
#include "app.hpp"
#include "ui/nvg_util.hpp"
#include "fs.hpp"
#include "nro.hpp"
#include "defines.hpp"
#include "image.hpp"
#include "download.hpp"
#include "owo.hpp"
#include "swkbd.hpp"
#include "i18n.hpp"
#include "hasher.hpp"
#include "location.hpp"
#include "threaded_file_transfer.hpp"

#include "yati/yati.hpp"
#include "yati/source/file.hpp"

#include <minIni.h>
#include <minizip/zip.h>
#include <minizip/unzip.h>
#include <dirent.h>
#include <cstring>
#include <cassert>
#include <string>
#include <string_view>
#include <ctime>
#include <span>
#include <utility>
#include <ranges>
// #include <stack>
#include <expected>

namespace sphaira::ui::menu::filebrowser {
namespace {

constexpr FsEntry FS_ENTRY_DEFAULT{
    "microSD card", "/", FsType::Sd, FsEntryFlag_Assoc,
};

constexpr FsEntry FS_ENTRIES[]{
    FS_ENTRY_DEFAULT,
    { "Image System memory", "/", FsType::ImageNand },
    { "Image microSD card", "/", FsType::ImageSd},
};

struct ExtDbEntry {
    std::string_view db_name;
    std::span<const std::string_view> ext;
};

constexpr std::string_view AUDIO_EXTENSIONS[] = {
    "mp3", "ogg", "flac", "wav", "aac" "ac3", "aif", "asf", "bfwav",
    "bfsar", "bfstm",
};
constexpr std::string_view VIDEO_EXTENSIONS[] = {
    "mp4", "mkv", "m3u", "m3u8", "hls", "vob", "avi", "dv", "flv", "m2ts",
    "m2v", "m4a", "mov", "mpeg", "mpg", "mts", "swf", "ts", "vob", "wma", "wmv",
};
constexpr std::string_view IMAGE_EXTENSIONS[] = {
    "png", "jpg", "jpeg", "bmp", "gif",
};
constexpr std::string_view INSTALL_EXTENSIONS[] = {
    "nsp", "xci", "nsz", "xcz",
};
// these are files that are already compressed or encrypted and should
// be stored raw in a zip file.
constexpr std::string_view COMPRESSED_EXTENSIONS[] = {
    "zip", "xz", "7z", "rar", "tar", "nca", "nsp", "xci", "nsz", "xcz"
};
constexpr std::string_view ZIP_EXTENSIONS[] = {
    "zip",
};


struct RomDatabaseEntry {
    // uses the naming scheme from retropie.
    std::string_view folder{};
    // uses the naming scheme from Retroarch.
    std::string_view database{};
    // custom alias, to make everyone else happy.
    std::array<std::string_view, 4> alias{};

    // compares against all of the above strings.
    auto IsDatabase(std::string_view name) const {
        if (name == folder || name == database) {
            return true;
        }

        for (const auto& str : alias) {
            if (!str.empty() && name == str) {
                return true;
            }
        }

        return false;
    }
};

constexpr RomDatabaseEntry PATHS[]{
    { "3do", "The 3DO Company - 3DO"},
    { "atari800", "Atari - 8-bit"},
    { "atari2600", "Atari - 2600"},
    { "atari5200", "Atari - 5200"},
    { "atari7800", "Atari - 7800"},
    { "atarilynx", "Atari - Lynx"},
    { "atarijaguar", "Atari - Jaguar"},
    { "atarijaguarcd", ""},
    { "n3ds", "Nintendo - Nintendo 3DS"},
    { "n64", "Nintendo - Nintendo 64"},
    { "nds", "Nintendo - Nintendo DS"},
    { "fds", "Nintendo - Famicom Disk System"},
    { "nes", "Nintendo - Nintendo Entertainment System"},
    { "pokemini", "Nintendo - Pokemon Mini"},
    { "gb", "Nintendo - Game Boy"},
    { "gba", "Nintendo - Game Boy Advance"},
    { "gbc", "Nintendo - Game Boy Color"},
    { "virtualboy", "Nintendo - Virtual Boy"},
    { "gameandwatch", ""},
    { "sega32x", "Sega - 32X"},
    { "segacd", "Sega - Mega CD - Sega CD"},
    { "dreamcast", "Sega - Dreamcast"},
    { "gamegear", "Sega - Game Gear"},
    { "genesis", "Sega - Mega Drive - Genesis"},
    { "mastersystem", "Sega - Master System - Mark III"},
    { "megadrive", "Sega - Mega Drive - Genesis"},
    { "saturn", "Sega - Saturn"},
    { "sg-1000", "Sega - SG-1000"},
    { "psx", "Sony - PlayStation"},
    { "psp", "Sony - PlayStation Portable"},
    { "snes", "Nintendo - Super Nintendo Entertainment System"},
    { "pico8", "Sega - PICO"},
    { "wonderswan", "Bandai - WonderSwan"},
    { "wonderswancolor", "Bandai - WonderSwan Color"},

    { "mame", "MAME 2000", { "MAME", "mame-libretro", } },
    { "mame", "MAME 2003", { "MAME", "mame-libretro", } },
    { "mame", "MAME 2003-Plus", { "MAME", "mame-libretro", } },

    { "neogeo", "SNK - Neo Geo Pocket" },
    { "neogeo", "SNK - Neo Geo Pocket Color" },
    { "neogeo", "SNK - Neo Geo CD" },
};

constexpr fs::FsPath DAYBREAK_PATH{"/switch/daybreak.nro"};

auto IsExtension(std::string_view ext, std::span<const std::string_view> list) -> bool {
    for (auto e : list) {
        if (e.length() == ext.length() && !strncasecmp(ext.data(), e.data(), ext.length())) {
            return true;
        }
    }
    return false;
}

auto IsExtension(std::string_view ext1, std::string_view ext2) -> bool {
    return ext1.length() == ext2.length() && !strncasecmp(ext1.data(), ext2.data(), ext1.length());
}

// tries to find database path using folder name
// names are taken from retropie
// retroarch database names can also be used
using RomDatabaseIndexs = std::vector<size_t>;
auto GetRomDatabaseFromPath(std::string_view path) -> RomDatabaseIndexs {
    if (path.length() <= 1) {
        return {};
    }

    // this won't fail :)
    RomDatabaseIndexs indexs;
    const auto db_name = path.substr(path.find_last_of('/') + 1);
    // log_write("new path: %s\n", db_name.c_str());

    for (int i = 0; i < std::size(PATHS); i++) {
        const auto& p = PATHS[i];
        if (p.IsDatabase(db_name)) {
            log_write("found it :) %.*s\n", (int)p.database.length(), p.database.data());
            indexs.emplace_back(i);
        }
    }

    // if we failed, try again but with the folder about
    // "/roms/psx/scooby-doo/scooby-doo.bin", this will check psx
    if (indexs.empty()) {
        const auto last_off = path.substr(0, path.find_last_of('/'));
        if (const auto off = last_off.find_last_of('/'); off != std::string_view::npos) {
            const auto db_name2 = last_off.substr(off + 1);
            // printf("got db: %s\n", db_name2.c_str());
            for (int i = 0; i < std::size(PATHS); i++) {
                const auto& p = PATHS[i];
                if (p.IsDatabase(db_name2)) {
                    log_write("found it :) %.*s\n", (int)p.database.length(), p.database.data());
                    indexs.emplace_back(i);
                }
            }
        }
    }

    return indexs;
}

//
auto GetRomIcon(fs::Fs* fs, ProgressBox* pbox, std::string filename, const RomDatabaseIndexs& db_indexs, const NroEntry& nro) {
    // if no db entries, use nro icon
    if (db_indexs.empty()) {
        log_write("using nro image\n");
        return nro_get_icon(nro.path, nro.icon_size, nro.icon_offset);
    }

    // fix path to be url friendly
    constexpr std::string_view bad_chars{"&*/:`<>?\\|\""};
    for (auto& c : filename) {
        for (auto bad_c : bad_chars) {
            if (c == bad_c) {
                c = '_';
                break;
            }
        }
    }

    #define RA_BOXART_URL "https://thumbnails.libretro.com/"
    #define GH_BOXART_URL "https://raw.githubusercontent.com/libretro-thumbnails/"
    #define RA_BOXART_NAME "/Named_Boxarts/"
    #define RA_THUMBNAIL_PATH "/retroarch/thumbnails/"
    #define RA_BOXART_EXT ".png"

    for (auto db_idx : db_indexs) {
        const auto system_name = std::string{PATHS[db_idx].database.data(), PATHS[db_idx].database.length()};//GetDatabaseFromExt(database, extension);
        auto system_name_gh = system_name + "/master";
        for (auto& c : system_name_gh) {
            if (c == ' ') {
                c = '_';
            }
        }

        std::string filename_gh;
        filename_gh.reserve(filename.size());
        for (auto c : filename) {
            if (c == ' ') {
                filename_gh += "%20";
            } else {
                filename_gh.push_back(c);
            }
        }

        const std::string thumbnail_path = system_name + RA_BOXART_NAME + filename + RA_BOXART_EXT;
        const std::string ra_thumbnail_path = RA_THUMBNAIL_PATH + thumbnail_path;
        const std::string ra_thumbnail_url = RA_BOXART_URL + thumbnail_path;
        const std::string gh_thumbnail_url = GH_BOXART_URL + system_name_gh + RA_BOXART_NAME + filename_gh + RA_BOXART_EXT;

        log_write("starting image convert on: %s\n", ra_thumbnail_path.c_str());
        // try and find icon locally
        if (!pbox->ShouldExit()) {
            pbox->NewTransfer("Trying to load "_i18n + ra_thumbnail_path);
            std::vector<u8> image_file;
            if (R_SUCCEEDED(fs->read_entire_file(ra_thumbnail_path.c_str(), image_file))) {
                return image_file;
            }
        }

        // try and download icon
        if (!pbox->ShouldExit()) {
            pbox->NewTransfer("Downloading "_i18n + gh_thumbnail_url);
            const auto result = curl::Api().ToMemory(
                curl::Url{gh_thumbnail_url},
                curl::OnProgress{pbox->OnDownloadProgressCallback()}
            );

            if (result.success && !result.data.empty()) {
                return result.data;
            }
        }
    }

    // use nro icon
    log_write("using nro image\n");
    return nro_get_icon(nro.path, nro.icon_size, nro.icon_offset);
}

} // namespace

FsView::FsView(Menu* menu, const fs::FsPath& path, const FsEntry& entry, ViewSide side) : m_menu{menu}, m_side{side} {
    this->SetActions(
        std::make_pair(Button::L2, Action{[this](){
            if (!m_menu->m_selected.Empty()) {
                m_menu->ResetSelection();
            }

            // if both set, select all.
            if (App::GetApp()->m_controller.GotHeld(Button::R2)) {
                const auto set = m_selected_count != m_entries_current.size();

                for (u32 i = 0; i < m_entries_current.size(); i++) {
                    auto& e = GetEntry(i);
                    if (e.selected != set) {
                        e.selected = set;
                        if (set) {
                            m_selected_count++;
                        } else {
                            m_selected_count--;
                        }
                    }
                }
            } else {
                GetEntry().selected ^= 1;
                if (GetEntry().selected) {
                    m_selected_count++;
                } else {
                    m_selected_count--;
                }
            }
        }}),
        std::make_pair(Button::A, Action{"Open"_i18n, [this](){
            if (m_entries_current.empty()) {
                return;
            }

            if (IsSd() && m_is_update_folder && m_daybreak_path.has_value()) {
                App::Push(std::make_shared<OptionBox>("Open with DayBreak?"_i18n, "No"_i18n, "Yes"_i18n, 1, [this](auto op_index){
                    if (op_index && *op_index) {
                        // daybreak uses native fs so do not use nro_add_arg_file
                        // otherwise it'll fail to open the folder...
                        nro_launch(m_daybreak_path.value(), nro_add_arg(m_path));
                    }
                }));
                return;
            }

            const auto& entry = GetEntry();

            if (entry.type == FsDirEntryType_Dir) {
                Scan(GetNewPathCurrent());
            } else {
                // special case for nro
                if (IsSd() && entry.GetExtension() == "nro") {
                    App::Push(std::make_shared<OptionBox>("Launch "_i18n + entry.GetName() + '?',
                        "No"_i18n, "Launch"_i18n, 1, [this](auto op_index){
                            if (op_index && *op_index) {
                                nro_launch(GetNewPathCurrent());
                            }
                        }));
                } else if (App::GetInstallEnable() && IsExtension(entry.GetExtension(), INSTALL_EXTENSIONS)) {
                    InstallFiles();
                } else if (IsSd()) {
                    const auto assoc_list = m_menu->FindFileAssocFor();
                    if (!assoc_list.empty()) {
                        // for (auto&e : assoc_list) {
                        //     log_write("assoc got: %s\n", e.path.c_str());
                        // }

                        PopupList::Items items;
                        for (const auto&p : assoc_list) {
                            items.emplace_back(p.name);
                        }

                        const auto title = "Launch option for: "_i18n + GetEntry().name;
                        App::Push(std::make_shared<PopupList>(
                            title, items, [this, assoc_list](auto op_index){
                                if (op_index) {
                                    log_write("selected: %s\n", assoc_list[*op_index].name.c_str());
                                    nro_launch(assoc_list[*op_index].path, nro_add_arg_file(GetNewPathCurrent()));
                                } else {
                                    log_write("pressed B to skip launch...\n");
                                }
                            }

                        ));
                    } else {
                        log_write("assoc list is empty\n");
                    }
                }
            }
        }}),

        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            if (!m_menu->IsTab() && App::GetApp()->m_controller.GotHeld(Button::R2)) {
                m_menu->PromptIfShouldExit();
                return;
            }

            std::string_view view{m_path};
            if (view != m_fs->Root()) {
                const auto end = view.find_last_of('/');
                assert(end != view.npos);

                if (end == 0) {
                    Scan(m_fs->Root(), true);
                } else {
                    Scan(view.substr(0, end), true);
                }
            } else {
                if (!m_menu->IsTab()) {
                    m_menu->PromptIfShouldExit();
                }
            }
        }}),

        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            if (App::GetApp()->m_controller.GotHeld(Button::R2)) {
                DisplayAdvancedOptions();
            } else {
                DisplayOptions();
            }
        }})
    );

    SetSide(m_side);

    auto buf = path;
    if (path.empty()) {
        ini_gets("paths", "last_path", entry.root, buf, sizeof(buf), App::CONFIG_PATH);
    }

    SetFs(buf, entry);
}

FsView::FsView(Menu* menu, ViewSide side) : FsView{menu, "", FS_ENTRY_DEFAULT, side} {

}

FsView::~FsView() {
    // don't store mount points for non-sd card paths.
    if (IsSd()) {
        ini_puts("paths", "last_path", m_path, App::CONFIG_PATH);
    }
}

void FsView::Update(Controller* controller, TouchInfo* touch) {
    Widget::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_index, m_entries_current.size(), [this](bool touch, auto i) {
        if (touch && m_index == i) {
            FireAction(Button::A);
        } else {
            App::PlaySoundEffect(SoundEffect_Focus);
            SetIndex(i);
        }
    });
}

void FsView::Draw(NVGcontext* vg, Theme* theme) {
    const auto& text_col = theme->GetColour(ThemeEntryID_TEXT);

    if (m_entries_current.empty()) {
        gfx::drawTextArgs(vg, GetX() + GetW() / 2.f, GetY() + GetH() / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Empty..."_i18n.c_str());
        return;
    }

    constexpr float text_xoffset{15.f};

    m_list->Draw(vg, theme, m_entries_current.size(), [this, text_col](auto* vg, auto* theme, auto v, auto i) {
        const auto& [x, y, w, h] = v;
        auto& e = GetEntry(i);

        if (e.IsDir()) {
            // NOTE: make this native only if hdd dir scan is too slow.
            // if (m_fs->IsNative() && e.file_count == -1 && e.dir_count == -1) {
            if (e.file_count == -1 && e.dir_count == -1) {
                m_fs->DirGetEntryCount(GetNewPath(e), &e.file_count, &e.dir_count);
            }
        } else if (!e.checked_extension) {
            e.checked_extension = true;
            if (auto ext = std::strrchr(e.name, '.')) {
                e.extension = ext+1;
            }
        }

        auto text_id = ThemeEntryID_TEXT;
        const auto selected = m_index == i;
        if (selected) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, theme, 4.f, v);
        } else {
            if (i != m_entries_current.size() - 1) {
                gfx::drawRect(vg, Vec4{x, y + h, w, 1.f}, theme->GetColour(ThemeEntryID_LINE_SEPARATOR));
            }
        }

        if (e.IsDir()) {
            DrawElement(x + text_xoffset, y + 5, 50, 50, ThemeEntryID_ICON_FOLDER);
        } else {
            auto icon = ThemeEntryID_ICON_FILE;
            const auto ext = e.GetExtension();
            if (IsExtension(ext, AUDIO_EXTENSIONS)) {
                icon = ThemeEntryID_ICON_AUDIO;
            } else if (IsExtension(ext, VIDEO_EXTENSIONS)) {
                icon = ThemeEntryID_ICON_VIDEO;
            } else if (IsExtension(ext, IMAGE_EXTENSIONS)) {
                icon = ThemeEntryID_ICON_IMAGE;
            } else if (IsExtension(ext, INSTALL_EXTENSIONS)) {
                // todo: maybe replace this icon with something else?
                icon = ThemeEntryID_ICON_NRO;
            } else if (IsExtension(ext, ZIP_EXTENSIONS)) {
                icon = ThemeEntryID_ICON_ZIP;
            } else if (IsExtension(ext, "nro")) {
                icon = ThemeEntryID_ICON_NRO;
            }

            DrawElement(x + text_xoffset, y + 5, 50, 50, icon);
        }

        if (e.IsSelected()) {
            gfx::drawText(vg, x + text_xoffset + 50 / 2, y + (h / 2.f) - (24.f / 2), 24.f, "\uE14B", nullptr, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT_SELECTED));
        }

        m_scroll_name.Draw(vg, selected, x + text_xoffset+65, y + (h / 2.f), w-(75+text_xoffset+65+50), 20, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(text_id), e.name);

        // NOTE: make this native only if i disable dir scan from above.
        if (e.IsDir()) {
            gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) - 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_BOTTOM, theme->GetColour(text_id), "%zd files"_i18n.c_str(), e.file_count);
            gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) + 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_TOP, theme->GetColour(text_id), "%zd dirs"_i18n.c_str(), e.dir_count);
        } else if (e.IsFile()) {
            if (!e.time_stamp.is_valid) {
                const auto path = GetNewPath(e);
                if (m_fs->IsNative()) {
                    m_fs->GetFileTimeStampRaw(path, &e.time_stamp);
                } else {
                    m_fs->FileGetSizeAndTimestamp(path, &e.time_stamp, &e.file_size);
                }
            }

            const auto t = (time_t)(e.time_stamp.modified);
            struct tm tm{};
            localtime_r(&t, &tm);
            gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) + 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_TOP, theme->GetColour(text_id), "%02u/%02u/%u", tm.tm_mday, tm.tm_mon + 1, tm.tm_year + 1900);
            if ((double)e.file_size / 1024.0 / 1024.0 <= 0.009) {
                gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) - 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_BOTTOM, theme->GetColour(text_id), "%.2f KiB", (double)e.file_size / 1024.0);
            } else {
                gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) - 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_BOTTOM, theme->GetColour(text_id), "%.2f MiB", (double)e.file_size / 1024.0 / 1024.0);
            }
        }
    });
}

void FsView::OnFocusGained() {
    Widget::OnFocusGained();
    if (m_entries.empty()) {
        if (m_path.empty()) {
            Scan(m_fs->Root());
        } else {
            Scan(m_path);
        }
    }
}

void FsView::SetSide(ViewSide side) {
    m_side = side;

    const auto pos = m_menu->GetPos();
    this->SetPos(pos);
    Vec4 v{75, GetY() + 1.f + 42.f, 1220.f - 45.f * 2, 60};

    if (m_menu->IsSplitScreen()) {
        if (m_side == ViewSide::Left) {
            this->SetW(pos.w / 2 - pos.x / 2);
            this->SetX(pos.x / 2);
        } else if (m_side == ViewSide::Right) {
            this->SetW(pos.w / 2 - pos.x / 2);
            this->SetX(pos.x / 2 + SCREEN_WIDTH / 2);
        }

        v.w /= 2;
        v.w -= v.x / 2;

        if (m_side == ViewSide::Left) {
            v.x = v.x / 2;
        } else if (m_side == ViewSide::Right) {
            v.x = v.x / 2 + SCREEN_WIDTH / 2;
        }
    }

    m_list = std::make_unique<List>(1, 8, m_pos, v);
    if (m_menu->IsSplitScreen()) {
        m_list->SetPageJump(false);
    }

    // reset scroll position.
    m_scroll_name.Reset();
}

void FsView::SetIndex(s64 index) {
    m_index = index;
    if (!m_index) {
        m_list->SetYoff();
    }

    if (IsSd() && !m_entries_current.empty() && !GetEntry().checked_internal_extension && GetEntry().extension == "zip") {
        GetEntry().checked_internal_extension = true;

        if (auto zfile = unzOpen64(GetNewPathCurrent())) {
            ON_SCOPE_EXIT(unzClose(zfile));

            // only check first entry (i think RA does the same)
            fs::FsPath filename_inzip{};
            unz_file_info64 file_info{};
            if (UNZ_OK == unzOpenCurrentFile(zfile)) {
                ON_SCOPE_EXIT(unzCloseCurrentFile(zfile));
                if (UNZ_OK == unzGetCurrentFileInfo64(zfile, &file_info, filename_inzip, sizeof(filename_inzip), NULL, 0, NULL, 0)) {
                    if (auto ext = std::strrchr(filename_inzip, '.')) {
                        GetEntry().internal_name = filename_inzip.toString();
                        GetEntry().internal_extension = ext+1;
                    }
                }
            }
        }
    }

    m_menu->UpdateSubheading();
}

void FsView::InstallForwarder() {
    if (GetEntry().GetExtension() == "nro") {
        if (R_FAILED(homebrew::Menu::InstallHomebrewFromPath(GetNewPathCurrent()))) {
            log_write("failed to create forwarder\n");
        }
        return;
    }

    const auto assoc_list = m_menu->FindFileAssocFor();
    if (assoc_list.empty()) {
        log_write("failed to find assoc for: %s ext: %s\n", GetEntry().name, GetEntry().extension.c_str());
        return;
    }

    PopupList::Items items;
    for (const auto&p : assoc_list) {
        items.emplace_back(p.name);
    }

    const auto title = std::string{"Select launcher for: "_i18n} + GetEntry().name;
    App::Push(std::make_shared<PopupList>(
        title, items, [this, assoc_list](auto op_index){
            if (op_index) {
                const auto assoc = assoc_list[*op_index];
                log_write("pushing it\n");
                App::Push(std::make_shared<ProgressBox>(0, "Installing Forwarder"_i18n, GetEntry().name, [assoc, this](auto pbox) -> Result {
                    log_write("inside callback\n");

                    NroEntry nro{};
                    log_write("parsing nro\n");
                    R_TRY(nro_parse(assoc.path, nro));

                    NacpStruct nacp;
                    R_TRY(nro_get_nacp(assoc.path, nacp));

                    log_write("got nro data\n");
                    auto file_name = assoc.use_base_name ? GetEntry().GetName() : GetEntry().GetInternalName();

                    if (auto pos = file_name.find_last_of('.'); pos != std::string::npos) {
                        log_write("got filename\n");
                        file_name = file_name.substr(0, pos);
                        log_write("got filename2: %s\n\n", file_name.c_str());
                    }

                    const auto db_indexs = GetRomDatabaseFromPath(m_path);

                    OwoConfig config{};
                    config.nro_path = assoc.path.toString();
                    config.args = nro_add_arg_file(GetNewPathCurrent());
                    config.name = nro.nacp.lang.name + std::string{" | "} + file_name;
                    // config.name = file_name;
                    config.nacp = nacp;
                    config.icon = GetRomIcon(m_fs.get(), pbox, file_name, db_indexs, nro);
                    pbox->SetImageDataConst(config.icon);

                    return App::Install(pbox, config);
                }));
            } else {
                log_write("pressed B to skip launch...\n");
            }
        }
    ));
}

void FsView::InstallFiles() {
    const auto targets = GetSelectedEntries();

    App::Push(std::make_shared<OptionBox>("Install Selected files?"_i18n, "No"_i18n, "Yes"_i18n, 0, [this, targets](auto op_index){
        if (op_index && *op_index) {
            App::PopToMenu();

            App::Push(std::make_shared<ui::ProgressBox>(0, "Installing "_i18n, "", [this, targets](auto pbox) -> Result {
                for (auto& e : targets) {
                    R_TRY(yati::InstallFromFile(pbox, m_fs.get(), GetNewPath(e)));
                    App::Notify("Installed "_i18n + e.GetName());
                }

                R_SUCCEED();
            }));
        }
    }));
}

void FsView::UnzipFiles(fs::FsPath dir_path) {
    const auto targets = GetSelectedEntries();

    // set to current path.
    if (dir_path.empty()) {
        dir_path = m_path;
    }

    App::Push(std::make_shared<ui::ProgressBox>(0, "Extracting "_i18n, "", [this, dir_path, targets](auto pbox) -> Result {
        constexpr auto chunk_size = 1024 * 512; // 512KiB

        for (auto& e : targets) {
            pbox->SetTitle(e.GetName());

            const auto zip_out = GetNewPath(e);
            auto zfile = unzOpen64(zip_out);
            R_UNLESS(zfile, 0x1);
            ON_SCOPE_EXIT(unzClose(zfile));

            unz_global_info64 pglobal_info;
            if (UNZ_OK != unzGetGlobalInfo64(zfile, &pglobal_info)) {
                R_THROW(0x1);
            }

            for (s64 i = 0; i < pglobal_info.number_entry; i++) {
                if (i > 0) {
                    if (UNZ_OK != unzGoToNextFile(zfile)) {
                        log_write("failed to unzGoToNextFile\n");
                        R_THROW(0x1);
                    }
                }

                if (UNZ_OK != unzOpenCurrentFile(zfile)) {
                    log_write("failed to open current file\n");
                    R_THROW(0x1);
                }
                ON_SCOPE_EXIT(unzCloseCurrentFile(zfile));

                unz_file_info64 info;
                char name[512];
                if (UNZ_OK != unzGetCurrentFileInfo64(zfile, &info, name, sizeof(name), 0, 0, 0, 0)) {
                    log_write("failed to get current info\n");
                    R_THROW(0x1);
                }

                const auto file_path = fs::AppendPath(dir_path, name);
                pbox->NewTransfer(name);

                // create directories
                m_fs->CreateDirectoryRecursivelyWithPath(file_path);

                Result rc;
                if (R_FAILED(rc = m_fs->CreateFile(file_path, info.uncompressed_size, 0)) && rc != FsError_PathAlreadyExists) {
                    log_write("failed to create file: %s 0x%04X\n", file_path.s, rc);
                    R_THROW(rc);
                }

                fs::File f;
                R_TRY(m_fs->OpenFile(file_path, FsOpenMode_Write, &f));
                R_TRY(f.SetSize(info.uncompressed_size));

                std::vector<char> buf(chunk_size);
                s64 offset{};
                while (offset < info.uncompressed_size) {
                    R_TRY(pbox->ShouldExitResult());

                    const auto bytes_read = unzReadCurrentFile(zfile, buf.data(), buf.size());
                    if (bytes_read <= 0) {
                        log_write("failed to read zip file: %s\n", name);
                        R_THROW(0x1);
                    }

                    R_TRY(f.Write(offset, buf.data(), bytes_read, FsWriteOption_None));

                    pbox->UpdateTransfer(offset, info.uncompressed_size);
                    offset += bytes_read;
                }
            }
        }

        R_SUCCEED();
    }, [this](Result rc){
        App::PushErrorBox(rc, "Extract failed!"_i18n);

        if (R_SUCCEEDED(rc)) {
            App::Notify("Extract success!");
        }

        Scan(m_path);
        log_write("did extract\n");
    }));
}

void FsView::ZipFiles(fs::FsPath zip_out) {
    const auto targets = GetSelectedEntries();

    // set to current path.
    if (zip_out.empty()) {
        if (std::size(targets) == 1) {
            const auto name = targets[0].name;
            const auto ext = std::strrchr(targets[0].name, '.');
            fs::FsPath file_path;
            if (!ext) {
                std::snprintf(file_path, sizeof(file_path), "%s.zip", name);
            } else {
                std::snprintf(file_path, sizeof(file_path), "%.*s.zip", (int)(ext - name), name);
            }
            zip_out = fs::AppendPath(m_path, file_path);
            log_write("zip out: %s name: %s file_path: %s\n", zip_out.s, name, file_path.s);
        } else {
            // loop until we find an unused file name.
            for (u64 i = 0; ; i++) {
                fs::FsPath file_path = "Archive.zip";
                if (i) {
                    std::snprintf(file_path, sizeof(file_path), "Archive (%zu).zip", i);
                }

                zip_out = fs::AppendPath(m_path, file_path);
                if (!m_fs->FileExists(zip_out)) {
                    break;
                }
            }
        }
    } else {
        if (!std::string_view(zip_out).ends_with(".zip")) {
            zip_out += ".zip";
        }
    }

    App::Push(std::make_shared<ui::ProgressBox>(0, "Compressing "_i18n, "", [this, zip_out, targets](auto pbox) -> Result {
        constexpr auto chunk_size = 1024 * 512; // 512KiB

        const auto t = std::time(NULL);
        const auto tm = std::localtime(&t);

        // pre-calculate the time rather than calculate it in the loop.
        zip_fileinfo zip_info{};
        zip_info.tmz_date.tm_sec = tm->tm_sec;
        zip_info.tmz_date.tm_min = tm->tm_min;
        zip_info.tmz_date.tm_hour = tm->tm_hour;
        zip_info.tmz_date.tm_mday = tm->tm_mday;
        zip_info.tmz_date.tm_mon = tm->tm_mon;
        zip_info.tmz_date.tm_year = tm->tm_year;

        auto zfile = zipOpen(zip_out, APPEND_STATUS_CREATE);
        R_UNLESS(zfile, 0x1);
        ON_SCOPE_EXIT(zipClose(zfile, "sphaira v" APP_VERSION_HASH));

        const auto zip_add = [&](const fs::FsPath& file_path) -> Result {
            // the file name needs to be relative to the current directory.
            const char* file_name_in_zip = file_path.s + std::strlen(m_path);

            // root paths are banned in zips, they will warn when extracting otherwise.
            if (file_name_in_zip[0] == '/') {
                file_name_in_zip++;
            }

            pbox->NewTransfer(file_name_in_zip);

            const auto ext = std::strrchr(file_name_in_zip, '.');
            const auto raw = ext && IsExtension(ext + 1, COMPRESSED_EXTENSIONS);

            if (ZIP_OK != zipOpenNewFileInZip2(zfile, file_name_in_zip, &zip_info, NULL, 0, NULL, 0, NULL, Z_DEFLATED, Z_DEFAULT_COMPRESSION, raw)) {
                R_THROW(0x1);
            }
            ON_SCOPE_EXIT(zipCloseFileInZip(zfile));

            fs::File f;
            R_TRY(m_fs->OpenFile(file_path, FsOpenMode_Read, &f));

            s64 file_size;
            R_TRY(f.GetSize(&file_size));

            std::vector<char> buf(chunk_size);
            s64 offset{};
            while (offset < file_size) {
                R_TRY(pbox->ShouldExitResult());

                u64 bytes_read;
                R_TRY(f.Read(offset, buf.data(), buf.size(), FsReadOption_None, &bytes_read));

                if (ZIP_OK != zipWriteInFileInZip(zfile, buf.data(), bytes_read)) {
                    log_write("failed to write zip file: %s\n", file_path.s);
                    R_THROW(0x1);
                }

                pbox->UpdateTransfer(offset, file_size);
                offset += bytes_read;
            }

            R_SUCCEED();
        };

        for (auto& e : targets) {
            pbox->SetTitle(e.GetName());
            if (e.IsFile()) {
                const auto file_path = GetNewPath(e);
                R_TRY(zip_add(file_path));
            } else {
                FsDirCollections collections;
                get_collections(GetNewPath(e), e.name, collections);

                for (const auto& collection : collections) {
                    for (const auto& file : collection.files) {
                        const auto file_path = fs::AppendPath(collection.path, file.name);
                        R_TRY(zip_add(file_path));
                    }
                }
            }
        }

        R_SUCCEED();
    }, [this](Result rc){
        App::PushErrorBox(rc, "Compress failed!"_i18n);

        if (R_SUCCEEDED(rc)) {
            App::Notify("Compress success!");
        }

        Scan(m_path);
        log_write("did compress\n");
    }));
}

void FsView::UploadFiles() {
    const auto targets = GetSelectedEntries();

    const auto network_locations = location::Load();
    if (network_locations.empty()) {
        App::Notify("No upload locations set!"_i18n);
        return;
    }

    PopupList::Items items;
    for (const auto&p : network_locations) {
        items.emplace_back(p.name);
    }

    App::Push(std::make_shared<PopupList>(
        "Select upload location"_i18n, items, [this, network_locations](auto op_index){
            if (!op_index) {
                return;
            }

            const auto loc = network_locations[*op_index];
            App::Push(std::make_shared<ProgressBox>(0, "Uploading"_i18n, "", [this, loc](auto pbox) -> Result {
                auto targets = GetSelectedEntries();
                const auto is_file_based_emummc = App::IsFileBaseEmummc();

                const auto file_add = [&](s64 file_size, const fs::FsPath& file_path, const char* name) -> Result {
                    // the file name needs to be relative to the current directory.
                    const auto relative_file_name = file_path.s + std::strlen(m_path);
                    pbox->SetTitle(name);
                    pbox->NewTransfer(relative_file_name);

                    fs::File f;
                    R_TRY(m_fs->OpenFile(file_path, FsOpenMode_Read, &f));

                    return thread::TransferPull(pbox, file_size,
                        [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                            const auto rc = f.Read(off, data, size, FsReadOption_None, bytes_read);
                            if (m_fs->IsNative() && is_file_based_emummc) {
                                svcSleepThread(2e+6); // 2ms
                            }
                            return rc;
                        },
                        [&](thread::PullCallback pull) -> Result {
                            s64 offset{};
                            const auto result = curl::Api().FromMemory(
                                CURL_LOCATION_TO_API(loc),
                                curl::OnProgress{pbox->OnDownloadProgressCallback()},
                                curl::UploadInfo{
                                    relative_file_name, file_size,
                                    [&](void *ptr, size_t size) -> size_t {
                                        // curl will request past the size of the file, causing an error.
                                        if (offset >= file_size) {
                                            log_write("finished file upload\n");
                                            return 0;
                                        }

                                        u64 bytes_read{};
                                        if (R_FAILED(pull(ptr, size, &bytes_read))) {
                                            log_write("failed to read in custom callback: %zd size: %zd\n", offset, size);
                                            return 0;
                                        }

                                        offset += bytes_read;
                                        return bytes_read;
                                    }
                                }
                            );

                            R_UNLESS(result.success, 0x1);
                            R_SUCCEED();
                        }
                    );
                };

                for (auto& e : targets) {
                    if (e.IsFile()) {
                        const auto file_path = GetNewPath(e);
                        R_TRY(file_add(e.file_size, file_path, e.GetName().c_str()));
                    } else {
                        FsDirCollections collections;
                        get_collections(GetNewPath(e), e.name, collections, true);

                        for (const auto& collection : collections) {
                            for (const auto& file : collection.files) {
                                const auto file_path = fs::AppendPath(collection.path, file.name);
                                R_TRY(file_add(file.file_size, file_path, file.name));
                            }
                        }
                    }
                }

                R_SUCCEED();
            }, [this](Result rc){
                App::PushErrorBox(rc, "Failed to, TODO: add message here"_i18n);
                m_menu->ResetSelection();

                if (R_SUCCEEDED(rc)) {
                    App::Notify("Upload successfull!"_i18n);
                    log_write("Upload successfull!!!\n");
                } else {
                    App::Notify("Upload failed!"_i18n);
                    log_write("Upload failed!!!\n");
                }
            }));
        }
    ));
}

auto FsView::Scan(const fs::FsPath& new_path, bool is_walk_up) -> Result {
    appletSetCpuBoostMode(ApmCpuBoostMode_FastLoad);
    ON_SCOPE_EXIT(appletSetCpuBoostMode(ApmCpuBoostMode_Normal));

    log_write("new scan path: %s\n", new_path.s);
    if (!is_walk_up && !m_path.empty() && !m_entries_current.empty()) {
        const LastFile f(GetEntry().name, m_index, m_list->GetYoff(), m_entries_current.size());
        m_previous_highlighted_file.emplace_back(f);
    }

    m_path = new_path;
    m_entries.clear();
    m_index = 0;
    m_list->SetYoff(0);
    m_menu->SetTitleSubHeading(m_path);
    m_selected_count = 0;

    fs::Dir d;
    R_TRY(m_fs->OpenDirectory(new_path, FsDirOpenMode_ReadDirs | FsDirOpenMode_ReadFiles, &d));

    // we won't run out of memory here (tm)
    std::vector<FsDirectoryEntry> dir_entries;
    R_TRY(d.ReadAll(dir_entries));

    const auto count = dir_entries.size();
    m_entries.reserve(count);

    m_entries_index.clear();
    m_entries_index_hidden.clear();
    m_entries_index_search.clear();

    m_entries_index.reserve(count);
    m_entries_index_hidden.reserve(count);

    u32 i = 0;
    for (const auto& e : dir_entries) {
        m_entries_index_hidden.emplace_back(i);
        if ('.' != e.name[0]) {
            m_entries_index.emplace_back(i);
        }

        m_entries.emplace_back(e);
        i++;
    }

    m_entries.shrink_to_fit();
    m_entries_index.shrink_to_fit();
    m_entries_index_hidden.shrink_to_fit();
    Sort();

    // quick check to see if this is an update folder
    m_is_update_folder = R_SUCCEEDED(CheckIfUpdateFolder());

    SetIndex(0);

    // find previous entry
    if (is_walk_up && !m_previous_highlighted_file.empty()) {
        ON_SCOPE_EXIT(m_previous_highlighted_file.pop_back());
        SetIndexFromLastFile(m_previous_highlighted_file.back());
    }

    R_SUCCEED();
}

void FsView::Sort() {
    // returns true if lhs should be before rhs
    const auto sort = m_menu->m_sort.Get();
    const auto order = m_menu->m_order.Get();
    const auto folders_first = m_menu->m_folders_first.Get();
    const auto hidden_last = m_menu->m_hidden_last.Get();

    const auto sorter = [this, sort, order, folders_first, hidden_last](u32 _lhs, u32 _rhs) -> bool {
        const auto& lhs = m_entries[_lhs];
        const auto& rhs = m_entries[_rhs];

        if (hidden_last) {
            if (lhs.IsHidden() && !rhs.IsHidden()) {
                return false;
            } else if (!lhs.IsHidden() && rhs.IsHidden()) {
                return true;
            }
        }

        if (folders_first) {
            if (lhs.type == FsDirEntryType_Dir && !(rhs.type == FsDirEntryType_Dir)) { // left is folder
                return true;
            } else if (!(lhs.type == FsDirEntryType_Dir) && rhs.type == FsDirEntryType_Dir) { // right is folder
                return false;
            }
        }

        switch (sort) {
            case SortType_Size: {
                if (lhs.file_size == rhs.file_size) {
                    return strncasecmp(lhs.name, rhs.name, sizeof(lhs.name)) < 0;
                } else if (order == OrderType_Descending) {
                    return lhs.file_size > rhs.file_size;
                } else {
                    return lhs.file_size < rhs.file_size;
                }
            } break;
            case SortType_Alphabetical: {
                if (order == OrderType_Descending) {
                    return strncasecmp(lhs.name, rhs.name, sizeof(lhs.name)) < 0;
                } else {
                    return strncasecmp(lhs.name, rhs.name, sizeof(lhs.name)) > 0;
                }
            } break;
        }

        std::unreachable();
    };

    if (m_menu->m_show_hidden.Get()) {
        m_entries_current = m_entries_index_hidden;
    } else {
        m_entries_current = m_entries_index;
    }

    std::sort(m_entries_current.begin(), m_entries_current.end(), sorter);
}

void FsView::SortAndFindLastFile() {
    std::optional<LastFile> last_file;
    if (!m_path.empty() && !m_entries_current.empty()) {
        last_file = LastFile(GetEntry().name, m_index, m_list->GetYoff(), m_entries_current.size());
    }

    Sort();

    if (last_file.has_value()) {
        SetIndexFromLastFile(*last_file);
    }
}

void FsView::SetIndexFromLastFile(const LastFile& last_file) {
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries_current.size(); i++) {
        if (last_file.name == GetEntry(i).name) {
            index = i;
            break;
        }
    }
    if (index >= 0) {
        if (index == last_file.index && m_entries_current.size() == last_file.entries_count) {
            m_list->SetYoff(last_file.offset);
            log_write("index is the same as last time\n");
        } else {
            // file position changed!
            log_write("file position changed\n");
            // guesstimate where the position is
            if (index >= 8) {
                m_list->SetYoff(((index - 8) + 1) * m_list->GetMaxY());
            } else {
                m_list->SetYoff(0);
            }
        }
        SetIndex(index);
    }
}

void FsView::OnDeleteCallback() {
    bool use_progress_box{true};

    // check if we only have 1 file / folder
    if (m_menu->m_selected.m_files.size() == 1) {
        const auto& entry = m_menu->m_selected.m_files[0];
        const auto full_path = GetNewPath(m_menu->m_selected.m_path, entry.name);

        if (entry.IsDir()) {
            bool empty{};
            m_fs->IsDirEmpty(full_path, &empty);
            if (empty) {
                if (auto rc = m_fs->DeleteDirectory(full_path); R_FAILED(rc)) {
                    App::PushErrorBox(rc, "Failed to delete directory"_i18n);
                }
                use_progress_box = false;
            }
        } else {
            if (auto rc = m_fs->DeleteFile(full_path); R_FAILED(rc)) {
                App::PushErrorBox(rc, "Failed to delete file"_i18n);
            }
            use_progress_box = false;
        }
    }

    if (!use_progress_box) {
        m_menu->RefreshViews();
        log_write("did delete\n");
    } else {
        App::Push(std::make_shared<ProgressBox>(0, "Deleting"_i18n, "", [this](auto pbox) -> Result {
            FsDirCollections collections;
            auto& selected = m_menu->m_selected;
            auto src_fs = selected.m_view->GetFs();

            // build list of dirs / files
            for (const auto&p : selected.m_files) {
                pbox->Yield();
                R_TRY(pbox->ShouldExitResult());

                const auto full_path = GetNewPath(selected.m_path, p.name);
                if (p.IsDir()) {
                    pbox->NewTransfer("Scanning "_i18n + full_path);
                    R_TRY(get_collections(src_fs, full_path, p.name, collections));
                }
            }

            return DeleteAllCollections(pbox, src_fs, selected, collections);
        }, [this](Result rc){
            App::PushErrorBox(rc, "Failed to, TODO: add message here"_i18n);

            m_menu->RefreshViews();
            log_write("did delete\n");
        }));
    }
}

void FsView::OnPasteCallback() {
    // check if we only have 1 file / folder and is cut (rename)
    if (m_menu->m_selected.SameFs(this) && m_menu->m_selected.m_files.size() == 1 && m_menu->m_selected.m_type == SelectedType::Cut) {
        const auto& entry = m_menu->m_selected.m_files[0];
        const auto full_path = GetNewPath(m_menu->m_selected.m_path, entry.name);

        if (entry.IsDir()) {
            m_fs->RenameDirectory(full_path, GetNewPath(entry));
        } else {
            m_fs->RenameFile(full_path, GetNewPath(entry));
        }

        m_menu->RefreshViews();
    } else {
        App::Push(std::make_shared<ProgressBox>(0, "Pasting"_i18n, "", [this](auto pbox) -> Result {
            auto& selected = m_menu->m_selected;
            auto src_fs = selected.m_view->GetFs();

            if (selected.SameFs(this) && selected.m_type == SelectedType::Cut) {
                for (const auto& p : selected.m_files) {
                    pbox->Yield();
                    R_TRY(pbox->ShouldExitResult());

                    const auto src_path = GetNewPath(selected.m_path, p.name);
                    const auto dst_path = GetNewPath(m_path, p.name);

                    pbox->SetTitle(p.name);
                    pbox->NewTransfer("Pasting "_i18n + src_path);

                    if (p.IsDir()) {
                        m_fs->RenameDirectory(src_path, dst_path);
                    } else {
                        m_fs->RenameFile(src_path, dst_path);
                    }
                }
            } else {
                FsDirCollections collections;

                const auto on_paste_file = [&](auto& src_path, auto& dst_path) -> Result {
                    if (selected.m_type == SelectedType::Cut) {
                        // update timestamp if possible.
                        if (!m_fs->IsNative()) {
                            FsTimeStampRaw ts;
                            if (R_SUCCEEDED(src_fs->GetFileTimeStampRaw(src_path, &ts))) {
                                m_fs->SetTimestamp(dst_path, &ts);
                            }
                        }

                        // delete src file. folders are removed after.
                        R_TRY(src_fs->DeleteFile(src_path));
                    }

                    R_SUCCEED();
                };

                // build list of dirs / files
                for (const auto&p : selected.m_files) {
                    pbox->Yield();
                    R_TRY(pbox->ShouldExitResult());

                    const auto full_path = GetNewPath(selected.m_path, p.name);
                    if (p.IsDir()) {
                        pbox->NewTransfer("Scanning "_i18n + full_path);
                        R_TRY(get_collections(src_fs, full_path, p.name, collections));
                    }
                }

                for (const auto& p : selected.m_files) {
                    pbox->Yield();
                    R_TRY(pbox->ShouldExitResult());

                    const auto src_path = GetNewPath(selected.m_path, p.name);
                    const auto dst_path = GetNewPath(p);

                    if (p.IsDir()) {
                        pbox->SetTitle(p.name);
                        pbox->NewTransfer("Creating "_i18n + dst_path);
                        m_fs->CreateDirectory(dst_path);
                    } else {
                        pbox->SetTitle(p.name);
                        pbox->NewTransfer("Copying "_i18n + src_path);
                        R_TRY(pbox->CopyFile(src_fs, m_fs.get(), src_path, dst_path));
                        R_TRY(on_paste_file(src_path, dst_path));
                    }
                }

                // copy everything in collections
                for (const auto& c : collections) {
                    const auto base_dst_path = GetNewPath(m_path, c.parent_name);

                    for (const auto& p : c.dirs) {
                        pbox->Yield();
                        R_TRY(pbox->ShouldExitResult());

                        // const auto src_path = GetNewPath(c.path, p.name);
                        const auto dst_path = GetNewPath(base_dst_path, p.name);

                        pbox->SetTitle(p.name);
                        pbox->NewTransfer("Creating "_i18n + dst_path);
                        m_fs->CreateDirectory(dst_path);
                    }

                    for (const auto& p : c.files) {
                        pbox->Yield();
                        R_TRY(pbox->ShouldExitResult());

                        const auto src_path = GetNewPath(c.path, p.name);
                        const auto dst_path = GetNewPath(base_dst_path, p.name);

                        pbox->SetTitle(p.name);
                        pbox->NewTransfer("Copying "_i18n + src_path);
                        R_TRY(pbox->CopyFile(src_fs, m_fs.get(), src_path, dst_path));
                        R_TRY(on_paste_file(src_path, dst_path));
                    }
                }

                // moving accross fs is not possible, thus files have to be copied.
                // this leaves the files on the src_fs.
                // the files are deleted one by one after a successfull copy (see above)
                // however this leaves the folders.
                // the folders cannot be deleted until the end as they have to be removed in
                // reverse order so that the folder can be deleted (it must be empty).
                if (selected.m_type == SelectedType::Cut) {
                    R_TRY(DeleteAllCollections(pbox, src_fs, selected, collections, FsDirOpenMode_ReadDirs));
                }
            }

            R_SUCCEED();
        }, [this](Result rc){
            App::PushErrorBox(rc, "Failed to, TODO: add message here"_i18n);

            m_menu->RefreshViews();
            log_write("did paste\n");
        }));
    }
}

void FsView::OnRenameCallback() {

}

auto FsView::CheckIfUpdateFolder() -> Result {
    R_UNLESS(IsSd(), FsError_InvalidMountName);
    R_UNLESS(m_fs->IsNative(), 0x1);

    // check if we have already tried to find daybreak
    if (m_daybreak_path.has_value() && m_daybreak_path.value().empty()) {
        return FsError_FileNotFound;
    }

    // check that we have daybreak installed
    if (!m_daybreak_path.has_value()) {
        auto daybreak_path = DAYBREAK_PATH;
        if (!m_fs->FileExists(DAYBREAK_PATH)) {
            if (auto e = nro_find(homebrew::GetNroEntries(), "Daybreak", "Atmosphere-NX", {}); e.has_value()) {
                daybreak_path = e.value().path;
            } else {
                log_write("failed to find daybreak\n");
                m_daybreak_path = "";
                return FsError_FileNotFound;
            }
        }
        m_daybreak_path = daybreak_path;
        log_write("found daybreak in: %s\n", m_daybreak_path.value().s);
    }

    // check that we have enough ncas and not too many
    R_UNLESS(m_entries.size() > 150 && m_entries.size() < 300, 0x1);

    // check that all entries end in .nca
    const auto nca_ext = std::string_view{".nca"};
    for (auto& e : m_entries) {
        // check that we are at the bottom level
        R_UNLESS(e.type == FsDirEntryType_File, 0x1);

        const auto ext = std::strrchr(e.name, '.');
        R_UNLESS(ext && ext == nca_ext, 0x1);
    }

    R_SUCCEED();
}

auto FsView::get_collection(fs::Fs* fs, const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollection& out, bool inc_file, bool inc_dir, bool inc_size) -> Result {
    out.path = path;
    out.parent_name = parent_name;

    const auto fetch = [fs, &path](std::vector<FsDirectoryEntry>& out, u32 flags) -> Result {
        fs::Dir d;
        R_TRY(fs->OpenDirectory(path, flags, &d));
        return d.ReadAll(out);
    };

    if (inc_file) {
        u32 flags = FsDirOpenMode_ReadFiles;
        if (!inc_size) {
            flags |= FsDirOpenMode_NoFileSize;
        }
        R_TRY(fetch(out.files, flags));
    }

    if (inc_dir) {
        R_TRY(fetch(out.dirs, FsDirOpenMode_ReadDirs));
    }

    R_SUCCEED();
}

auto FsView::get_collections(fs::Fs* fs, const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollections& out, bool inc_size) -> Result {
    // get a list of all the files / dirs
    FsDirCollection collection;
    R_TRY(get_collection(fs, path, parent_name, collection, true, true, inc_size));
    log_write("got collection: %s parent_name: %s files: %zu dirs: %zu\n", path.s, parent_name.s, collection.files.size(), collection.dirs.size());
    out.emplace_back(collection);

    // for (size_t i = 0; i < collection.dirs.size(); i++) {
    for (const auto&p : collection.dirs) {
        // use heap as to not explode the stack
        const auto new_path = std::make_unique<fs::FsPath>(FsView::GetNewPath(path, p.name));
        const auto new_parent_name = std::make_unique<fs::FsPath>(FsView::GetNewPath(parent_name, p.name));
        log_write("trying to get nested collection: %s parent_name: %s\n", new_path->s, new_parent_name->s);
        R_TRY(get_collections(fs, *new_path, *new_parent_name, out, inc_size));
    }

    R_SUCCEED();
}

auto FsView::get_collection(const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollection& out, bool inc_file, bool inc_dir, bool inc_size) -> Result {
    return get_collection(m_fs.get(), path, parent_name, out, true, true, inc_size);
}

auto FsView::get_collections(const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollections& out, bool inc_size) -> Result {
    return get_collections(m_fs.get(), path, parent_name, out, inc_size);
}

static Result DeleteAllCollections(ProgressBox* pbox, fs::Fs* fs, const SelectedStash& selected, const FsDirCollections& collections, u32 mode = FsDirOpenMode_ReadDirs|FsDirOpenMode_ReadFiles) {
    // delete everything in collections, reversed
    for (const auto& c : std::views::reverse(collections)) {
        const auto delete_func = [&](auto& array) -> Result {
            for (const auto& p : array) {
                pbox->Yield();
                R_TRY(pbox->ShouldExitResult());

                const auto full_path = FsView::GetNewPath(c.path, p.name);
                pbox->SetTitle(p.name);
                pbox->NewTransfer("Deleting "_i18n + full_path.toString());
                if ((mode & FsDirOpenMode_ReadDirs) && p.type == FsDirEntryType_Dir) {
                    log_write("deleting dir: %s\n", full_path.s);
                    R_TRY(fs->DeleteDirectory(full_path));
                } else if ((mode & FsDirOpenMode_ReadFiles) && p.type == FsDirEntryType_File) {
                    log_write("deleting file: %s\n", full_path.s);
                    R_TRY(fs->DeleteFile(full_path));
                }
            }

            R_SUCCEED();
        };

        R_TRY(delete_func(c.files));
        R_TRY(delete_func(c.dirs));
    }

    for (const auto& p : selected.m_files) {
        pbox->Yield();
        R_TRY(pbox->ShouldExitResult());

        const auto full_path = FsView::GetNewPath(selected.m_path, p.name);
        pbox->SetTitle(p.name);
        pbox->NewTransfer("Deleting "_i18n + full_path.toString());

        if ((mode & FsDirOpenMode_ReadDirs) && p.type == FsDirEntryType_Dir) {
            log_write("deleting dir: %s\n", full_path.s);
            R_TRY(fs->DeleteDirectory(full_path));
        } else if ((mode & FsDirOpenMode_ReadFiles) && p.type == FsDirEntryType_File) {
            log_write("deleting file: %s\n", full_path.s);
            R_TRY(fs->DeleteFile(full_path));
        }
    }

    R_SUCCEED();
}

void FsView::SetFs(const fs::FsPath& new_path, const FsEntry& new_entry) {
    if (m_fs && m_fs_entry.root == new_entry.root && m_fs_entry.type == new_entry.type) {
        log_write("same fs, ignoring\n");
        return;
    }

    // m_fs.reset();
    m_path = new_path;
    m_entries.clear();
    m_entries_index.clear();
    m_entries_index_hidden.clear();
    m_entries_index_search.clear();
    m_entries_current = {};
    m_previous_highlighted_file.clear();
    m_menu->m_selected.Reset();
    m_selected_count = 0;
    m_fs_entry = new_entry;

    switch (new_entry.type) {
         case FsType::Sd:
            m_fs = std::make_unique<fs::FsNativeSd>(m_menu->m_ignore_read_only.Get());
            break;
        case FsType::ImageNand:
            m_fs = std::make_unique<fs::FsNativeImage>(FsImageDirectoryId_Nand);
            break;
        case FsType::ImageSd:
            m_fs = std::make_unique<fs::FsNativeImage>(FsImageDirectoryId_Sd);
            break;
        case FsType::Stdio:
            m_fs = std::make_unique<fs::FsStdio>(true, new_entry.root);
            break;
    }

    if (HasFocus()) {
        if (m_path.empty()) {
            Scan(m_fs->Root());
        } else {
            Scan(m_path);
        }
    }
}

void FsView::DisplayHash(hash::Type type) {
    // hack because we cannot share output between threaded calls...
    static std::string hash_out;
    hash_out.clear();

    App::Push(std::make_shared<ProgressBox>(0, "Hashing"_i18n, GetEntry().name, [this, type](auto pbox) -> Result {
        const auto full_path = GetNewPathCurrent();
        pbox->NewTransfer(full_path);
        R_TRY(hash::Hash(pbox, type, m_fs.get(), full_path, hash_out));

        R_SUCCEED();
    }, [this, type](Result rc){
        App::PushErrorBox(rc, "Failed to hash file..."_i18n);

        if (R_SUCCEEDED(rc)) {
            char buf[0x100];
            // std::snprintf(buf, sizeof(buf), "%s\n%s\n%s", hash::GetTypeStr(type), hash_out.c_str(), GetEntry().GetName());
            std::snprintf(buf, sizeof(buf), "%s\n%s", hash::GetTypeStr(type), hash_out.c_str());
            App::Push(std::make_shared<OptionBox>(buf, "OK"_i18n));
        }
    }));
}

void FsView::DisplayOptions() {
    auto options = std::make_shared<Sidebar>("File Options"_i18n, Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(options));

    options->Add(std::make_shared<SidebarEntryCallback>("Sort By"_i18n, [this](){
        auto options = std::make_shared<Sidebar>("Sort Options"_i18n, Sidebar::Side::RIGHT);
        ON_SCOPE_EXIT(App::Push(options));

        SidebarEntryArray::Items sort_items;
        sort_items.push_back("Size"_i18n);
        sort_items.push_back("Alphabetical"_i18n);

        SidebarEntryArray::Items order_items;
        order_items.push_back("Descending"_i18n);
        order_items.push_back("Ascending"_i18n);

        options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this](s64& index_out){
            m_menu->m_sort.Set(index_out);
            SortAndFindLastFile();
        }, m_menu->m_sort.Get()));

        options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this](s64& index_out){
            m_menu->m_order.Set(index_out);
            SortAndFindLastFile();
        }, m_menu->m_order.Get()));

        options->Add(std::make_shared<SidebarEntryBool>("Show Hidden"_i18n, m_menu->m_show_hidden.Get(), [this](bool& v_out){
            m_menu->m_show_hidden.Set(v_out);
            SortAndFindLastFile();
        }));

        options->Add(std::make_shared<SidebarEntryBool>("Folders First"_i18n, m_menu->m_folders_first.Get(), [this](bool& v_out){
            m_menu->m_folders_first.Set(v_out);
            SortAndFindLastFile();
        }));

        options->Add(std::make_shared<SidebarEntryBool>("Hidden Last"_i18n, m_menu->m_hidden_last.Get(), [this](bool& v_out){
            m_menu->m_hidden_last.Set(v_out);
            SortAndFindLastFile();
        }));
    }));

    if (m_entries_current.size()) {
        options->Add(std::make_shared<SidebarEntryCallback>("Cut"_i18n, [this](){
            m_menu->AddSelectedEntries(SelectedType::Cut);
        }, true));

        options->Add(std::make_shared<SidebarEntryCallback>("Copy"_i18n, [this](){
            m_menu->AddSelectedEntries(SelectedType::Copy);
        }, true));
    }

    if (!m_menu->m_selected.Empty() && (m_menu->m_selected.Type() == SelectedType::Cut || m_menu->m_selected.Type() == SelectedType::Copy)) {
        options->Add(std::make_shared<SidebarEntryCallback>("Paste"_i18n, [this](){
            const std::string buf = "Paste file(s)?"_i18n;
            App::Push(std::make_shared<OptionBox>(
                buf, "No"_i18n, "Yes"_i18n, 0, [this](auto op_index){
                if (op_index && *op_index) {
                    App::PopToMenu();
                    OnPasteCallback();
                }
            }));
        }));
    }

    // can't rename more than 1 file
    if (m_entries_current.size() && !m_selected_count) {
        options->Add(std::make_shared<SidebarEntryCallback>("Rename"_i18n, [this](){
            std::string out;
            const auto& entry = GetEntry();
            const auto name = entry.GetName();
            if (R_SUCCEEDED(swkbd::ShowText(out, "Set New File Name"_i18n.c_str(), name.c_str())) && !out.empty() && out != name) {
                App::PopToMenu();

                const auto src_path = GetNewPath(entry);
                const auto dst_path = GetNewPath(m_path, out);

                Result rc;
                if (entry.IsFile()) {
                    rc = m_fs->RenameFile(src_path, dst_path);
                } else {
                    rc = m_fs->RenameDirectory(src_path, dst_path);
                }

                if (R_SUCCEEDED(rc)) {
                    Scan(m_path);
                } else {
                    const auto msg = std::string("Failed to rename file: ") + entry.name;
                    App::PushErrorBox(rc, msg);
                }
            }
        }));
    }

    if (m_entries_current.size()) {
        options->Add(std::make_shared<SidebarEntryCallback>("Delete"_i18n, [this](){
            m_menu->AddSelectedEntries(SelectedType::Delete);

            log_write("clicked on delete\n");
            App::Push(std::make_shared<OptionBox>(
                "Delete Selected files?"_i18n, "No"_i18n, "Yes"_i18n, 0, [this](auto op_index){
                    if (op_index && *op_index) {
                        App::PopToMenu();
                        OnDeleteCallback();
                    }
                }
            ));
            log_write("pushed delete\n");
        }));
    }

    // returns true if all entries match the ext array.
    const auto check_all_ext = [this](auto& exts){
        const auto entries = GetSelectedEntries();
        for (auto&e : entries) {
            if (!IsExtension(e.GetExtension(), exts)) {
                return false;
            }
        }
        return true;
    };

    // if install is enabled, check if all currently selected files are installable.
    if (m_entries_current.size() && App::GetInstallEnable()) {
        if (check_all_ext(INSTALL_EXTENSIONS)) {
            options->Add(std::make_shared<SidebarEntryCallback>("Install"_i18n, [this](){
                InstallFiles();
            }));
        }
    }

    if (IsSd() && m_entries_current.size() && !m_selected_count) {
        if (App::GetInstallEnable() && GetEntry().IsFile() && (GetEntry().GetExtension() == "nro" || !m_menu->FindFileAssocFor().empty())) {
            options->Add(std::make_shared<SidebarEntryCallback>("Install Forwarder"_i18n, [this](){;
                if (App::GetInstallPrompt()) {
                    App::Push(std::make_shared<OptionBox>(
                        "WARNING: Installing forwarders will lead to a ban!"_i18n,
                        "Back"_i18n, "Install"_i18n, 0, [this](auto op_index){
                            if (op_index && *op_index) {
                                InstallForwarder();
                            }
                        }
                    ));
                } else {
                    InstallForwarder();
                }
            }));
        }
    }

    if (m_entries_current.size()) {
        if (check_all_ext(ZIP_EXTENSIONS)) {
            options->Add(std::make_shared<SidebarEntryCallback>("Extract zip"_i18n, [this](){
                auto options = std::make_shared<Sidebar>("Extract Options"_i18n, Sidebar::Side::RIGHT);
                ON_SCOPE_EXIT(App::Push(options));

                options->Add(std::make_shared<SidebarEntryCallback>("Extract here"_i18n, [this](){
                    UnzipFiles("");
                }));

                options->Add(std::make_shared<SidebarEntryCallback>("Extract to root"_i18n, [this](){
                    App::Push(std::make_shared<OptionBox>("Are you sure you want to extract to root?"_i18n,
                        "No"_i18n, "Yes"_i18n, 0, [this](auto op_index){
                        if (op_index && *op_index) {
                            UnzipFiles(m_fs->Root());
                        }
                    }));
                }));

                options->Add(std::make_shared<SidebarEntryCallback>("Extract to..."_i18n, [this](){
                    std::string out;
                    if (R_SUCCEEDED(swkbd::ShowText(out, "Enter the path to the folder to extract into", fs::AppendPath(m_path, ""))) && !out.empty()) {
                        UnzipFiles(out);
                    }
                }));
            }));
        }

        if (!check_all_ext(ZIP_EXTENSIONS) || m_selected_count) {
            options->Add(std::make_shared<SidebarEntryCallback>("Compress to zip"_i18n, [this](){
                auto options = std::make_shared<Sidebar>("Compress Options"_i18n, Sidebar::Side::RIGHT);
                ON_SCOPE_EXIT(App::Push(options));

                options->Add(std::make_shared<SidebarEntryCallback>("Compress"_i18n, [this](){
                    ZipFiles("");
                }));

                options->Add(std::make_shared<SidebarEntryCallback>("Compress to..."_i18n, [this](){
                    std::string out;
                    if (R_SUCCEEDED(swkbd::ShowText(out, "Enter the path to the folder to extract into", m_path)) && !out.empty()) {
                        ZipFiles(out);
                    }
                }));
            }));
        }
    }

    options->Add(std::make_shared<SidebarEntryCallback>("Advanced"_i18n, [this](){
        DisplayAdvancedOptions();
    }));
}

void FsView::DisplayAdvancedOptions() {
    auto options = std::make_shared<Sidebar>("Advanced Options"_i18n, Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(options));

    SidebarEntryArray::Items mount_items;
    std::vector<FsEntry> fs_entries;

    const auto stdio_locations = location::GetStdio(false);
    for (const auto& e: stdio_locations) {
        u32 flags{};
        if (e.write_protect) {
            flags |= FsEntryFlag_ReadOnly;
        }

        fs_entries.emplace_back(e.name, e.mount, FsType::Stdio, flags);
        mount_items.push_back(e.name);
    }

    for (const auto& e: FS_ENTRIES) {
        fs_entries.emplace_back(e);
        mount_items.push_back(i18n::get(e.name));
    }

    options->Add(std::make_shared<SidebarEntryArray>("Mount"_i18n, mount_items, [this, fs_entries](s64& index_out){
        App::PopToMenu();
        SetFs(fs_entries[index_out].root, fs_entries[index_out]);
    }, m_fs_entry.name));

    options->Add(std::make_shared<SidebarEntryCallback>("Create File"_i18n, [this](){
        std::string out;
        if (R_SUCCEEDED(swkbd::ShowText(out, "Set File Name"_i18n.c_str(), fs::AppendPath(m_path, ""))) && !out.empty()) {
            App::PopToMenu();

            fs::FsPath full_path;
            if (out.starts_with(m_fs_entry.root.s)) {
                full_path = out;
            } else {
                full_path = fs::AppendPath(m_path, out);
            }

            m_fs->CreateDirectoryRecursivelyWithPath(full_path);
            if (R_SUCCEEDED(m_fs->CreateFile(full_path, 0, 0))) {
                log_write("created file: %s\n", full_path.s);
                Scan(m_path);
            } else {
                log_write("failed to create file: %s\n", full_path.s);
            }
        }
    }));

    options->Add(std::make_shared<SidebarEntryCallback>("Create Folder"_i18n, [this](){
        std::string out;
        if (R_SUCCEEDED(swkbd::ShowText(out, "Set Folder Name"_i18n.c_str(), fs::AppendPath(m_path, ""))) && !out.empty()) {
            App::PopToMenu();

            fs::FsPath full_path;
            if (out.starts_with(m_fs_entry.root.s)) {
                full_path = out;
            } else {
                full_path = fs::AppendPath(m_path, out);
            }

            if (R_SUCCEEDED(m_fs->CreateDirectoryRecursively(full_path))) {
                log_write("created dir: %s\n", full_path.s);
                Scan(m_path);
            } else {
                log_write("failed to create dir: %s\n", full_path.s);
            }
        }
    }));

    if (IsSd() && m_entries_current.size() && !m_selected_count && GetEntry().IsFile() && GetEntry().file_size < 1024*64) {
        options->Add(std::make_shared<SidebarEntryCallback>("View as text (unfinished)"_i18n, [this](){
            App::Push(std::make_shared<fileview::Menu>(GetNewPathCurrent()));
        }));
    }

    if (m_entries_current.size()) {
        options->Add(std::make_shared<SidebarEntryCallback>("Upload"_i18n, [this](){
            UploadFiles();
        }));
    }

    if (m_entries_current.size() && !m_selected_count && GetEntry().IsFile()) {
        options->Add(std::make_shared<SidebarEntryCallback>("Hash"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("Hash Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            options->Add(std::make_shared<SidebarEntryCallback>("CRC32"_i18n, [this](){
                DisplayHash(hash::Type::Crc32);
            }));
            options->Add(std::make_shared<SidebarEntryCallback>("MD5"_i18n, [this](){
                DisplayHash(hash::Type::Md5);
            }));
            options->Add(std::make_shared<SidebarEntryCallback>("SHA1"_i18n, [this](){
                DisplayHash(hash::Type::Sha1);
            }));
            options->Add(std::make_shared<SidebarEntryCallback>("SHA256"_i18n, [this](){
                DisplayHash(hash::Type::Sha256);
            }));
        }));
    }

    options->Add(std::make_shared<SidebarEntryBool>("Ignore read only"_i18n, m_menu->m_ignore_read_only.Get(), [this](bool& v_out){
        m_menu->m_ignore_read_only.Set(v_out);
        m_fs->SetIgnoreReadOnly(v_out);
    }));
}

Menu::Menu(u32 flags) : MenuBase{"FileBrowser"_i18n, flags} {
    SetAction(Button::L3, Action{"Split"_i18n, [this](){
        SetSplitScreen(IsSplitScreen() ^ 1);
    }});

    if (!IsTab()) {
        SetAction(Button::SELECT, Action{"Close"_i18n, [this](){
            PromptIfShouldExit();
        }});
    }

    view = view_left = std::make_shared<FsView>(this, ViewSide::Left);
}

Menu::~Menu() {
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
    view->Update(controller, touch);
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    // workaround the buttons not being display properly.
    // basically, inherit all actions from the view, draw them,
    // then restore state after.
    const auto actions_copy = GetActions();
    ON_SCOPE_EXIT(m_actions = actions_copy);
    m_actions.insert_range(view->GetActions());

    MenuBase::Draw(vg, theme);

    if (IsSplitScreen()) {
        view_left->Draw(vg, theme);
        view_right->Draw(vg, theme);

        if (view == view_left) {
            gfx::drawRect(vg, view_right->GetPos(), theme->GetColour(ThemeEntryID_FOCUS), 5);
        } else {
            gfx::drawRect(vg, view_left->GetPos(), theme->GetColour(ThemeEntryID_FOCUS), 5);
        }

        gfx::drawRect(vg, SCREEN_WIDTH/2, GetY(), 1, GetH(), theme->GetColour(ThemeEntryID_LINE));
    } else {
        view->Draw(vg, theme);
    }
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();

    if (IsSplitScreen()) {
        view_left->OnFocusGained();
        view_right->OnFocusGained();
    } else {
        view->OnFocusGained();
    }

    if (!m_loaded_assoc_entries) {
        m_loaded_assoc_entries = true;
        log_write("loading assoc entries\n");
        LoadAssocEntries();
    }
}

auto Menu::FindFileAssocFor() -> std::vector<FileAssocEntry> {
    // only support roms in correctly named folders, sorry!
    const auto db_indexs = GetRomDatabaseFromPath(view->m_path);
    const auto& entry = view->GetEntry();
    const auto extension = entry.extension;
    const auto internal_extension = entry.internal_extension.empty() ? entry.extension : entry.internal_extension;
    if (extension.empty() && internal_extension.empty()) {
        // log_write("failed to get extension for db: %s path: %s\n", database_entry.c_str(), m_path);
        return {};
    }

    std::vector<FileAssocEntry> out_entries;
    if (!db_indexs.empty()) {
        // if database isn't empty, then we are in a valid folder
        // search for an entry that matches the db and ext
        for (const auto& assoc : m_assoc_entries) {
            for (const auto& assoc_db : assoc.database) {
                // if (assoc_db == PATHS[db_idx].folder || assoc_db == PATHS[db_idx].database) {
                for (auto db_idx : db_indexs) {
                    if (PATHS[db_idx].IsDatabase(assoc_db)) {
                        if (assoc.IsExtension(extension, internal_extension)) {
                            out_entries.emplace_back(assoc);
                            goto jump;
                        }
                    }
                }
            }
            jump:
        }
    } else {
        // otherwise, if not in a valid folder, find an entry that doesn't
        // use a database, ie, not a emulator.
        // this is because media players and hbmenu can launch from anywhere
        // and the extension is enough info to know what type of file it is.
        // whereas with roms, a .iso can be used for multiple systems, so it needs
        // to be in the correct folder, ie psx, to know what system that .iso is for.
        for (const auto& assoc : m_assoc_entries) {
            if (assoc.database.empty()) {
                if (assoc.IsExtension(extension, internal_extension)) {
                    log_write("found ext: %s\n", assoc.path.s);
                    out_entries.emplace_back(assoc);
                }
            }
        }
    }

    return out_entries;
}

void Menu::LoadAssocEntriesPath(const fs::FsPath& path) {
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
        if (!ext || strcasecmp(ext, ".ini")) {
            continue;
        }

        const auto full_path = GetNewPath(path, d->d_name);
        FileAssocEntry assoc{};

        ini_browse([](const mTCHAR *Section, const mTCHAR *Key, const mTCHAR *Value, void *UserData) {
            auto assoc = static_cast<FileAssocEntry*>(UserData);
            if (!std::strcmp(Key, "path")) {
                assoc->path = Value;
            } else if (!std::strcmp(Key, "supported_extensions")) {
                for (const auto& p : std::views::split(std::string_view{Value}, '|')) {
                    if (p.empty()) {
                        continue;
                    }
                    assoc->ext.emplace_back(p.data(), p.size());
                }
            } else if (!std::strcmp(Key, "database")) {
                for (const auto& p : std::views::split(std::string_view{Value}, '|')) {
                    if (p.empty()) {
                        continue;
                    }
                    assoc->database.emplace_back(p.data(), p.size());
                }
            } else if (!std::strcmp(Key, "use_base_name")) {
                if (!std::strcmp(Value, "true") || !std::strcmp(Value, "1")) {
                    assoc->use_base_name = true;
                }
            }
            return 1;
        }, &assoc, full_path);

        if (assoc.ext.empty()) {
            continue;
        }

        assoc.name.assign(d->d_name, ext - d->d_name);

        // if path isn't empty, check if the file exists
        bool file_exists{};
        if (!assoc.path.empty()) {
            file_exists = view->m_fs->FileExists(assoc.path);
        } else {
            const auto nro_name = assoc.name + ".nro";
            for (const auto& nro : homebrew::GetNroEntries()) {
                const auto len = std::strlen(nro.path);
                if (len < nro_name.length()) {
                    continue;
                }
                if (!strcasecmp(nro.path + len - nro_name.length(), nro_name.c_str())) {
                    assoc.path = nro.path;
                    file_exists = true;
                    break;
                }
            }
        }

        // after all of that, the file doesn't exist :(
        if (!file_exists) {
            // log_write("removing: %s\n", assoc.name.c_str());
            continue;
        }

        // log_write("\tpath: %s\n", assoc.path.s);
        // log_write("\tname: %s\n", assoc.name.c_str());
        // for (const auto& ext : assoc.ext) {
        //     log_write("\t\text: %s\n", ext.c_str());
        // }
        // for (const auto& db : assoc.database) {
        //     log_write("\t\tdb: %s\n", db.c_str());
        // }

        m_assoc_entries.emplace_back(assoc);
    }
}

void Menu::LoadAssocEntries() {
    // load from romfs first
    if (R_SUCCEEDED(romfsInit())) {
        LoadAssocEntriesPath("romfs:/assoc/");
        romfsExit();
    }
    // then load custom entries
    LoadAssocEntriesPath("/config/sphaira/assoc/");
}

void Menu::UpdateSubheading() {
    const auto index = view->m_entries_current.empty() ? 0 : view->m_index + 1;
    this->SetSubHeading(std::to_string(index) + " / " + std::to_string(view->m_entries_current.size()));
}

void Menu::SetSplitScreen(bool enable) {
    if (m_split_screen != enable) {
        m_split_screen = enable;

        if (m_split_screen) {
            const auto change_view = [this](auto new_view){
                if (view != new_view) {
                    view->OnFocusLost();
                    view = new_view;
                    view->OnFocusGained();
                    SetTitleSubHeading(view->m_path);
                    UpdateSubheading();
                }
            };

            // load second screen as a copy of the left side.
            view->SetSide(ViewSide::Left);
            view_right = std::make_shared<FsView>(this, view->m_path, view->GetFsEntry(), ViewSide::Right);
            change_view(view_right);

            SetAction(Button::LEFT, Action{[this, change_view](){
                change_view(view_left);
            }});
            SetAction(Button::RIGHT, Action{[this, change_view](){
                change_view(view_right);
            }});
        } else {
            view_left = {};
            view_right = {};
            view_left = view;
            view->SetSide(ViewSide::Left);

            RemoveAction(Button::LEFT);
            RemoveAction(Button::RIGHT);
            ResetSelection();
        }
    }
}

void Menu::RefreshViews() {
    ResetSelection();

    if (IsSplitScreen()) {
        view_left->Scan(view_left->m_path);
        view_right->Scan(view_right->m_path);
    } else {
        view->Scan(view->m_path);
    }
}

void Menu::PromptIfShouldExit() {
    if (IsTab()) {
        return;
    }

    App::Push(std::make_shared<ui::OptionBox>(
        "Close FileBrowser?"_i18n,
        "No"_i18n, "Yes"_i18n, 1, [this](auto op_index){
            if (op_index && *op_index) {
                SetPop();
            }
        }
    ));
}

} // namespace sphaira::ui::menu::filebrowser
