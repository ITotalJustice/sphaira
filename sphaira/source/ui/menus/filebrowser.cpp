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

#include <minIni.h>
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
#include <stack>

namespace sphaira::ui::menu::filebrowser {
namespace {

struct FsDirCollection {
    fs::FsPath path;
    fs::FsPath parent_name;
    std::vector<FsDirectoryEntry> files;
    std::vector<FsDirectoryEntry> dirs;
};

struct ExtDbEntry {
    std::string_view db_name;
    std::span<const std::string_view> ext;
};

using FsDirCollections = std::vector<FsDirCollection>;

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

using PathPair = std::pair<std::string_view, std::string_view>;
constexpr PathPair PATHS[]{
    PathPair{"3do", "The 3DO Company - 3DO"},
    PathPair{"atari800", "Atari - 8-bit"},
    PathPair{"atari2600", "Atari - 2600"},
    PathPair{"atari5200", "Atari - 5200"},
    PathPair{"atari7800", "Atari - 7800"},
    PathPair{"atarilynx", "Atari - Lynx"},
    PathPair{"atarijaguar", "Atari - Jaguar"},
    PathPair{"atarijaguarcd", ""},
    PathPair{"n3ds", "Nintendo - Nintendo 3DS"},
    PathPair{"n64", "Nintendo - Nintendo 64"},
    PathPair{"nds", "Nintendo - Nintendo DS"},
    PathPair{"fds", "Nintendo - Famicom Disk System"},
    PathPair{"nes", "Nintendo - Nintendo Entertainment System"},
    PathPair{"pokemini", "Nintendo - Pokemon Mini"},
    PathPair{"gb", "Nintendo - Game Boy"},
    PathPair{"gba", "Nintendo - Game Boy Advance"},
    PathPair{"gbc", "Nintendo - Game Boy Color"},
    PathPair{"virtualboy", "Nintendo - Virtual Boy"},
    PathPair{"gameandwatch", ""},
    PathPair{"sega32x", "Sega - 32X"},
    PathPair{"segacd", "Sega - Mega CD - Sega CD"},
    PathPair{"dreamcast", "Sega - Dreamcast"},
    PathPair{"gamegear", "Sega - Game Gear"},
    PathPair{"genesis", "Sega - Mega Drive - Genesis"},
    PathPair{"mastersystem", "Sega - Master System - Mark III"},
    PathPair{"megadrive", "Sega - Mega Drive - Genesis"},
    PathPair{"saturn", "Sega - Saturn"},
    PathPair{"sg-1000", "Sega - SG-1000"},
    PathPair{"psx", "Sony - PlayStation"},
    PathPair{"psp", "Sony - PlayStation Portable"},
    PathPair{"snes", "Nintendo - Super Nintendo Entertainment System"},
    PathPair{"pico8", "Sega - PICO"},
    PathPair{"wonderswan", "Bandai - WonderSwan"},
    PathPair{"wonderswancolor", "Bandai - WonderSwan Color"},
};

constexpr fs::FsPath DAYBREAK_PATH{"/switch/daybreak.nro"};

constexpr const char* SORT_STR[] = {
    "Size",
    "Alphabetical",
};

constexpr const char* ORDER_STR[] = {
    "Desc",
    "Asc",
};

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
auto GetRomDatabaseFromPath(const std::string& path) -> std::string {
    if (path.length() <= 1) {
        return {};
    }

    // this won't fail :)
    const auto db_name = path.substr(path.find_last_of('/') + 1);
    log_write("new path: %s\n", db_name.c_str());

    for (auto& [folder_name, database_name] : PATHS) {
        if ((
            folder_name.length() == db_name.length() && !strncasecmp(folder_name.data(), db_name.data(), folder_name.length())) ||
            (database_name.length() == db_name.length() && !strncasecmp(database_name.data(), db_name.data(), database_name.length()))) {
            log_write("found it :) %s\n", std::string{database_name.data(), database_name.length()}.c_str());
            return std::string{database_name.data(), database_name.length()};
        }
    }

    // if we failed, try again but with the folder about
    // "/roms/psx/scooby-doo/scooby-doo.bin", this will check psx
    const auto last_off = path.substr(0, path.find_last_of('/'));
    if (const auto off = last_off.find_last_of('/'); off != std::string::npos) {
        const auto db_name2 = last_off.substr(off + 1);
        printf("got db: %s\n", db_name2.c_str());
        for (auto& [folder_name, database_name] : PATHS) {
            if ((
                folder_name.length() == db_name2.length() && !strcasecmp(folder_name.data(), db_name2.data())) ||
                (database_name.length() == db_name2.length() && !strcasecmp(database_name.data(), db_name2.data()))) {
                log_write("found it :) %s\n", std::string{database_name.data(), database_name.length()}.c_str());
                return std::string{database_name.data(), database_name.length()};
            }
        }
    }

    return {};
}

//
auto GetRomIcon(ProgressBox* pbox, std::string filename, std::string extension, std::string database, const NroEntry& nro) {
    // if no db entries, use nro icon
    if (database.empty()) {
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

    const std::string system_name = database;//GetDatabaseFromExt(database, extension);
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
        pbox->NewTransfer("Trying to load " + ra_thumbnail_path);
        std::vector<u8> image_file;
        if (R_SUCCEEDED(fs::FsNativeSd().read_entire_file(ra_thumbnail_path.c_str(), image_file))) {
            return image_file;
        }
    }

    // try and download icon
    if (!pbox->ShouldExit()) {
        // auto png_image = DownloadMemory(ra_thumbnail_url.c_str());
        pbox->NewTransfer("Downloading "_i18n + gh_thumbnail_url);
        auto png_image = DownloadMemory(gh_thumbnail_url, "", [pbox](u32 dltotal, u32 dlnow, u32 ultotal, u32 ulnow){
            if (pbox->ShouldExit()) {
                return false;
            }
            pbox->UpdateTransfer(dlnow, dltotal);
            return true;
        });
        if (!png_image.empty()) {
            return png_image;
        }
    }

    // use nro icon
    log_write("using nro image\n");
    return nro_get_icon(nro.path, nro.icon_size, nro.icon_offset);
}

// returns 0 if true
auto CheckIfUpdateFolder(const fs::FsPath& path, std::span<FileEntry> entries) -> Result {
    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    // check that we have daybreak installed
    R_UNLESS(fs.FileExists(DAYBREAK_PATH), FsError_FileNotFound);

    FsDir d;
    R_TRY(fs.OpenDirectory(path, FsDirOpenMode_ReadDirs, &d));
    ON_SCOPE_EXIT(fs.DirClose(&d));

    s64 count;
    R_TRY(fs.DirGetEntryCount(&d, &count));

    // check that we are at the bottom level
    R_UNLESS(count == 0, 0x1);
    // check that we have enough ncas and not too many
    R_UNLESS(entries.size() > 150 && entries.size() < 300, 0x1);

    // check that all entries end in .nca
    const auto nca_ext = std::string_view{".nca"};
    for (auto& e : entries) {
        const auto ext = std::strrchr(e.name, '.');
        R_UNLESS(ext && ext == nca_ext, 0x1);
    }

    R_SUCCEED();
}

auto get_collection(fs::FsNative& fs, const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollection& out, bool inc_file, bool inc_dir, bool inc_size) -> Result {
    out.path = path;
    out.parent_name = parent_name;

    const auto fetch = [&path, &fs](std::vector<FsDirectoryEntry>& out, u32 flags) -> Result {
        FsDir d;
        R_TRY(fs.OpenDirectory(path, flags, &d));
        ON_SCOPE_EXIT(fs.DirClose(&d));

        s64 count;
        R_TRY(fs.DirGetEntryCount(&d, &count));

        out.resize(count);
        return fs.DirRead(&d, &count, out.size(), out.data());
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

auto get_collections(fs::FsNative& fs, const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollections& out) -> Result {
    // get a list of all the files / dirs
    FsDirCollection collection;
    R_TRY(get_collection(fs, path, parent_name, collection, true, true, false));
    log_write("got collection: %s parent_name: %s files: %zu dirs: %zu\n", path, parent_name, collection.files.size(), collection.dirs.size());
    out.emplace_back(collection);

    // for (size_t i = 0; i < collection.dirs.size(); i++) {
    for (const auto&p : collection.dirs) {
        // use heap as to not explode the stack
        const auto new_path = std::make_unique<fs::FsPath>(Menu::GetNewPath(path, p.name));
        const auto new_parent_name = std::make_unique<fs::FsPath>(Menu::GetNewPath(parent_name, p.name));
        log_write("trying to get nested collection: %s parent_name: %s\n", *new_path, *new_parent_name);
        R_TRY(get_collections(fs, *new_path, *new_parent_name, out));
    }

    R_SUCCEED();
}

auto get_collections(const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollections& out) -> Result {
    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    return get_collections(fs, path, parent_name, out);
}

} // namespace

Menu::Menu(const std::vector<NroEntry>& nro_entries) : MenuBase{"FileBrowser"_i18n}, m_nro_entries{nro_entries} {
    this->SetActions(
        std::make_pair(Button::R2, Action{[this](){
            if (!m_selected_files.empty()) {
                ResetSelection();
            }

            GetEntry().selected ^= 1;
            if (GetEntry().selected) {
                m_selected_count++;
            } else {
                m_selected_count--;
            }
        }}),
        std::make_pair(Button::DOWN, Action{[this](){
            if (m_index < (m_entries_current.size() - 1)) {
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
        std::make_pair(Button::A, Action{"Open"_i18n, [this](){
            if (m_entries_current.empty()) {
                return;
            }

            if (m_is_update_folder) {
                App::Push(std::make_shared<OptionBox>("Open with DayBreak?"_i18n, "No"_i18n, "Yes"_i18n, 1, [this](auto op_index){
                    if (op_index && *op_index) {
                        // daybreak uses native fs so do not use nro_add_arg_file
                        // otherwise it'll fail to open the folder...
                        nro_launch(DAYBREAK_PATH, nro_add_arg(m_path));
                    }
                }));
                return;
            }

            const auto& entry = GetEntry();

            if (entry.type == FsDirEntryType_Dir) {
                Scan(GetNewPathCurrent());
            } else {
                // special case for nro
                if (entry.GetExtension() == "nro") {
                    App::Push(std::make_shared<OptionBox>("Launch "_i18n + entry.GetName() + '?',
                        "No"_i18n, "Launch"_i18n, 1, [this](auto op_index){
                            if (op_index && *op_index) {
                                nro_launch(GetNewPathCurrent());
                            }
                        }));
                } else {
                    const auto assoc_list = FindFileAssocFor();
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
            std::string_view view{m_path};
            if (view != "/") {
                const auto end = view.find_last_of('/');
                assert(end != view.npos);

                if (end == 0) {
                    Scan("/", true);
                } else {
                    Scan(view.substr(0, end), true);
                }
            }
        }}),

        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("File Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            options->Add(std::make_shared<SidebarEntryCallback>("Sort By"_i18n, [this](){
                auto options = std::make_shared<Sidebar>("Sort Options"_i18n, Sidebar::Side::RIGHT);
                ON_SCOPE_EXIT(App::Push(options));

                SidebarEntryArray::Items sort_items;
                sort_items.push_back("Size"_i18n);
                sort_items.push_back("Alphabetical"_i18n);

                SidebarEntryArray::Items order_items;
                order_items.push_back("Decending"_i18n);
                order_items.push_back("Ascending"_i18n);

                options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this, sort_items](std::size_t& index_out){
                    m_sort.Set(index_out);
                    SortAndFindLastFile();
                }, m_sort.Get()));

                options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this, order_items](std::size_t& index_out){
                    m_order.Set(index_out);
                    SortAndFindLastFile();
                }, m_order.Get()));

                options->Add(std::make_shared<SidebarEntryBool>("Show Hidden"_i18n, m_show_hidden.Get(), [this](bool& v_out){
                    m_show_hidden.Set(v_out);
                    SortAndFindLastFile();
                }, "Yes"_i18n, "No"_i18n));

                options->Add(std::make_shared<SidebarEntryBool>("Folders First"_i18n, m_folders_first.Get(), [this](bool& v_out){
                    m_folders_first.Set(v_out);
                    SortAndFindLastFile();
                }, "Yes"_i18n, "No"_i18n));

                options->Add(std::make_shared<SidebarEntryBool>("Hidden Last"_i18n, m_hidden_last.Get(), [this](bool& v_out){
                    m_hidden_last.Set(v_out);
                    SortAndFindLastFile();
                }, "Yes"_i18n, "No"_i18n));
            }));

            if (m_entries_current.size()) {
                options->Add(std::make_shared<SidebarEntryCallback>("Cut"_i18n, [this](){
                    if (!m_selected_count) {
                        AddCurrentFileToSelection(SelectedType::Cut);
                    } else {
                        AddSelectedEntries(SelectedType::Cut);
                    }
                }, true));

                options->Add(std::make_shared<SidebarEntryCallback>("Copy"_i18n, [this](){
                    if (!m_selected_count) {
                        AddCurrentFileToSelection(SelectedType::Copy);
                    } else {
                        AddSelectedEntries(SelectedType::Copy);
                    }
                }, true));

                options->Add(std::make_shared<SidebarEntryCallback>("Delete"_i18n, [this](){
                    if (!m_selected_count) {
                        AddCurrentFileToSelection(SelectedType::Delete);
                    } else {
                        AddSelectedEntries(SelectedType::Delete);
                    }
                    log_write("clicked on delete\n");
                    App::Push(std::make_shared<OptionBox>(
                        "Delete Selected files?"_i18n, "No"_i18n, "Yes"_i18n, 1, [this](auto op_index){
                            if (op_index && *op_index) {
                                OnDeleteCallback();
                            }
                        }
                    ));
                    log_write("pushed delete\n");
                }, true));
            }

            if (!m_selected_files.empty() && (m_selected_type == SelectedType::Cut || m_selected_type == SelectedType::Copy)) {
                options->Add(std::make_shared<SidebarEntryCallback>("Paste"_i18n, [this](){
                    const std::string buf = "Paste "_i18n + std::to_string(m_selected_files.size()) + " file(s)?"_i18n;
                    App::Push(std::make_shared<OptionBox>(
                        buf, "No"_i18n, "Yes"_i18n, 1, [this](auto op_index){
                        if (op_index && *op_index) {
                            OnPasteCallback();
                        }
                    }));
                }, true));
            }

            // can't rename more than 1 file
            if (m_entries_current.size() && !m_selected_count) {
                options->Add(std::make_shared<SidebarEntryCallback>("Rename"_i18n, [this](){
                    std::string out;
                    const auto& entry = GetEntry();
                    const auto name = entry.GetName();
                    if (R_SUCCEEDED(swkbd::ShowText(out, "Set New File Name"_i18n.c_str(), name.c_str())) && !out.empty() && out != name) {
                        const auto src_path = GetNewPath(entry);
                        const auto dst_path = GetNewPath(m_path, out);

                        Result rc;
                        if (entry.IsFile()) {
                            rc = fs::FsNativeSd().RenameFile(src_path, dst_path);
                        } else {
                            rc = fs::FsNativeSd().RenameDirectory(src_path, dst_path);
                        }

                        if (R_SUCCEEDED(rc)) {
                            Scan(m_path);
                        } else {
                            char msg[FS_MAX_PATH];
                            std::snprintf(msg, sizeof(msg), "Failed to rename file: %s", entry.name);
                            App::Push(std::make_shared<ErrorBox>(rc, msg));
                        }
                    }
                }, true));
            }

            options->Add(std::make_shared<SidebarEntryCallback>("Advanced"_i18n, [this](){
                auto options = std::make_shared<Sidebar>("Advanced Options"_i18n, Sidebar::Side::RIGHT);
                ON_SCOPE_EXIT(App::Push(options));

                options->Add(std::make_shared<SidebarEntryCallback>("Create File"_i18n, [this](){
                    std::string out;
                    if (R_SUCCEEDED(swkbd::ShowText(out, "Set File Name"_i18n.c_str())) && !out.empty()) {
                        fs::FsPath full_path;
                        if (out[0] == '/') {
                            full_path = out;
                        } else {
                            full_path = fs::AppendPath(m_path, out);
                        }

                        fs::FsNativeSd fs;
                        fs.CreateDirectoryRecursivelyWithPath(full_path);
                        if (R_SUCCEEDED(fs.CreateFile(full_path, 0, 0))) {
                            log_write("created file: %s\n", full_path);
                            Scan(m_path);
                        } else {
                            log_write("failed to create file: %s\n", full_path);
                        }
                    }
                }, true));

                options->Add(std::make_shared<SidebarEntryCallback>("Create Folder"_i18n, [this](){
                    std::string out;
                    if (R_SUCCEEDED(swkbd::ShowText(out, "Set Folder Name"_i18n.c_str())) && !out.empty()) {
                        fs::FsPath full_path;
                        if (out[0] == '/') {
                            full_path = out;
                        } else {
                            full_path = fs::AppendPath(m_path, out);
                        }

                        if (R_SUCCEEDED(fs::FsNativeSd().CreateDirectoryRecursively(full_path))) {
                            log_write("created dir: %s\n", full_path);
                            Scan(m_path);
                        } else {
                            log_write("failed to create dir: %s\n", full_path);
                        }
                    }
                }, true));

                if (m_entries_current.size()) {
                    options->Add(std::make_shared<SidebarEntryCallback>("View as text (unfinished)"_i18n, [this](){
                        App::Push(std::make_shared<fileview::Menu>(GetNewPathCurrent()));
                    }, true));
                }

                if (m_entries_current.size()) {
                    if (HasTypeInSelectedEntries(FsDirEntryType_File) && !m_selected_count && (GetEntry().GetExtension() == "nro" || !FindFileAssocFor().empty())) {
                        options->Add(std::make_shared<SidebarEntryCallback>("Install Forwarder"_i18n, [this](){;
                            App::Push(std::make_shared<OptionBox>(
                                "WARNING: Installing forwarders will lead to a ban!"_i18n,
                                "Back"_i18n, "Install"_i18n, 0, [this](auto op_index){
                                    if (op_index && *op_index) {
                                        if (GetEntry().GetExtension() == "nro") {
                                            if (R_FAILED(homebrew::Menu::InstallHomebrewFromPath(GetNewPathCurrent()))) {
                                                log_write("failed to create forwarder\n");
                                            }
                                        } else {
                                            InstallForwarder();
                                        }
                                    }
                                }
                            ));
                        }));
                    }

                    if (HasTypeInSelectedEntries(FsDirEntryType_Dir)) {
                        options->Add(std::make_shared<SidebarEntryCallback>("Set Archive Bit"_i18n, [this](){
                            if (!m_selected_count) {
                                AddCurrentFileToSelection(SelectedType::None);
                            } else {
                                AddSelectedEntries(SelectedType::None);
                            }

                            App::Push(std::make_shared<OptionBox>(
                                "Warning! Setting the archive bit cannot be undone!"_i18n,
                                "No"_i18n, "Yes"_i18n, 1, [this](auto op_index) {
                                    if (op_index && *op_index == 1) {
                                        bool re_sort{};
                                        for (const auto&p : m_selected_files) {
                                            const auto file_path = GetNewPath(m_selected_path, p.name);
                                            if (p.IsDir()) {
                                                fs::FsNativeSd fs;
                                                if (R_SUCCEEDED(fsFsSetConcatenationFileAttribute(&fs.m_fs, file_path))) {
                                                    log_write("updated timestamp\n");
                                                    re_sort |= true;
                                                } else {
                                                    log_write("failed to set concatenation file attribute\n");
                                                }
                                            }
                                        }

                                        if (re_sort) {
                                            Scan(m_path);
                                        }
                                    }
                                }
                            ));

                            ResetSelection();
                        }, true));
                    }
                }
            }));
        }})
    );

    fs::FsPath buf;
    ini_gets("paths", "last_path", "/", buf, sizeof(buf), App::CONFIG_PATH);
    m_path = buf;
}

Menu::~Menu() {
    ini_puts("paths", "last_path", m_path, App::CONFIG_PATH);
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    const auto& text_col = theme->elements[ThemeEntryID_TEXT].colour;

    if (m_entries_current.empty()) {
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, text_col, "Empty..."_i18n.c_str());
        return;
    }

    const u64 SCROLL = m_index_offset;
    constexpr u64 max_entry_display = 8;
    const u64 entry_total = m_entries_current.size();

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

    for (std::size_t i = m_index_offset; i < m_entries_current.size(); i++) {
        auto& e = GetEntry(i);

        if (e.IsDir()) {
            if (e.file_count == -1 && e.dir_count == -1) {
                const auto full_path = GetNewPath(e);
                fs::FsNativeSd fs;
                fs.DirGetEntryCount(full_path, FsDirOpenMode_ReadFiles | FsDirOpenMode_NoFileSize, &e.file_count);
                fs.DirGetEntryCount(full_path, FsDirOpenMode_ReadDirs | FsDirOpenMode_NoFileSize, &e.dir_count);
            }
        } else if (!e.checked_extension) {
            e.checked_extension = true;
            if (auto ext = std::strrchr(e.name, '.')) {
                e.extension = ext+1;
            }
        }

        if (e.IsSelected()) {
            // gfx::drawText(vg, x - 60.f, y + (h / 2.f) - (48.f / 2), 48.f, "\uE14B", nullptr, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, gfx::Colour::CYAN);
            gfx::drawText(vg, x, y + (h / 2.f) - (24.f / 2), 24.f, "\uE14B", nullptr, NVG_ALIGN_RIGHT | NVG_ALIGN_TOP, gfx::Colour::CYAN);
        }

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
            } else if (IsExtension(ext, "zip")) {
                icon = ThemeEntryID_ICON_ZIP;
            } else if (IsExtension(ext, "nro")) {
                icon = ThemeEntryID_ICON_NRO;
            }

            DrawElement(x + text_xoffset, y + 5, 50, 50, icon);
        }

        nvgSave(vg);
        const auto txt_clip = std::min(GetY() + GetH(), y + h) - y;
        nvgScissor(vg, x + text_xoffset+65, y, w-(x+text_xoffset+65+50), txt_clip);
            gfx::drawText(vg, x + text_xoffset+65, y + (h / 2.f), 20.f, e.name, NULL, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->elements[text_id].colour);
        nvgRestore(vg);

        if (e.IsDir()) {
            gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) - 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_BOTTOM, theme->elements[text_id].colour, "%zd files"_i18n.c_str(), e.file_count);
            gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) + 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_TOP, theme->elements[text_id].colour, "%zd dirs"_i18n.c_str(), e.dir_count);
        } else {
            if (!e.time_stamp.is_valid) {
                fs::FsNativeSd fs;
                const auto path = GetNewPath(e);

                if (R_SUCCEEDED(fs.GetFsOpenResult())) {
                    fs.GetFileTimeStampRaw(path, &e.time_stamp);
                }
            }
            const auto t = (time_t)(e.time_stamp.modified);
            struct tm tm{};
            localtime_r(&t, &tm);
            gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) + 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_TOP, theme->elements[text_id].colour, "%02u/%02u/%u", tm.tm_mday, tm.tm_mon + 1, tm.tm_year + 1900);
            if ((double)e.file_size / 1024.0 / 1024.0 <= 0.009) {
                gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) - 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_BOTTOM, theme->elements[text_id].colour, "%.2f KiB", (double)e.file_size / 1024.0);
            } else {
                gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) - 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_BOTTOM, theme->elements[text_id].colour, "%.2f MiB", (double)e.file_size / 1024.0 / 1024.0);
            }
        }

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
        if (m_path.empty()) {
            Scan("/");
        } else {
            Scan(m_path);
        }
    }

    if (!m_loaded_assoc_entries) {
        m_loaded_assoc_entries = true;
        log_write("loading assoc entries\n");
        LoadAssocEntries();
    }
}

void Menu::SetIndex(std::size_t index) {
    m_index = index;
    if (!m_index) {
        m_index_offset = 0;
    }

    if (!m_entries_current.empty() && !GetEntry().checked_internal_extension && GetEntry().extension == "zip") {
        GetEntry().checked_internal_extension = true;

        if (auto zfile = unzOpen64(GetNewPathCurrent())) {
            ON_SCOPE_EXIT(unzClose(zfile));
            unz_global_info gi{};
            // only check first entry (i think RA does the same)
            if (UNZ_OK == unzGetGlobalInfo(zfile, &gi) && gi.number_entry >= 1) {
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
    }

    UpdateSubheading();
}

void Menu::InstallForwarder() {
    const auto assoc_list = FindFileAssocFor();
    if (assoc_list.empty()) {
        log_write("failed to find assoc for: %s ext: %s\n", GetEntry().name, GetEntry().extension.c_str());
        return;
    }

    PopupList::Items items;
    for (const auto&p : assoc_list) {
        items.emplace_back(p.name);
    }

    const auto title = std::string{"Select launcher for: "} + GetEntry().name;
    App::Push(std::make_shared<PopupList>(
        title, items, [this, assoc_list](auto op_index){
            if (op_index) {
                const auto assoc = assoc_list[*op_index];
                log_write("pushing it\n");
                App::Push(std::make_shared<ProgressBox>("Installing Forwarder"_i18n, [assoc, this](auto pbox) -> bool {
                    log_write("inside callback\n");

                    NroEntry nro{};
                    log_write("parsing nro\n");
                    if (R_FAILED(nro_parse(assoc.path, nro))) {
                        log_write("failed nro parse\n");
                        return false;
                    }
                    log_write("got nro data\n");
                    std::string file_name = GetEntry().GetInternalName();
                    std::string extension = GetEntry().GetInternalExtension();

                    if (auto pos = file_name.find_last_of('.'); pos != std::string::npos) {
                        log_write("got filename\n");
                        file_name = file_name.substr(0, pos);
                        log_write("got filename2: %s\n\n", file_name.c_str());
                    }

                    const auto database = GetRomDatabaseFromPath(m_path);

                    OwoConfig config{};
                    config.nro_path = assoc.path.toString();
                    config.args = nro_add_arg_file(GetNewPathCurrent());
                    config.name = nro.nacp.lang[0].name + std::string{" | "} + file_name;
                    // config.name = file_name;
                    config.nacp = nro.nacp;
                    config.icon = GetRomIcon(pbox, file_name, extension, database, nro);

                    return R_SUCCEEDED(App::Install(pbox, config));
                }));
            } else {
                log_write("pressed B to skip launch...\n");
            }
        }
    ));
}

auto Menu::Scan(const fs::FsPath& new_path, bool is_walk_up) -> Result {
    log_write("new scan path: %s\n", new_path);
    if (!is_walk_up && !m_path.empty() && !m_entries_current.empty()) {
        const LastFile f{GetEntry().name, m_index, m_index_offset, m_entries_current.size()};
        m_previous_highlighted_file.emplace_back(f);
    }

    log_write("\nold index: %zu start: %zu\n", m_index, m_index_offset);

    m_path = new_path;
    m_entries.clear();
    m_index = 0;
    m_index_offset = 0;
    SetTitleSubHeading(m_path);

    if (m_selected_type == SelectedType::None) {
        ResetSelection();
    }

    // fs::FsNativeSd fs;
    // R_TRY(fs.GetFsOpenResult());

    // FsDirCollection collection;
    // R_TRY(get_collection(fs, new_path, "", collection, true, true, false));

    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    FsDir d;
    R_TRY(fsFsOpenDirectory(&fs.m_fs, new_path, FsDirOpenMode_ReadDirs | FsDirOpenMode_ReadFiles | FsDirOpenMode_NoFileSize, &d));
    ON_SCOPE_EXIT(fsDirClose(&d));

    s64 count;
    R_TRY(fs.DirGetEntryCount(&d, &count));

    // we won't run out of memory here (tm)
    std::vector<FsDirectoryEntry> dir_entries(count);

    R_TRY(fs.DirRead(&d, &count, dir_entries.size(), dir_entries.data()));

    // size may of changed
    dir_entries.resize(count);
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
    m_is_update_folder = R_SUCCEEDED(CheckIfUpdateFolder(new_path, m_entries));

    SetIndex(0);

    // find previous entry
    if (is_walk_up && !m_previous_highlighted_file.empty()) {
        ON_SCOPE_EXIT(m_previous_highlighted_file.pop_back());
        SetIndexFromLastFile(m_previous_highlighted_file.back());
    }

    R_SUCCEED();
}

auto Menu::FindFileAssocFor() -> std::vector<FileAssocEntry> {
    // only support roms in correctly named folders, sorry!
    const auto database_name = GetRomDatabaseFromPath(m_path);
    const auto& entry = GetEntry();
    const auto extension = entry.internal_extension.empty() ? entry.extension : entry.internal_extension;
    if (extension.empty()) {
        log_write("failed to get extension for db: %s path: %s\n", database_name.c_str(), m_path);
        return {};
    }

    log_write("got extension for db: %s path: %s\n", database_name.c_str(), m_path);


    std::vector<FileAssocEntry> out_entries;
    if (!database_name.empty()) {
        // if database isn't empty, then we are in a valid folder
        // search for an entry that matches the db and ext
        for (const auto& assoc : m_assoc_entries) {
            for (const auto& assoc_db : assoc.database) {
                if (assoc_db == database_name) {
                    for (const auto& assoc_ext : assoc.ext) {
                        if (assoc_ext == extension) {
                            log_write("found ext: %s assoc_ext: %s assoc.ext: %s\n", assoc.path, assoc_ext.c_str(), extension.c_str());
                            out_entries.emplace_back(assoc);
                        }
                    }
                }
            }
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
                for (const auto& assoc_ext : assoc.ext) {
                    if (assoc_ext == extension) {
                        log_write("found ext: %s\n", assoc.path);
                        out_entries.emplace_back(assoc);
                    }
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

        // const std::string name = d->d_name;
        const auto ext = std::strrchr(d->d_name, '.');
        if (!ext) {
            continue;
        }
        if (strcasecmp(ext, ".ini")) {
            continue;
        }

        const auto full_path = GetNewPath(path, d->d_name);

        fs::FsPath buf{};
        const auto ext_len = 1 + ini_gets("config", "supported_extensions", "", buf, sizeof(buf) - 1, full_path);
        if (ext_len <= 1) {
            continue;
        }

        // log_write("reading ini: %s\n", name.c_str());

        FileAssocEntry assoc{};

        for (int i = 0; i < ext_len; i++) {
            for (int j = i; j < ext_len; j++) {
                if (buf[j] == '|' || buf[j] == '\0') {
                    assoc.ext.emplace_back(buf + i, j - i);
                    i += j - i;
                    break;
                }
            }
        }

        if (assoc.ext.empty()) {
            continue;
        }

        // assoc.name = name.substr(0, name.find_last_of('.'));
        assoc.name.assign(d->d_name, ext - d->d_name);

        const auto path_len = ini_gets("config", "path", "", buf, sizeof(buf) - 1, full_path);
        if (path_len > 0) {
            assoc.path = buf;
        }

        const auto database_len = 1 + ini_gets("config", "database", "", buf, sizeof(buf) - 1, full_path);
        if (database_len > 1) {
            for (int i = 0; i < database_len; i++) {
                for (int j = i; j < database_len; j++) {
                    if (buf[j] == '|' || buf[j] == '\0') {
                        assoc.database.emplace_back(buf + i, j - i);
                        i += j - i;
                        break;
                    }
                }
            }
        }

        bool file_exists{};

        // if path isn't empty, check if the file exists
        if (!assoc.path.empty()) {
            file_exists = fs::FsNativeSd().FileExists(assoc.path);
        } else {
            log_write("\tpath is empty\n");
            #if 1
            const auto nro_name = assoc.name + ".nro";
            for (const auto& nro : m_nro_entries) {
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
            #endif
        }

        // after all of that, the file doesn't exist :(
        if (!file_exists) {
            // log_write("removing: %s\n", assoc.name.c_str());
            continue;
        }

        // log_write("\tpath: %s\n", assoc.path.c_str());
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

#if 0
void Menu::OnIndexChange() {
    if (!GetEntry().checked_internal_extension && GetEntry().extension == "zip") {
        GetEntry().checked_internal_extension = true;
        if (auto zfile = unzOpen64(GetNewPathCurrent())) {
            ON_SCOPE_EXIT(unzClose(zfile));
            unz_global_info gi{};
            // only check first entry (i think RA does the same)
            if (UNZ_OK == unzGetGlobalInfo(zfile, &gi) && gi.number_entry >= 1) {
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
    }
}
#endif

void Menu::Sort() {
    // returns true if lhs should be before rhs
    const auto sort = m_sort.Get();
    const auto order = m_order.Get();
    const auto folders_first = m_folders_first.Get();
    const auto hidden_last = m_hidden_last.Get();

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
                } else if (order == OrderType_Decending) {
                    return lhs.file_size > rhs.file_size;
                } else {
                    return lhs.file_size < rhs.file_size;
                }
            } break;
            case SortType_Alphabetical: {
                if (order == OrderType_Decending) {
                    return strncasecmp(lhs.name, rhs.name, sizeof(lhs.name)) < 0;
                } else {
                    return strncasecmp(lhs.name, rhs.name, sizeof(lhs.name)) > 0;
                }
            } break;
        }

        std::unreachable();
    };

    if (m_show_hidden.Get()) {
        m_entries_current = m_entries_index_hidden;
    } else {
        m_entries_current = m_entries_index;
    }

    std::sort(m_entries_current.begin(), m_entries_current.end(), sorter);
}

void Menu::SortAndFindLastFile() {
    std::optional<LastFile> last_file;
    if (!m_path.empty() && !m_entries_current.empty()) {
        last_file = LastFile{GetEntry().name, m_index, m_index_offset, m_entries_current.size()};
    }

    Sort();

    if (last_file.has_value()) {
        SetIndexFromLastFile(*last_file);
    }
}

void Menu::SetIndexFromLastFile(const LastFile& last_file) {
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries_current.size(); i++) {
        if (last_file.name == GetEntry(i).name) {
            index = i;
            break;
        }
    }
    if (index >= 0) {
        if ((u64)index == last_file.index && m_entries_current.size() == last_file.entries_count) {
            m_index_offset = last_file.offset;
            log_write("index is the same as last time\n");
        } else {
            // file position changed!
            log_write("file position changed\n");
            // guesstimate where the position is
            if (index >= 8) {
                m_index_offset = (index - 8) + 1;
            } else {
                m_index_offset = 0;
            }
        }
        SetIndex(index);
        log_write("\nnew index: %zu start: %zu mod: %zu\n", m_index, m_index_offset, index % 8);
    }
}

void Menu::UpdateSubheading() {
    const auto index = m_entries_current.empty() ? 0 : m_index + 1;
    this->SetSubHeading(std::to_string(index) + " / " + std::to_string(m_entries_current.size()));
}

void Menu::OnDeleteCallback() {
    bool use_progress_box{true};

    // check if we only have 1 file / folder
    if (m_selected_files.size() == 1) {
        fs::FsNativeSd fs;
        const auto& entry = m_selected_files[0];
        const auto full_path = GetNewPath(m_selected_path, entry.name);

        if (entry.IsDir()) {
            s64 count{};
            fs.DirGetEntryCount(full_path, FsDirOpenMode_ReadDirs | FsDirOpenMode_ReadFiles | FsDirOpenMode_NoFileSize, &count);
            if (!count) {
                fs.DeleteDirectory(full_path);
                use_progress_box = false;
            }
        } else {
            fs.DeleteFile(full_path);
            use_progress_box = false;
        }
    }

    if (!use_progress_box) {
        ResetSelection();
        Scan(m_path);
        log_write("did delete\n");
    } else {
        App::Push(std::make_shared<ProgressBox>("Deleting"_i18n, [this](auto pbox){
            fs::FsNativeSd fs;
            FsDirCollections collections;

            // build list of dirs / files
            for (const auto&p : m_selected_files) {
                pbox->Yield();
                if (pbox->ShouldExit()) {
                    return false;
                }

                const auto full_path = GetNewPath(m_selected_path, p.name);
                if (p.IsDir()) {
                    pbox->NewTransfer("Scanning "_i18n + full_path);
                    if (R_FAILED(get_collections(full_path, p.name, collections))) {
                        log_write("failed to get dir collection: %s\n", full_path);
                        return false;
                    }
                }
            }

            // delete everything in collections, reversed
            for (const auto& c : std::views::reverse(collections)) {
                const auto delete_func = [&](auto& array) {
                    for (const auto& p : array) {
                        pbox->Yield();
                        if (pbox->ShouldExit()) {
                            return false;
                        }

                        const auto full_path = GetNewPath(c.path, p.name);
                        pbox->NewTransfer("Deleting "_i18n + full_path);
                        if (p.type == FsDirEntryType_Dir) {
                            log_write("deleting dir: %s\n", full_path);
                            fs.DeleteDirectory(full_path);
                        } else {
                            log_write("deleting file: %s\n", full_path);
                            fs.DeleteFile(full_path);
                        }
                    }
                    return true;
                };

                if (!delete_func(c.files)) {
                    return false;
                }
                if (!delete_func(c.dirs)) {
                    return false;
                }
            }

            for (const auto& p : m_selected_files) {
                pbox->Yield();
                if (pbox->ShouldExit()) {
                    return false;
                }

                const auto full_path = GetNewPath(m_selected_path, p.name);
                pbox->NewTransfer("Deleting "_i18n + full_path);

                if (p.IsDir()) {
                    log_write("deleting dir: %s\n", full_path);
                    fs.DeleteDirectory(full_path);
                } else {
                    log_write("deleting file: %s\n", full_path);
                    fs.DeleteFile(full_path);
                }
            }

            return true;
        }, [this](bool success){
            ResetSelection();
            Scan(m_path);
            log_write("did delete\n");
        }, 2));
    }
}

void Menu::OnPasteCallback() {
    bool use_progress_box{true};

    // check if we only have 1 file / folder and is cut (rename)
    if (m_selected_files.size() == 1 && m_selected_type == SelectedType::Cut) {
        fs::FsNativeSd fs;
        const auto& entry = m_selected_files[0];
        const auto full_path = GetNewPath(m_selected_path, entry.name);

        if (entry.IsDir()) {
            fs.RenameDirectory(full_path, GetNewPath(entry));
        } else {
            fs.RenameFile(full_path, GetNewPath(entry));
        }

        ResetSelection();
        Scan(m_path);
        log_write("did paste\n");
    } else {
        App::Push(std::make_shared<ProgressBox>("Pasting"_i18n, [this](auto pbox){
            fs::FsNativeSd fs;

            if (m_selected_type == SelectedType::Cut) {
                for (const auto& p : m_selected_files) {
                    pbox->Yield();
                    if (pbox->ShouldExit()) {
                        return false;
                    }

                    const auto src_path = GetNewPath(m_selected_path, p.name);
                    const auto dst_path = GetNewPath(m_path, p.name);

                    pbox->NewTransfer("Pasting "_i18n + src_path);

                    if (p.IsDir()) {
                        fs.RenameDirectory(src_path, dst_path);
                    } else {
                        fs.RenameFile(src_path, dst_path);
                    }
                }
            } else {
                FsDirCollections collections;

                // build list of dirs / files
                for (const auto&p : m_selected_files) {
                    pbox->Yield();
                    if (pbox->ShouldExit()) {
                        return false;
                    }

                    const auto full_path = GetNewPath(m_selected_path, p.name);
                    if (p.IsDir()) {
                        pbox->NewTransfer("Scanning "_i18n + full_path);
                        if (R_FAILED(get_collections(full_path, p.name, collections))) {
                            log_write("failed to get dir collection: %s\n", full_path);
                            return false;
                        }
                    }
                }

                for (const auto& p : m_selected_files) {
                    pbox->Yield();
                    if (pbox->ShouldExit()) {
                        return false;
                    }

                    const auto src_path = GetNewPath(m_selected_path, p.name);
                    const auto dst_path = GetNewPath(p);

                    if (p.IsDir()) {
                        pbox->NewTransfer("Creating "_i18n + dst_path);
                        fs.CreateDirectory(dst_path);
                    } else {
                        pbox->NewTransfer("Copying "_i18n + src_path);
                        R_TRY_RESULT(pbox->CopyFile(src_path, dst_path), false);
                    }
                }

                // copy everything in collections
                for (const auto& c : collections) {
                    const auto base_dst_path = GetNewPath(m_path, c.parent_name);

                    for (const auto& p : c.dirs) {
                        pbox->Yield();
                        if (pbox->ShouldExit()) {
                            return false;
                        }

                        const auto src_path = GetNewPath(c.path, p.name);
                        const auto dst_path = GetNewPath(base_dst_path, p.name);

                        log_write("creating: %s to %s\n", src_path, dst_path);
                        pbox->NewTransfer("Creating "_i18n + dst_path);
                        fs.CreateDirectory(dst_path);
                    }

                    for (const auto& p : c.files) {
                        pbox->Yield();
                        if (pbox->ShouldExit()) {
                            return false;
                        }

                        const auto src_path = GetNewPath(c.path, p.name);
                        const auto dst_path = GetNewPath(base_dst_path, p.name);

                        pbox->NewTransfer("Copying "_i18n + src_path);
                        log_write("copying: %s to %s\n", src_path, dst_path);
                        R_TRY_RESULT(pbox->CopyFile(src_path, dst_path), false);
                    }
                }
            }

            return true;
        }, [this](bool success){
            ResetSelection();
            Scan(m_path);
            log_write("did paste\n");
        }, 2));
    }
}

void Menu::OnRenameCallback() {

}

} // namespace sphaira::ui::menu::filebrowser

// options
// Cancel
// Skip
// Rename
// Overwrite
