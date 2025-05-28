#include "ui/menus/main_menu.hpp"

#include "ui/sidebar.hpp"
#include "ui/popup_list.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/error_box.hpp"

#include "ui/menus/homebrew.hpp"
#include "ui/menus/filebrowser.hpp"
#include "ui/menus/irs_menu.hpp"
#include "ui/menus/themezer.hpp"
#include "ui/menus/ghdl.hpp"
#include "ui/menus/usb_menu.hpp"
#include "ui/menus/ftp_menu.hpp"
#include "ui/menus/gc_menu.hpp"
#include "ui/menus/game_menu.hpp"
#include "ui/menus/appstore.hpp"

#include "app.hpp"
#include "log.hpp"
#include "download.hpp"
#include "defines.hpp"
#include "i18n.hpp"

#include <cstring>
#include <minizip/unzip.h>
#include <yyjson.h>

namespace sphaira::ui::menu::main {
namespace {

constexpr const char* GITHUB_URL{"https://api.github.com/repos/ITotalJustice/sphaira/releases/latest"};
constexpr fs::FsPath CACHE_PATH{"/switch/sphaira/cache/sphaira_latest.json"};

// paths where sphaira can be installed, used when updating
constexpr const fs::FsPath SPHAIRA_PATHS[]{
    "/hbmenu.nro",
    "/switch/sphaira.nro",
    "/switch/sphaira/sphaira.nro",
};

template<typename T>
auto MiscMenuFuncGenerator(u32 flags) {
    return std::make_shared<T>(flags);
}

const MiscMenuEntry MISC_MENU_ENTRIES[] = {
    { .name = "Appstore", .title = "Appstore", .func = MiscMenuFuncGenerator<ui::menu::appstore::Menu>, .flag = MiscMenuFlag_Shortcut },
    { .name = "Games", .title = "Games", .func = MiscMenuFuncGenerator<ui::menu::game::Menu>, .flag = MiscMenuFlag_Shortcut },
    { .name = "FileBrowser", .title = "FileBrowser", .func = MiscMenuFuncGenerator<ui::menu::filebrowser::Menu>, .flag = MiscMenuFlag_Shortcut },
    { .name = "Themezer", .title = "Themezer", .func = MiscMenuFuncGenerator<ui::menu::themezer::Menu>, .flag = MiscMenuFlag_Shortcut },
    { .name = "GitHub", .title = "GitHub", .func = MiscMenuFuncGenerator<ui::menu::gh::Menu>, .flag = MiscMenuFlag_Shortcut },
    { .name = "FTP", .title = "FTP Install", .func = MiscMenuFuncGenerator<ui::menu::ftp::Menu>, .flag = MiscMenuFlag_Install },
    { .name = "USB", .title = "USB Install", .func = MiscMenuFuncGenerator<ui::menu::usb::Menu>, .flag = MiscMenuFlag_Install },
    { .name = "GameCard", .title = "GameCard", .func = MiscMenuFuncGenerator<ui::menu::gc::Menu>, .flag = MiscMenuFlag_Shortcut },
    { .name = "IRS", .title = "IRS (Infrared Joycon Camera)", .func = MiscMenuFuncGenerator<ui::menu::irs::Menu>, .flag = MiscMenuFlag_Shortcut },
};

auto InstallUpdate(ProgressBox* pbox, const std::string url, const std::string version) -> Result {
    static fs::FsPath zip_out{"/switch/sphaira/cache/update.zip"};
    constexpr auto chunk_size = 1024 * 512; // 512KiB

    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    // 1. download the zip
    if (!pbox->ShouldExit()) {
        pbox->NewTransfer("Downloading "_i18n + version);
        log_write("starting download: %s\n", url.c_str());

        const auto result = curl::Api().ToFile(
            curl::Url{url},
            curl::Path{zip_out},
            curl::OnProgress{pbox->OnDownloadProgressCallback()}
        );

        R_UNLESS(result.success, 0x1);
    }

    ON_SCOPE_EXIT(fs.DeleteFile(zip_out));

    // 2. extract the zip
    if (!pbox->ShouldExit()) {
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
            fs::FsPath file_path;
            if (UNZ_OK != unzGetCurrentFileInfo64(zfile, &info, file_path, sizeof(file_path), 0, 0, 0, 0)) {
                log_write("failed to get current info\n");
                R_THROW(0x1);
            }

            if (file_path[0] != '/') {
                file_path = fs::AppendPath("/", file_path);
            }

            if (std::strstr(file_path, "sphaira.nro")) {
                file_path = App::GetExePath();
            }

            Result rc;
            if (file_path[std::strlen(file_path) -1] == '/') {
                if (R_FAILED(rc = fs.CreateDirectoryRecursively(file_path)) && rc != FsError_PathAlreadyExists) {
                    log_write("failed to create folder: %s 0x%04X\n", file_path.s, rc);
                    R_THROW(rc);
                }
            } else {
                Result rc;
                if (R_FAILED(rc = fs.CreateFile(file_path, info.uncompressed_size, 0)) && rc != FsError_PathAlreadyExists) {
                    log_write("failed to create file: %s 0x%04X\n", file_path.s, rc);
                    R_THROW(rc);
                }

                fs::File f;
                R_TRY(fs.OpenFile(file_path, FsOpenMode_Write, &f));
                R_TRY(f.SetSize(info.uncompressed_size));

                std::vector<char> buf(chunk_size);
                s64 offset{};
                while (offset < info.uncompressed_size) {
                    const auto bytes_read = unzReadCurrentFile(zfile, buf.data(), buf.size());
                    if (bytes_read <= 0) {
                        // log_write("failed to read zip file: %s\n", inzip.c_str());
                        R_THROW(0x1);
                    }

                    R_TRY(f.Write(offset, buf.data(), bytes_read, FsWriteOption_None));

                    pbox->UpdateTransfer(offset, info.uncompressed_size);
                    offset += bytes_read;
                }
            }

            // check if we have sphaira installed in other locations and update them.
            if (file_path == App::GetExePath()) {
                for (auto& path : SPHAIRA_PATHS) {
                    log_write("[UPD] checking path: %s\n", path.s);
                    // skip if we already updated this path.
                    if (file_path == path) {
                        log_write("[UPD] skipped as already updated\n");
                        continue;
                    }

                    // check that this is really sphaira.
                    log_write("[UPD] checking nacp\n");
                    NacpStruct nacp;
                    if (R_SUCCEEDED(nro_get_nacp(path, nacp)) && !std::strcmp(nacp.lang[0].name, "sphaira")) {
                        log_write("[UPD] found, updating\n");
                        pbox->NewTransfer(path);
                        R_TRY(pbox->CopyFile(&fs, file_path, path));
                    }
                }
            }
        }
    }

    log_write("finished update :)\n");
    R_SUCCEED();
}

auto CreateLeftSideMenu(std::string& name_out) -> std::shared_ptr<MenuBase> {
    const auto name = App::GetApp()->m_left_menu.Get();

    for (auto& e : GetMiscMenuEntries()) {
        if (e.name == name) {
            name_out = name;
            return e.func(MenuFlag_Tab);
        }
    }

    name_out = "FileBrowser";
    return std::make_shared<ui::menu::filebrowser::Menu>(MenuFlag_Tab);
}

auto CreateRightSideMenu(std::string_view left_name) -> std::shared_ptr<MenuBase> {
    const auto name = App::GetApp()->m_right_menu.Get();

    // handle if the user tries to mount the same menu twice.
    if (name == left_name) {
        // check if we can mount the default.
        if (left_name != "AppStore") {
            return std::make_shared<ui::menu::appstore::Menu>(MenuFlag_Tab);
        } else {
            // otherwise, fallback to left side default.
            return std::make_shared<ui::menu::filebrowser::Menu>(MenuFlag_Tab);
        }
    }

    for (auto& e : GetMiscMenuEntries()) {
        if (e.name == name) {
            return e.func(MenuFlag_Tab);
        }
    }

    return std::make_shared<ui::menu::appstore::Menu>(MenuFlag_Tab);
}

} // namespace

auto GetMiscMenuEntries() -> std::span<const MiscMenuEntry> {
    return MISC_MENU_ENTRIES;
}

MainMenu::MainMenu() {
    curl::Api().ToFileAsync(
        curl::Url{GITHUB_URL},
        curl::Path{CACHE_PATH},
        curl::Flags{curl::Flag_Cache},
        curl::StopToken{this->GetToken()},
        curl::Header{
            { "Accept", "application/vnd.github+json" },
        },
        curl::OnComplete{[this](auto& result){
            log_write("inside github download\n");
            m_update_state = UpdateState::Error;
            ON_SCOPE_EXIT( log_write("update status: %u\n", (u8)m_update_state) );

            if (!result.success) {
                return false;
            }

            auto json = yyjson_read_file(CACHE_PATH, YYJSON_READ_NOFLAG, nullptr, nullptr);
            R_UNLESS(json, false);
            ON_SCOPE_EXIT(yyjson_doc_free(json));

            auto root = yyjson_doc_get_root(json);
            R_UNLESS(root, false);

            auto tag_key = yyjson_obj_get(root, "tag_name");
            R_UNLESS(tag_key, false);

            const auto version = yyjson_get_str(tag_key);
            R_UNLESS(version, false);
            if (!App::IsVersionNewer(APP_VERSION, version)) {
                m_update_state = UpdateState::None;
                return true;
            }

            auto body_key = yyjson_obj_get(root, "body");
            R_UNLESS(body_key, false);

            const auto body = yyjson_get_str(body_key);
            R_UNLESS(body, false);

            auto assets = yyjson_obj_get(root, "assets");
            R_UNLESS(assets, false);

            auto idx0 = yyjson_arr_get(assets, 0);
            R_UNLESS(idx0, false);

            auto url_key = yyjson_obj_get(idx0, "browser_download_url");
            R_UNLESS(url_key, false);

            const auto url = yyjson_get_str(url_key);
            R_UNLESS(url, false);

            m_update_version = version;
            m_update_url = url;
            m_update_description = body;
            m_update_state = UpdateState::Update;
            log_write("found url: %s\n", url);
            log_write("found body: %s\n", body);
            App::Notify("Update avaliable: "_i18n + m_update_version);
            App::Notify("Download via the Network options!"_i18n);

            return true;
        }
    });

    this->SetActions(
        std::make_pair(Button::START, Action{App::Exit}),
        std::make_pair(Button::SELECT, Action{App::DisplayMiscOptions}),
        std::make_pair(Button::Y, Action{"Menu"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("Menu Options"_i18n, "v" APP_VERSION_HASH, Sidebar::Side::LEFT);
            ON_SCOPE_EXIT(App::Push(options));

            SidebarEntryArray::Items language_items;
            language_items.push_back("Auto"_i18n);
            language_items.push_back("English"_i18n);
            language_items.push_back("Japanese"_i18n);
            language_items.push_back("French"_i18n);
            language_items.push_back("German"_i18n);
            language_items.push_back("Italian"_i18n);
            language_items.push_back("Spanish"_i18n);
            language_items.push_back("Chinese"_i18n);
            language_items.push_back("Korean"_i18n);
            language_items.push_back("Dutch"_i18n);
            language_items.push_back("Portuguese"_i18n);
            language_items.push_back("Russian"_i18n);
            language_items.push_back("Swedish"_i18n);
            language_items.push_back("Vietnamese"_i18n);
            language_items.push_back("Ukrainian"_i18n);

            options->Add(std::make_shared<SidebarEntryCallback>("Theme"_i18n, [](){
                App::DisplayThemeOptions();
            }));

            options->Add(std::make_shared<SidebarEntryCallback>("Network"_i18n, [this](){
                auto options = std::make_shared<Sidebar>("Network Options"_i18n, Sidebar::Side::LEFT);
                ON_SCOPE_EXIT(App::Push(options));

                if (m_update_state == UpdateState::Update) {
                    options->Add(std::make_shared<SidebarEntryCallback>("Download update: "_i18n + m_update_version, [this](){
                        App::Push(std::make_shared<ProgressBox>(0, "Downloading "_i18n, "Sphaira v" + m_update_version, [this](auto pbox) -> Result {
                            return InstallUpdate(pbox, m_update_url, m_update_version);
                        }, [this](Result rc){
                            App::PushErrorBox(rc, "Failed to download update"_i18n);

                            if (R_SUCCEEDED(rc)) {
                                m_update_state = UpdateState::None;
                                App::Notify("Updated to "_i18n + m_update_version);
                                App::Push(std::make_shared<OptionBox>(
                                    "Press OK to restart Sphaira"_i18n, "OK"_i18n, [](auto){
                                        App::ExitRestart();
                                    }
                                ));
                            }
                        }));
                    }));
                }

                options->Add(std::make_shared<SidebarEntryBool>("Ftp"_i18n, App::GetFtpEnable(), [](bool& enable){
                    App::SetFtpEnable(enable);
                }));

                options->Add(std::make_shared<SidebarEntryBool>("Mtp"_i18n, App::GetMtpEnable(), [](bool& enable){
                    App::SetMtpEnable(enable);
                }));

                options->Add(std::make_shared<SidebarEntryBool>("Nxlink"_i18n, App::GetNxlinkEnable(), [](bool& enable){
                    App::SetNxlinkEnable(enable);
                }));

                options->Add(std::make_shared<SidebarEntryBool>("Hdd"_i18n, App::GetHddEnable(), [](bool& enable){
                    App::SetHddEnable(enable);
                }));

                options->Add(std::make_shared<SidebarEntryBool>("Hdd write protect"_i18n, App::GetWriteProtect(), [](bool& enable){
                    App::SetWriteProtect(enable);
                }));
            }));

            options->Add(std::make_shared<SidebarEntryArray>("Language"_i18n, language_items, [](s64& index_out){
                App::SetLanguage(index_out);
            }, (s64)App::GetLanguage()));

            options->Add(std::make_shared<SidebarEntryCallback>("Misc"_i18n, [](){
                App::DisplayMiscOptions();
            }));

            options->Add(std::make_shared<SidebarEntryCallback>("Advanced"_i18n, [](){
                App::DisplayAdvancedOptions();
            }));
        }})
    );

    m_centre_menu = std::make_shared<homebrew::Menu>();
    m_current_menu = m_centre_menu;


    std::string left_side_name;
    m_left_menu = CreateLeftSideMenu(left_side_name);
    m_right_menu = CreateRightSideMenu(left_side_name);

    AddOnLRPress();

    for (auto [button, action] : m_actions) {
        m_current_menu->SetAction(button, action);
    }
}

MainMenu::~MainMenu() {

}

void MainMenu::Update(Controller* controller, TouchInfo* touch) {
    m_current_menu->Update(controller, touch);
}

void MainMenu::Draw(NVGcontext* vg, Theme* theme) {
    m_current_menu->Draw(vg, theme);
}

void MainMenu::OnFocusGained() {
    Widget::OnFocusGained();
    m_current_menu->OnFocusGained();
}

void MainMenu::OnFocusLost() {
    Widget::OnFocusLost();
    m_current_menu->OnFocusLost();
}

void MainMenu::OnLRPress(std::shared_ptr<MenuBase> menu, Button b) {
    m_current_menu->OnFocusLost();
    if (m_current_menu == m_centre_menu) {
        m_current_menu = menu;
        RemoveAction(b);
    } else {
        m_current_menu = m_centre_menu;
    }

    AddOnLRPress();
    m_current_menu->OnFocusGained();

    for (auto [button, action] : m_actions) {
        m_current_menu->SetAction(button, action);
    }
}

void MainMenu::AddOnLRPress() {
    if (m_current_menu != m_left_menu) {
        const auto label = m_current_menu == m_centre_menu ? m_left_menu->GetShortTitle() : m_centre_menu->GetShortTitle();
        SetAction(Button::L, Action{i18n::get(label), [this]{
            OnLRPress(m_left_menu, Button::L);
        }});
    }

    if (m_current_menu != m_right_menu) {
        const auto label = m_current_menu == m_centre_menu ? m_right_menu->GetShortTitle() : m_centre_menu->GetShortTitle();
        SetAction(Button::R, Action{i18n::get(label), [this]{
            OnLRPress(m_right_menu, Button::R);
        }});
    }
}

} // namespace sphaira::ui::menu::main
