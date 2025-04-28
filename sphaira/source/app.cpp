#include "ui/option_box.hpp"
#include "ui/bubbles.hpp"
#include "ui/sidebar.hpp"
#include "ui/popup_list.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/error_box.hpp"

#include "ui/menus/main_menu.hpp"
#include "ui/menus/irs_menu.hpp"
#include "ui/menus/themezer.hpp"
#include "ui/menus/ghdl.hpp"
#include "ui/menus/usb_menu.hpp"
#include "ui/menus/ftp_menu.hpp"
#include "ui/menus/gc_menu.hpp"

#include "app.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "nro.hpp"
#include "evman.hpp"
#include "owo.hpp"
#include "image.hpp"
#include "nxlink.h"
#include "fs.hpp"
#include "defines.hpp"
#include "i18n.hpp"
#include "ftpsrv_helper.hpp"
#include "web.hpp"

#include <nanovg_dk.h>
#include <minIni.h>
#include <pulsar.h>
#include <haze.h>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <ctime>
#include <span>
#include <dirent.h>

extern "C" {
    u32 __nx_applet_exit_mode = 0;
} // extern "C"

namespace sphaira {
namespace {

struct ThemeData {
    fs::FsPath music_path{"/config/sphaira/themes/default_music.bfstm"};
    std::string elements[ThemeEntryID_MAX]{};
};

struct ThemeIdPair {
    const char* label;
    ThemeEntryID id;
    ElementType type{ElementType::None};
};

struct FrameBufferSize {
    Vec2 size;
    Vec2 scale;
};

constexpr ThemeIdPair THEME_ENTRIES[] = {
    { "background", ThemeEntryID_BACKGROUND },
    { "grid", ThemeEntryID_GRID },
    { "text", ThemeEntryID_TEXT, ElementType::Colour },
    { "text_info", ThemeEntryID_TEXT_INFO, ElementType::Colour },
    { "text_selected", ThemeEntryID_TEXT_SELECTED, ElementType::Colour },
    { "selected_background", ThemeEntryID_SELECTED_BACKGROUND, ElementType::Colour },
    { "error", ThemeEntryID_ERROR, ElementType::Colour },
    { "popup", ThemeEntryID_POPUP, ElementType::Colour },
    { "line", ThemeEntryID_LINE, ElementType::Colour },
    { "line_separator", ThemeEntryID_LINE_SEPARATOR, ElementType::Colour },
    { "sidebar", ThemeEntryID_SIDEBAR, ElementType::Colour },
    { "scrollbar", ThemeEntryID_SCROLLBAR, ElementType::Colour },
    { "scrollbar_background", ThemeEntryID_SCROLLBAR_BACKGROUND, ElementType::Colour },
    { "progressbar", ThemeEntryID_PROGRESSBAR, ElementType::Colour },
    { "progressbar_background", ThemeEntryID_PROGRESSBAR_BACKGROUND, ElementType::Colour },
    { "highlight_1", ThemeEntryID_HIGHLIGHT_1, ElementType::Colour },
    { "highlight_2", ThemeEntryID_HIGHLIGHT_2, ElementType::Colour },
    { "icon_colour", ThemeEntryID_ICON_COLOUR, ElementType::Colour },
    { "icon_audio", ThemeEntryID_ICON_AUDIO, ElementType::Texture },
    { "icon_video", ThemeEntryID_ICON_VIDEO, ElementType::Texture },
    { "icon_image", ThemeEntryID_ICON_IMAGE, ElementType::Texture },
    { "icon_file", ThemeEntryID_ICON_FILE, ElementType::Texture },
    { "icon_folder", ThemeEntryID_ICON_FOLDER, ElementType::Texture },
    { "icon_zip", ThemeEntryID_ICON_ZIP, ElementType::Texture },
    { "icon_nro", ThemeEntryID_ICON_NRO, ElementType::Texture },
};

constinit App* g_app{};

void deko3d_error_cb(void* userData, const char* context, DkResult result, const char* message) {
    switch (result) {
        case DkResult_Success:
            break;

        case DkResult_Fail:
            log_write("[DkResult_Fail] %s\n", message);
            App::Notify("DkResult_Fail");
            break;

        case DkResult_Timeout:
            log_write("[DkResult_Timeout] %s\n", message);
            App::Notify("DkResult_Timeout");
            break;

        case DkResult_OutOfMemory:
            log_write("[DkResult_OutOfMemory] %s\n", message);
            App::Notify("DkResult_OutOfMemory");
            break;

        case DkResult_NotImplemented:
            log_write("[DkResult_NotImplemented] %s\n", message);
            App::Notify("DkResult_NotImplemented");
            break;

        case DkResult_MisalignedSize:
            log_write("[DkResult_MisalignedSize] %s\n", message);
            App::Notify("DkResult_MisalignedSize");
            break;

        case DkResult_MisalignedData:
            log_write("[DkResult_MisalignedData] %s\n", message);
            App::Notify("DkResult_MisalignedData");
            break;

        case DkResult_BadInput:
            log_write("[DkResult_BadInput] %s\n", message);
            App::Notify("DkResult_BadInput");
            break;

        case DkResult_BadFlags:
            log_write("[DkResult_BadFlags] %s\n", message);
            App::Notify("DkResult_BadFlags");
            break;

        case DkResult_BadState:
            log_write("[DkResult_BadState] %s\n", message);
            App::Notify("DkResult_BadState");
            break;
    }
}

void on_applet_focus_state(App* app) {
    switch (appletGetFocusState()) {
        case AppletFocusState_InFocus:
            log_write("[APPLET] AppletFocusState_InFocus\n");
            // App::Notify("AppletFocusState_InFocus");
            break;

        case AppletFocusState_OutOfFocus:
            log_write("[APPLET] AppletFocusState_OutOfFocus\n");
            // App::Notify("AppletFocusState_OutOfFocus");
            break;

        case AppletFocusState_Background:
            log_write("[APPLET] AppletFocusState_Background\n");
            // App::Notify("AppletFocusState_Background");
            break;
    }
}

void on_applet_operation_mode(App* app) {
    switch (appletGetOperationMode()) {
        case AppletOperationMode_Handheld:
            log_write("[APPLET] AppletOperationMode_Handheld\n");
            App::Notify("Switch-Handheld!"_i18n);
            break;

        case AppletOperationMode_Console:
            log_write("[APPLET] AppletOperationMode_Console\n");
            App::Notify("Switch-Docked!"_i18n);
            break;
    }
}

void applet_on_performance_mode(App* app) {
    switch (appletGetPerformanceMode()) {
        case ApmPerformanceMode_Invalid:
            log_write("[APPLET] ApmPerformanceMode_Invalid\n");
            App::Notify("ApmPerformanceMode_Invalid");
            break;

        case ApmPerformanceMode_Normal:
            log_write("[APPLET] ApmPerformanceMode_Normal\n");
            App::Notify("ApmPerformanceMode_Normal");
            break;

        case ApmPerformanceMode_Boost:
            log_write("[APPLET] ApmPerformanceMode_Boost\n");
            App::Notify("ApmPerformanceMode_Boost");
            break;
    }
}

void appplet_hook_calback(AppletHookType type, void *param) {
    auto app = static_cast<App*>(param);
    switch (type) {
        case AppletHookType_OnFocusState:
            // App::Notify("AppletHookType_OnFocusState");
            on_applet_focus_state(app);
            break;

        case AppletHookType_OnOperationMode:
            // App::Notify("AppletHookType_OnOperationMode");
            on_applet_operation_mode(app);
            break;

        case AppletHookType_OnPerformanceMode:
            // App::Notify("AppletHookType_OnPerformanceMode");
            applet_on_performance_mode(app);
            break;

        case AppletHookType_OnExitRequest:
            // App::Notify("AppletHookType_OnExitRequest");
            break;

        case AppletHookType_OnResume:
            // App::Notify("AppletHookType_OnResume");
            break;

        case AppletHookType_OnCaptureButtonShortPressed:
            // App::Notify("AppletHookType_OnCaptureButtonShortPressed");
            break;

        case AppletHookType_OnAlbumScreenShotTaken:
            // App::Notify("AppletHookType_OnAlbumScreenShotTaken");
            break;

        case AppletHookType_RequestToDisplay:
            // App::Notify("AppletHookType_RequestToDisplay");
            break;

        case AppletHookType_Max:
            assert(!"AppletHookType_Max hit");
            break;
    }
}

auto GetFrameBufferSize() -> FrameBufferSize {
    FrameBufferSize fb{};

    switch (appletGetOperationMode()) {
        case AppletOperationMode_Handheld:
            fb.size.x = 1280;
            fb.size.y = 720;
            break;

        case AppletOperationMode_Console:
            fb.size.x = 1920;
            fb.size.y = 1080;
            break;
    }

    fb.scale.x = fb.size.x / SCREEN_WIDTH;
    fb.scale.y = fb.size.y / SCREEN_HEIGHT;
    return fb;
}

// this will try to decompress the icon and then re-convert it to jpg
// in order to strip exif data.
// this doesn't take long at all, but it's very overkill.
// todo: look into jpeg/exif spec to manually strip data
auto GetNroIcon(const std::vector<u8>& nro_icon) -> std::vector<u8> {
    auto image = ImageLoadFromMemory(nro_icon);
    if (!image.data.empty()) {
        if (image.w != 256 || image.h != 256) {
            image = ImageResize(image.data, image.w, image.h, 256, 256);
        }
        if (!image.data.empty()) {
            image = ImageConvertToJpg(image.data, image.w, image.h);
            if (!image.data.empty()) {
                return image.data;
            }
        }
    }
    return nro_icon;
}

auto LoadThemeMeta(const fs::FsPath& path, ThemeMeta& meta) -> bool {
    meta = {};

    char buf[FS_MAX_PATH]{};
    int len{};
    len = ini_gets("meta", "name", "", buf, sizeof(buf) - 1, path);
    if (len <= 1) {
        return false;
    }
    meta.name = buf;

    len = ini_gets("meta", "author", "", buf, sizeof(buf) - 1, path);
    if (len <= 1) {
        return false;
    }
    meta.author = buf;

    len = ini_gets("meta", "version", "", buf, sizeof(buf) - 1, path);
    if (len <= 1) {
        return false;
    }
    meta.version = buf;

    len = ini_gets("meta", "inherit", "", buf, sizeof(buf) - 1, path);
    if (len > 1) {
        meta.inherit = buf;
    }

    log_write("loaded meta from: %s\n", path.s);
    meta.ini_path = path;
    return true;
}

void LoadThemeInternal(ThemeMeta meta, ThemeData& theme_data, int inherit_level = 0) {
    constexpr auto inherit_level_max = 5;

    // all themes will inherit from black theme by default.
    if (meta.inherit.empty() && !inherit_level) {
        meta.inherit = "romfs:/themes/base_black_theme.ini";
    }

    // check if the theme inherits from another, if so, load it.
    // block inheriting from itself.
    if (inherit_level < inherit_level_max && !meta.inherit.empty() && strcasecmp(meta.inherit, "none") && meta.inherit != meta.ini_path) {
        log_write("inherit is not empty: %s\n", meta.inherit.s);
        if (R_SUCCEEDED(romfsInit())) {
            ThemeMeta inherit_meta;
            const auto has_meta = LoadThemeMeta(meta.inherit, inherit_meta);
            romfsExit();

            // base themes do not have a meta
            if (!has_meta) {
                inherit_meta.ini_path = meta.inherit;
            }

            LoadThemeInternal(inherit_meta, theme_data, inherit_level + 1);
        }
    }

    static constexpr auto cb = [](const mTCHAR *Section, const mTCHAR *Key, const mTCHAR *Value, void *UserData) -> int {
        auto theme_data = static_cast<ThemeData*>(UserData);

        if (!std::strcmp(Section, "theme")) {
            if (!std::strcmp(Key, "music")) {
                theme_data->music_path = Value;
            } else {
                for (auto& e : THEME_ENTRIES) {
                    if (!std::strcmp(Key, e.label)) {
                        theme_data->elements[e.id] = Value;
                        break;
                    }
                }
            }
        }

        return 1;
    };

    if (R_SUCCEEDED(romfsInit())) {
        ON_SCOPE_EXIT(romfsExit());

        if (!ini_browse(cb, &theme_data, meta.ini_path)) {
            log_write("failed to open ini: %s\n", meta.ini_path.s);
        } else {
            log_write("opened ini: %s\n", meta.ini_path.s);
        }
    }
}

void haze_callback(const HazeCallbackData *data) {
    App::NotifyFlashLed();
    evman::push(*data, false);
}

void nxlink_callback(const NxlinkCallbackData *data) {
    App::NotifyFlashLed();
    evman::push(*data, false);
}

void on_i18n_change() {
    i18n::exit();
    i18n::init(App::GetLanguage());
}

} // namespace

void App::Loop() {
    while (!m_quit && appletMainLoop()) {
        if (m_widgets.empty()) {
            m_quit = true;
            break;
        }

        ui::gfx::updateHighlightAnimation();

        // fire all events in in a 3ms timeslice
        TimeStamp ts_event;
        const u64 event_timeout = 3;

        // limit events to a max per frame in order to not block for too long.
        while (true) {
            if (ts_event.GetMs() >= event_timeout) {
                log_write("event loop timed-out\n");
                break;
            }

            auto event = evman::pop();
            if (!event.has_value()) {
                break;
            }

            std::visit([this](auto&& arg){
                using T = std::decay_t<decltype(arg)>;
                if constexpr(std::is_same_v<T, evman::LaunchNroEventData>) {
                    log_write("[LaunchNroEventData] got event\n");
                    u64 timestamp = 0;
                    timeGetCurrentTime(TimeType_LocalSystemClock, &timestamp);
                    const auto nro_path = nro_normalise_path(arg.path);

                    ini_puts("paths", "last_launch_full", arg.argv.c_str(), App::CONFIG_PATH);
                    ini_puts("paths", "last_launch_path", nro_path.c_str(), App::CONFIG_PATH);

                    // update timestamp
                    ini_putl(nro_path.c_str(), "timestamp", timestamp, App::PLAYLOG_PATH);
                    // update launch_count
                    const long old_launch_count = ini_getl(nro_path.c_str(), "launch_count", 0, App::PLAYLOG_PATH);
                    ini_putl(nro_path.c_str(), "launch_count", old_launch_count + 1, App::PLAYLOG_PATH);
                    log_write("updating timestamp and launch count for: %s %lu %ld\n", nro_path.c_str(), timestamp, old_launch_count + 1);

                    // force disable pop-back to main menu.
                    __nx_applet_exit_mode = 0;
                    m_quit = true;
                } else if constexpr(std::is_same_v<T, evman::ExitEventData>) {
                    log_write("[ExitEventData] got event\n");
                    m_quit = true;
                } else if constexpr(std::is_same_v<T, HazeCallbackData>) {
                    // log_write("[ExitEventData] got event\n");
                    // m_quit = true;
                } else if constexpr(std::is_same_v<T, NxlinkCallbackData>) {
                    switch (arg.type) {
                        case NxlinkCallbackType_Connected:
                            log_write("[NxlinkCallbackType_Connected]\n");
                            App::Notify("Nxlink Connected"_i18n);
                            break;
                        case NxlinkCallbackType_WriteBegin:
                            log_write("[NxlinkCallbackType_WriteBegin] %s\n", arg.file.filename);
                            App::Notify("Nxlink Upload"_i18n);
                            break;
                        case NxlinkCallbackType_WriteProgress:
                            // log_write("[NxlinkCallbackType_WriteProgress]\n");
                            break;
                        case NxlinkCallbackType_WriteEnd:
                            log_write("[NxlinkCallbackType_WriteEnd] %s\n", arg.file.filename);
                            App::Notify("Nxlink Finished"_i18n);
                            break;
                    }
                } else if constexpr(std::is_same_v<T, curl::DownloadEventData>) {
                    log_write("[DownloadEventData] got event\n");
                    if (arg.callback && !arg.stoken.stop_requested()) {
                        arg.callback(arg.result);
                    }
                } else {
                    static_assert(false, "non-exhaustive visitor!");
                }
            }, event.value());
        }

        const auto fb = GetFrameBufferSize();
        if (fb.size.x != s_width || fb.size.y != s_height) {
            s_width = fb.size.x;
            s_height = fb.size.y;
            m_scale = fb.scale;
            this->destroyFramebufferResources();
            this->createFramebufferResources();
            renderer->UpdateViewSize(s_width, s_height);
        }

        this->Poll();
        this->Update();
        this->Draw();
    }
}

auto App::Push(std::shared_ptr<ui::Widget> widget) -> void {
    log_write("[Mui] pushing widget\n");

    if (!g_app->m_widgets.empty()) {
        g_app->m_widgets.back()->OnFocusLost();
    }

    log_write("doing focus gained\n");
    g_app->m_widgets.emplace_back(widget)->OnFocusGained();
    log_write("did it\n");
}

auto App::PopToMenu() -> void {
    for (auto it = g_app->m_widgets.rbegin(); it != g_app->m_widgets.rend(); it++) {
        const auto& p = *it;
        if (p->IsMenu()) {
            break;
        }
        p->SetPop();
    }
}

void App::Notify(std::string text, ui::NotifEntry::Side side) {
    g_app->m_notif_manager.Push({text, side});
}

void App::Notify(ui::NotifEntry entry) {
    g_app->m_notif_manager.Push(entry);
}

void App::NotifyPop(ui::NotifEntry::Side side) {
    g_app->m_notif_manager.Pop(side);
}

void App::NotifyClear(ui::NotifEntry::Side side) {
    g_app->m_notif_manager.Clear(side);
}

void App::NotifyFlashLed() {
    static const HidsysNotificationLedPattern pattern = {
        .baseMiniCycleDuration = 0x1,             // 12.5ms.
        .totalMiniCycles = 0x1,                   // 1 mini cycle(s).
        .totalFullCycles = 0x1,                   // 1 full run(s).
        .startIntensity = 0xF,                    // 100%.
        .miniCycles = {{
            .ledIntensity = 0xF,                  // 100%.
            .transitionSteps = 0xF,               // 1 step(s). Total 12.5ms.
            .finalStepDuration = 0xF,             // Forced 12.5ms.
        }}
    };

    s32 total;
    HidsysUniquePadId unique_pad_ids[16] = {0};
    if (R_SUCCEEDED(hidsysGetUniquePadIds(unique_pad_ids, 16, &total))) {
        for (int i = 0; i < total; i++) {
            hidsysSetNotificationLedPattern(&pattern, unique_pad_ids[i]);
        }
    }
}

auto App::GetThemeMetaList() -> std::span<ThemeMeta> {
    return g_app->m_theme_meta_entries;
}

void App::SetTheme(s64 theme_index) {
    g_app->LoadTheme(g_app->m_theme_meta_entries[theme_index]);
    g_app->m_theme_index = theme_index;
}

auto App::GetThemeIndex() -> s64 {
    return g_app->m_theme_index;
}

auto App::GetDefaultImage() -> int {
    return g_app->m_default_image;
}

auto App::GetExePath() -> fs::FsPath {
    return g_app->m_app_path;
}

auto App::IsHbmenu() -> bool {
    return !strcasecmp(GetExePath().s, "/hbmenu.nro");
}

auto App::GetNxlinkEnable() -> bool {
    return g_app->m_nxlink_enabled.Get();
}

auto App::GetLogEnable() -> bool {
    return g_app->m_log_enabled.Get();
}

auto App::GetReplaceHbmenuEnable() -> bool {
    return g_app->m_replace_hbmenu.Get();
}

auto App::GetInstallEnable() -> bool {
    return g_app->m_install.Get();
}

auto App::GetInstallSdEnable() -> bool {
    return g_app->m_install_sd.Get();
}

auto App::GetInstallPrompt() -> bool {
    return g_app->m_install_prompt.Get();
}

auto App::GetThemeMusicEnable() -> bool {
    return g_app->m_theme_music.Get();
}

auto App::GetMtpEnable() -> bool {
    return g_app->m_mtp_enabled.Get();
}

auto App::GetFtpEnable() -> bool {
    return g_app->m_ftp_enabled.Get();
}

auto App::GetLanguage() -> long {
    return g_app->m_language.Get();
}

auto App::GetTextScrollSpeed() -> long {
    return g_app->m_text_scroll_speed.Get();
}

auto App::Get12HourTimeEnable() -> bool {
    return g_app->m_12hour_time.Get();
}

void App::SetNxlinkEnable(bool enable) {
    if (App::GetNxlinkEnable() != enable) {
        g_app->m_nxlink_enabled.Set(enable);
        if (enable) {
            nxlinkInitialize(nxlink_callback);
        } else {
            nxlinkExit();
        }
    }
}

void App::SetLogEnable(bool enable) {
    if (App::GetLogEnable() != enable) {
        g_app->m_log_enabled.Set(enable);
        if (enable) {
            log_file_init();
        } else {
            log_file_exit();
        }
    }
}

void App::SetReplaceHbmenuEnable(bool enable) {
    if (App::GetReplaceHbmenuEnable() != enable) {
        g_app->m_replace_hbmenu.Set(enable);
        if (!enable) {
            // check we have already replaced hbmenu with sphaira
            NacpStruct hbmenu_nacp{};
            if (R_SUCCEEDED(nro_get_nacp("/hbmenu.nro", hbmenu_nacp))) {
                if (std::strcmp(hbmenu_nacp.lang[0].name, "sphaira")) {
                    return;
                }
            }

            // ask user if they want to restore hbmenu
            App::Push(std::make_shared<ui::OptionBox>(
                "Restore hbmenu?"_i18n,
                "Back"_i18n, "Restore"_i18n, 1, [hbmenu_nacp](auto op_index){
                    if (!op_index || *op_index == 0) {
                        return;
                    }

                    NacpStruct actual_hbmenu_nacp;
                    if (R_FAILED(nro_get_nacp("/switch/hbmenu.nro", actual_hbmenu_nacp))) {
                        App::Push(std::make_shared<ui::OptionBox>(
                            "Failed to find /switch/hbmenu.nro\n"
                            "Use the Appstore to re-install hbmenu"_i18n,
                            "OK"_i18n
                        ));
                        return;
                    }

                    // NOTE: do NOT use rename anywhere here as it's possible
                    // to have a race condition with another app that opens hbmenu as a file
                    // in between the delete + rename.
                    // this would require a sys-module to open hbmenu.nro, such as an ftp server.
                    // a copy means that it opens the file handle, if successfull, then
                    // the full read/write will succeed.
                    fs::FsNativeSd fs;
                    NacpStruct sphaira_nacp;
                    fs::FsPath sphaira_path = "/switch/sphaira/sphaira.nro";
                    Result rc;

                    // first, try and backup sphaira, its not super important if this fails.
                    rc = nro_get_nacp(sphaira_path, sphaira_nacp);
                    if (R_FAILED(rc) || std::strcmp(sphaira_nacp.lang[0].name, "sphaira")) {
                        sphaira_path = "/switch/sphaira.nro";
                        rc = nro_get_nacp(sphaira_path, sphaira_nacp);
                    }

                    if (R_SUCCEEDED(rc) && !std::strcmp(sphaira_nacp.lang[0].name, "sphaira")) {
                        if (std::strcmp(sphaira_nacp.display_version, hbmenu_nacp.display_version) < 0) {
                            if (R_FAILED(rc = fs.copy_entire_file(sphaira_path, "/hbmenu.nro"))) {
                                log_write("failed to copy entire file: %s 0x%X module: %u desc: %u\n", sphaira_path.s, rc, R_MODULE(rc), R_DESCRIPTION(rc));
                            } else {
                                log_write("success with updating hbmenu!\n");
                            }
                        }
                    } else {
                        // sphaira doesn't yet exist, create a new file.
                        sphaira_path = "/switch/sphaira/sphaira.nro";
                        fs.CreateDirectoryRecursively("/switch/sphaira/");
                        fs.copy_entire_file(sphaira_path, "/hbmenu.nro");
                    }

                    // this should never fail, if it does, well then the sd card is fucked.
                    if (R_FAILED(rc = fs.copy_entire_file("/hbmenu.nro", "/switch/hbmenu.nro")))  {
                        // try and restore sphaira in a last ditch effort.
                        if (R_FAILED(rc = fs.copy_entire_file("/hbmenu.nro", sphaira_path))) {
                            App::Push(std::make_shared<ui::ErrorBox>(rc,
                                "Failed to restore hbmenu, please re-download hbmenu"_i18n
                            ));
                        } else {
                            App::Push(std::make_shared<ui::OptionBox>(
                                "Failed to restore hbmenu, using sphaira instead"_i18n,
                                "OK"_i18n
                            ));
                        }
                        return;
                    }

                    // don't need this any more.
                    fs.DeleteFile("/switch/hbmenu.nro");

                    // if we were hbmenu, exit now (as romfs is gone).
                    if (IsHbmenu()) {
                        App::Push(std::make_shared<ui::OptionBox>(
                            "Restored hbmenu, closing sphaira"_i18n,
                            "OK"_i18n, [](auto) {
                                App::Exit();
                            }
                        ));
                    } else {
                        App::Notify("Restored hbmenu"_i18n);
                    }
                }
            ));
        }
    }
}

void App::SetInstallEnable(bool enable) {
    g_app->m_install.Set(enable);
}

void App::SetInstallSdEnable(bool enable) {
    g_app->m_install_sd.Set(enable);
}

void App::SetInstallPrompt(bool enable) {
    g_app->m_install_prompt.Set(enable);
}

void App::SetThemeMusicEnable(bool enable) {
    g_app->m_theme_music.Set(enable);
    PlaySoundEffect(SoundEffect::SoundEffect_Music);
}

void App::Set12HourTimeEnable(bool enable) {
    g_app->m_12hour_time.Set(enable);
}

void App::SetMtpEnable(bool enable) {
    if (App::GetMtpEnable() != enable) {
        g_app->m_mtp_enabled.Set(enable);
        if (enable) {
            hazeInitialize(haze_callback);
        } else {
            hazeExit();
        }
    }
}

void App::SetFtpEnable(bool enable) {
    if (App::GetFtpEnable() != enable) {
        g_app->m_ftp_enabled.Set(enable);
        if (enable) {
            ftpsrv::Init();
        } else {
            ftpsrv::Exit();
        }
    }
}

void App::SetLanguage(long index) {
    if (App::GetLanguage() != index) {
        g_app->m_language.Set(index);
        on_i18n_change();

        App::Push(std::make_shared<ui::OptionBox>(
            "Restart Sphaira?"_i18n,
            "Back"_i18n, "Restart"_i18n, 1, [](auto op_index){
                if (op_index && *op_index) {
                    App::ExitRestart();
                }
            }
        ));
    }
}

void App::SetTextScrollSpeed(long index) {
    g_app->m_text_scroll_speed.Set(index);
}

auto App::Install(OwoConfig& config) -> Result {
    R_TRY(romfsInit());
    ON_SCOPE_EXIT(romfsExit());

    std::vector<u8> main_data, npdm_data, logo_data, gif_data;
    R_TRY(fs::read_entire_file("romfs:/exefs/main", main_data));
    R_TRY(fs::read_entire_file("romfs:/exefs/main.npdm", npdm_data));

    config.nro_path = nro_add_arg_file(config.nro_path);
    config.main = main_data;
    config.npdm = npdm_data;
    config.logo = logo_data;
    config.gif = gif_data;
    if (!config.icon.empty()) {
        config.icon = GetNroIcon(config.icon);
    }

    const auto rc = install_forwarder(config, App::GetInstallSdEnable() ? NcmStorageId_SdCard : NcmStorageId_BuiltInUser);

    if (R_FAILED(rc)) {
        App::PlaySoundEffect(SoundEffect_Error);
        App::Push(std::make_shared<ui::ErrorBox>(rc, "Failed to install forwarder"_i18n));
    } else {
        App::PlaySoundEffect(SoundEffect_Install);
        App::Notify("Installed!"_i18n);
    }

    return rc;
}

auto App::Install(ui::ProgressBox* pbox, OwoConfig& config) -> Result {
    R_TRY(romfsInit());
    ON_SCOPE_EXIT(romfsExit());

    std::vector<u8> main_data, npdm_data, logo_data, gif_data;
    R_TRY(fs::read_entire_file("romfs:/exefs/main", main_data));
    R_TRY(fs::read_entire_file("romfs:/exefs/main.npdm", npdm_data));

    config.nro_path = nro_add_arg_file(config.nro_path);
    config.main = main_data;
    config.npdm = npdm_data;
    config.logo = logo_data;
    config.gif = gif_data;
    if (!config.icon.empty()) {
        config.icon = GetNroIcon(config.icon);
    }

    const auto rc = install_forwarder(pbox, config, GetInstallSdEnable() ? NcmStorageId_SdCard : NcmStorageId_BuiltInUser);

    if (R_FAILED(rc)) {
        App::PlaySoundEffect(SoundEffect_Error);
        App::Push(std::make_shared<ui::ErrorBox>(rc, "Failed to install forwarder"_i18n));
    } else {
        App::PlaySoundEffect(SoundEffect_Install);
        App::Notify("Installed!"_i18n);
    }

    return rc;
}

void App::Exit() {
    g_app->m_quit = true;
}

void App::ExitRestart() {
    nro_launch(GetExePath());
    Exit();
}

void App::Poll() {
    m_controller.Reset();

    HidTouchScreenState state{};
    hidGetTouchScreenStates(&state, 1);
    m_touch_info.is_clicked = false;

// todo: replace old touch code with gestures from below
#if 0
    static HidGestureState prev_gestures[17]{};
    HidGestureState gestures[17]{};
    const auto gesture_count = hidGetGestureStates(gestures, std::size(gestures));
    for (int i = (int)gesture_count - 1; i >= 0; i--) {
        bool found = false;
        for (int j = 0; j < gesture_count; j++) {
            if (gestures[i].type == prev_gestures[j].type && gestures[i].sampling_number == prev_gestures[j].sampling_number) {
                found = true;
                break;
            }
        }

        if (found) {
            continue;
        }

        auto gesture = gestures[i];
        if (gesture_count && gesture.type == HidGestureType_Touch) {
            log_write("[TOUCH] got gesture attr: %u direction: %u sampling_number: %zu context_number: %zu\n", gesture.attributes, gesture.direction, gesture.sampling_number, gesture.context_number);
        }
        else if (gesture_count && gesture.type == HidGestureType_Swipe) {
            log_write("[SWIPE] got gesture direction: %u sampling_number: %zu context_number: %zu\n", gesture.direction, gesture.sampling_number, gesture.context_number);
        }
        else if (gesture_count && gesture.type == HidGestureType_Tap) {
            log_write("[TAP] got gesture direction: %u sampling_number: %zu context_number: %zu\n", gesture.direction, gesture.sampling_number, gesture.context_number);
        }
        else if (gesture_count && gesture.type == HidGestureType_Press) {
            log_write("[PRESS] got gesture direction: %u sampling_number: %zu context_number: %zu\n", gesture.direction, gesture.sampling_number, gesture.context_number);
        }
        else if (gesture_count && gesture.type == HidGestureType_Cancel) {
            log_write("[CANCEL] got gesture direction: %u sampling_number: %zu context_number: %zu\n", gesture.direction, gesture.sampling_number, gesture.context_number);
        }
        else if (gesture_count && gesture.type == HidGestureType_Complete) {
            log_write("[COMPLETE] got gesture direction: %u sampling_number: %zu context_number: %zu\n", gesture.direction, gesture.sampling_number, gesture.context_number);
        }
        else if (gesture_count && gesture.type == HidGestureType_Pan) {
            log_write("[PAN] got gesture direction: %u sampling_number: %zu context_number: %zu x: %d y: %d dx: %d dy: %d vx: %.2f vy: %.2f count: %d\n", gesture.direction, gesture.sampling_number, gesture.context_number, gesture.x, gesture.y, gesture.delta_x, gesture.delta_y, gesture.velocity_x, gesture.velocity_y, gesture.point_count);
        }
    }

    memcpy(prev_gestures, gestures, sizeof(gestures));
#endif

    if (state.count == 1 && !m_touch_info.is_touching) {
        m_touch_info.initial = m_touch_info.cur = state.touches[0];
        m_touch_info.is_touching = true;
        m_touch_info.is_tap = true;
    } else if (state.count >= 1 && m_touch_info.is_touching) {
        m_touch_info.cur = state.touches[0];

        if (m_touch_info.is_tap &&
            (std::abs((s32)m_touch_info.initial.x - (s32)m_touch_info.cur.x) > 20 ||
            std::abs((s32)m_touch_info.initial.y - (s32)m_touch_info.cur.y) > 20)) {
            m_touch_info.is_tap = false;
            m_touch_info.is_scroll = true;
        }
    } else if (m_touch_info.is_touching) {
        m_touch_info.is_touching = false;
        m_touch_info.is_scroll = false;
        if (m_touch_info.is_tap) {
            m_touch_info.is_clicked = true;
        } else {
            m_touch_info.is_end = true;
        }
    }

    // todo: better implement this to match hos
    if (!m_touch_info.is_touching && !m_touch_info.is_clicked) {
        padUpdate(&m_pad);
        m_controller.m_kdown = padGetButtonsDown(&m_pad);
        m_controller.m_kheld = padGetButtons(&m_pad);
        m_controller.m_kup = padGetButtonsUp(&m_pad);
        m_controller.UpdateButtonHeld(static_cast<u64>(Button::ANY_DIRECTION));
    }
}

void App::Update() {
    m_widgets.back()->Update(&m_controller, &m_touch_info);

    bool popped_at_least1 = false;
    while (true) {
        if (m_widgets.empty()) {
            log_write("[Mui] no widgets left, so we exit...");
            App::Exit();
            return;
        }

        if (m_widgets.back()->ShouldPop()) {
            log_write("popping widget\n");
            m_widgets.pop_back();
            popped_at_least1 = true;
        } else {
            break;
        }
    }

    if (!m_widgets.empty() && popped_at_least1) {
        m_widgets.back()->OnFocusGained();
    }
}

void App::Draw() {
    const auto slot = this->queue.acquireImage(this->swapchain);
    this->queue.submitCommands(this->framebuffer_cmdlists[slot]);
    this->queue.submitCommands(this->render_cmdlist);
    nvgBeginFrame(this->vg, s_width, s_height, 1.f);
    nvgScale(vg, m_scale.x, m_scale.y);

    // find the last menu in the list, start drawing from there
    auto menu_it = m_widgets.rend();
    for (auto it = m_widgets.rbegin(); it != m_widgets.rend(); it++) {
        const auto& p = *it;
        if (!p->IsHidden() && p->IsMenu()) {
            menu_it = it;
            break;
        }
    }

    // reverse itr so loop backwards to go forwarders.
    if (menu_it != m_widgets.rend()) {
        for (auto it = menu_it; ; it--) {
            const auto& p = *it;

            // draw everything not hidden on top of the menu.
            if (!p->IsHidden()) {
                p->Draw(vg, &m_theme);
            }

            if (it == m_widgets.rbegin()) {
                break;
            }
        }
    }

    m_notif_manager.Draw(vg, &m_theme);
    ui::bubble::Draw(vg, &m_theme);

    nvgResetTransform(vg);
    nvgEndFrame(this->vg);
    this->queue.presentImage(this->swapchain, slot);
}

auto App::GetApp() -> App* {
    return g_app;
}

auto App::GetVg() -> NVGcontext* {
    return g_app->vg;
}

void DrawElement(float x, float y, float w, float h, ThemeEntryID id) {
    DrawElement({x, y, w, h}, id);
}

void DrawElement(const Vec4& v, ThemeEntryID id) {
    const auto& e = g_app->m_theme.elements[id];

    switch (e.type) {
        case ElementType::None: {
        } break;
        case ElementType::Texture: {
            auto paint = nvgImagePattern(g_app->vg, v.x, v.y, v.w, v.h, 0, e.texture, 1.f);
            // override the icon colours if set
            if (id > ThemeEntryID_ICON_COLOUR && id < ThemeEntryID_MAX) {
                if (g_app->m_theme.elements[ThemeEntryID_ICON_COLOUR].type != ElementType::None) {
                    paint.innerColor = g_app->m_theme.GetColour(ThemeEntryID_ICON_COLOUR);
                }
            }
            ui::gfx::drawRect(g_app->vg, v, paint);
        } break;
        case ElementType::Colour: {
            ui::gfx::drawRect(g_app->vg, v, e.colour);
        } break;
    }
}

auto App::LoadElementImage(std::string_view value) -> ElementEntry {
    ElementEntry entry{};

    entry.texture = nvgCreateImage(vg, value.data(), 0);
    if (entry.texture) {
        entry.type = ElementType::Texture;
    }

    return entry;
}

auto App::LoadElementColour(std::string_view value) -> ElementEntry {
    ElementEntry entry{};

    if (value.starts_with("0x")) {
        value = value.substr(2);
    } else {
        return {};
    }

    char* end;
    u32 c = std::strtoul(value.data(), &end, 16);
    if (!c && value.data() == end) {
        return {};
    }

    // force alpha bit if not already set.
    if (value.length() <= 6) {
        c <<= 8;
        c |= 0xFF;
    }

    entry.colour = nvgRGBA((c >> 24) & 0xFF, (c >> 16) & 0xFF, (c >> 8) & 0xFF, c & 0xFF);
    entry.type = ElementType::Colour;
    return entry;
}

auto App::LoadElement(std::string_view value, ElementType type) -> ElementEntry {
    if (value.size() <= 1) {
        return {};
    }

    if (type == ElementType::None || type == ElementType::Colour) {
        // most assets are colours, so prioritise this first
        if (auto e = LoadElementColour(value); e.type != ElementType::None) {
            return e;
        }
    }

    if (type == ElementType::None || type == ElementType::Texture) {
        if (auto e = LoadElementImage(value); e.type != ElementType::None) {
            return e;
        }
    }

    return {};
}

void App::CloseTheme() {
    if (m_sound_ids[SoundEffect_Music]) {
        plsrPlayerFree(m_sound_ids[SoundEffect_Music]);
        m_sound_ids[SoundEffect_Music] = nullptr;
        plsrBFSTMClose(&m_theme.music);
    }

    for (auto& e : m_theme.elements) {
        if (e.type == ElementType::Texture) {
            nvgDeleteImage(vg, e.texture);
        }
    }

    m_theme = {};
}

void App::LoadTheme(const ThemeMeta& meta) {
    // reset theme
    CloseTheme();

    ThemeData theme_data{};
    LoadThemeInternal(meta, theme_data);
    m_theme.meta = meta;

    if (R_SUCCEEDED(romfsInit())) {
        ON_SCOPE_EXIT(romfsExit());

        // load all assets / colours.
        for (auto& e : THEME_ENTRIES) {
            m_theme.elements[e.id] = LoadElement(theme_data.elements[e.id], e.type);
        }

        // load music
        if (!theme_data.music_path.empty()) {
            if (R_SUCCEEDED(plsrBFSTMOpen(theme_data.music_path, &m_theme.music))) {
                if (R_SUCCEEDED(plsrPlayerLoadStream(&m_theme.music, &m_sound_ids[SoundEffect_Music]))) {
                    PlaySoundEffect(SoundEffect_Music);
                }
            }
        }
    }
}

// todo: only use opendir on if romfs, otherwise use native fs
void App::ScanThemes(const std::string& path) {
    auto dir = opendir(path.c_str());
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

        const std::string name = d->d_name;
        if (!name.ends_with(".ini")) {
            continue;
        }

        const auto full_path = path + name;

        ThemeMeta meta{};
        if (LoadThemeMeta(full_path, meta)) {
            m_theme_meta_entries.emplace_back(meta);
        }
    }
}

void App::ScanThemeEntries() {
    // load from romfs first
    if (R_SUCCEEDED(romfsInit())) {
        ScanThemes("romfs:/themes/");
        romfsExit();
    }
    // then load custom entries
    ScanThemes("/config/sphaira/themes/");
}

App::App(const char* argv0) {
    g_app = this;
    m_start_timestamp = armGetSystemTick();
    if (!std::strncmp(argv0, "sdmc:/", 6)) {
        // memmove(path, path + 5, strlen(path)-5);
        std::strncpy(m_app_path, argv0 + 5, std::strlen(argv0)-5);
    } else {
        m_app_path = argv0;
    }

    // set if we are hbmenu
    if (IsHbmenu()) {
        __nx_applet_exit_mode = 1;
    }

    fs::FsNativeSd fs;
    fs.CreateDirectoryRecursively("/config/sphaira/assoc");
    fs.CreateDirectoryRecursively("/config/sphaira/themes");
    fs.CreateDirectoryRecursively("/config/sphaira/github");
    fs.CreateDirectoryRecursively("/config/sphaira/i18n");

    if (App::GetLogEnable()) {
        log_file_init();
        log_write("hello world\n");
    }

    if (App::GetMtpEnable()) {
        hazeInitialize(haze_callback);
    }

    if (App::GetFtpEnable()) {
        ftpsrv::Init();
    }

    if (App::GetNxlinkEnable()) {
        nxlinkInitialize(nxlink_callback);
    }

    curl::Init();

    // get current size of the framebuffer
    const auto fb = GetFrameBufferSize();
    s_width = fb.size.x;
    s_height = fb.size.y;
    m_scale = fb.scale;

    // Create the deko3d device
    this->device = dk::DeviceMaker{}
        .setCbDebug(deko3d_error_cb)
        .create();

    // Create the main queue
    this->queue = dk::QueueMaker{this->device}
        .setFlags(DkQueueFlags_Graphics)
        .create();

    // Create the memory pools
    this->pool_images.emplace(device, DkMemBlockFlags_GpuCached | DkMemBlockFlags_Image, 16*1024*1024);
    this->pool_code.emplace(device, DkMemBlockFlags_CpuUncached | DkMemBlockFlags_GpuCached | DkMemBlockFlags_Code, 128*1024);
    this->pool_data.emplace(device, DkMemBlockFlags_CpuUncached | DkMemBlockFlags_GpuCached, 1*1024*1024);

    // Create the static command buffer and feed it freshly allocated memory
    this->cmdbuf = dk::CmdBufMaker{this->device}.create();
    const CMemPool::Handle cmdmem = this->pool_data->allocate(this->StaticCmdSize);
    this->cmdbuf.addMemory(cmdmem.getMemBlock(), cmdmem.getOffset(), cmdmem.getSize());

    // Create the framebuffer resources
    this->createFramebufferResources();

    this->renderer.emplace(s_width, s_height, this->device, this->queue, *this->pool_images, *this->pool_code, *this->pool_data);
    this->vg = nvgCreateDk(&*this->renderer, NVG_ANTIALIAS | NVG_STENCIL_STROKES);

    i18n::init(GetLanguage());

    // not sure if these are meant to be deleted or not...
    PlFontData font_standard, font_extended, font_lang;
    plGetSharedFontByType(&font_standard, PlSharedFontType_Standard);
    plGetSharedFontByType(&font_extended, PlSharedFontType_NintendoExt);

    auto standard_font = nvgCreateFontMem(this->vg, "Standard", (unsigned char*)font_standard.address, font_standard.size, 0);
    auto extended_font = nvgCreateFontMem(this->vg, "Extended", (unsigned char*)font_extended.address, font_extended.size, 0);
    nvgAddFallbackFontId(this->vg, standard_font, extended_font);

    constexpr PlSharedFontType lang_font[] = {
        PlSharedFontType_ChineseSimplified,
        PlSharedFontType_ExtChineseSimplified,
        PlSharedFontType_ChineseTraditional,
        PlSharedFontType_KO,
    };

    for (auto type : lang_font) {
        if (R_SUCCEEDED(plGetSharedFontByType(&font_lang, type))) {
            char name[32];
            snprintf(name, sizeof(name), "Lang_%u", font_lang.type);
            auto lang_font = nvgCreateFontMem(this->vg, name, (unsigned char*)font_lang.address, font_lang.size, 0);
            nvgAddFallbackFontId(this->vg, standard_font, lang_font);
        } else {
            log_write("failed plGetSharedFontByType(%d)\n", type);
        }
    }

    // disable audio in applet mode with a suspended application due to audren fatal.
    // see: https://github.com/ITotalJustice/sphaira/issues/92
    if (IsAppletWithSuspendedApp()) {
        App::Notify("Audio disabled due to suspended game"_i18n);
    } else {
        plsrPlayerInit();
    }

    if (R_SUCCEEDED(romfsMountDataStorageFromProgram(0x0100000000001000, "qlaunch"))) {
        ON_SCOPE_EXIT(romfsUnmount("qlaunch"));
        PLSR_BFSAR qlaunch_bfsar;
        if (R_SUCCEEDED(plsrBFSAROpen("qlaunch:/sound/qlaunch.bfsar", &qlaunch_bfsar))) {
            ON_SCOPE_EXIT(plsrBFSARClose(&qlaunch_bfsar));

            plsrPlayerLoadSoundByName(&qlaunch_bfsar, "SeGameIconFocus", &m_sound_ids[SoundEffect_Focus]);
            plsrPlayerLoadSoundByName(&qlaunch_bfsar, "SeGameIconScroll", &m_sound_ids[SoundEffect_Scroll]);
            plsrPlayerLoadSoundByName(&qlaunch_bfsar, "SeGameIconLimit", &m_sound_ids[SoundEffect_Limit]);
            plsrPlayerLoadSoundByName(&qlaunch_bfsar, "SeStartupMenu_game", &m_sound_ids[SoundEffect_Startup]);
            plsrPlayerLoadSoundByName(&qlaunch_bfsar, "SeGameIconAdd", &m_sound_ids[SoundEffect_Install]);
            plsrPlayerLoadSoundByName(&qlaunch_bfsar, "SeInsertError", &m_sound_ids[SoundEffect_Error]);

            plsrPlayerSetVolume(m_sound_ids[SoundEffect_Limit], 2.0f);
            plsrPlayerSetVolume(m_sound_ids[SoundEffect_Focus], 0.5f);
            PlaySoundEffect(SoundEffect_Startup);
        }
    } else {
        log_write("failed to mount romfs 0x0100000000001000\n");
    }

    ScanThemeEntries();

    fs::FsPath theme_path{};
    constexpr fs::FsPath default_theme_path{"romfs:/themes/abyss_theme.ini"};
    ini_gets("config", "theme", default_theme_path, theme_path, sizeof(theme_path), CONFIG_PATH);

    // try and load previous theme, default to previous version otherwise.
    ThemeMeta theme_meta;
    if (R_SUCCEEDED(romfsInit())) {
        ON_SCOPE_EXIT(romfsExit());
        if (!LoadThemeMeta(theme_path, theme_meta)) {
            log_write("failed to load meta using default\n");
            theme_path = default_theme_path;
            LoadThemeMeta(theme_path, theme_meta);
        }
    }
    log_write("loading theme from: %s\n", theme_meta.ini_path.s);
    LoadTheme(theme_meta);

    // find theme index using the path of the theme.ini
    for (u64 i = 0; i < m_theme_meta_entries.size(); i++) {
        if (m_theme.meta.ini_path == m_theme_meta_entries[i].ini_path) {
            m_theme_index = i;
            break;
        }
    }

    appletHook(&m_appletHookCookie, appplet_hook_calback, this);

    hidInitializeTouchScreen();
    hidInitializeGesture();
    padConfigureInput(8, HidNpadStyleSet_NpadStandard);
    // padInitializeDefault(&m_pad);
    padInitializeAny(&m_pad);

    m_prev_timestamp = ini_getl("paths", "timestamp", 0, App::CONFIG_PATH);
    const auto last_launch_path_size = ini_gets("paths", "last_launch_path", "", m_prev_last_launch, sizeof(m_prev_last_launch), App::CONFIG_PATH);
    fs::FsPath last_launch_path;
    if (last_launch_path_size) {
        ini_gets("paths", "last_launch_path", "", last_launch_path, sizeof(last_launch_path), App::CONFIG_PATH);
    }
    ini_puts("paths", "last_launch_path", "", App::CONFIG_PATH);

    const auto loader_info_size = envGetLoaderInfoSize();
    if (loader_info_size) {
        if (loader_info_size >= 8 && !std::memcmp(envGetLoaderInfo(), "sphaira", 7)) {
            log_write("launching from sphaira created forwarder\n");
            m_is_launched_via_sphaira_forwader = true;
        } else {
            log_write("launching from unknown forwader: %.*s size: %zu\n", (int)loader_info_size, envGetLoaderInfo(), loader_info_size);
        }
    } else {
        log_write("not launching from forwarder\n");
    }

    ini_putl(GetExePath(), "timestamp", m_start_timestamp, App::PLAYLOG_PATH);
    const long old_launch_count = ini_getl(GetExePath(), "launch_count", 0, App::PLAYLOG_PATH);
    ini_putl(GetExePath(), "launch_count", old_launch_count + 1, App::PLAYLOG_PATH);

    // load default image
    if (R_SUCCEEDED(romfsInit())) {
        ON_SCOPE_EXIT(romfsExit());
        const auto image = ImageLoadFromFile("romfs:/default.png");
        if (!image.data.empty()) {
            m_default_image = nvgCreateImageRGBA(vg, image.w, image.h, 0, image.data.data());
        }
    }

    struct EventDay {
        u8 day;
        u8 month;
    };

    static constexpr EventDay event_days[] = {
        { .day = 1, .month = 1 }, // New years

        { .day = 3, .month = 3 }, // March 3 (switch 1)
        { .day = 10, .month = 5 }, // June 10 (switch 2)
        { .day = 15, .month = 5 }, // June 15

        { .day = 25, .month = 12 }, // Christmas
        { .day = 26, .month = 12 },
        { .day = 27, .month = 12 },
        { .day = 28, .month = 12 },
    };

    const auto time = std::time(nullptr);
    const auto tm = std::localtime(&time);

    for (auto e : event_days) {
        if (e.day == tm->tm_mday && e.month == (tm->tm_mon + 1)) {
            ui::bubble::Init();
            break;
        }
    }

    App::Push(std::make_shared<ui::menu::main::MainMenu>());
    log_write("finished app constructor\n");
}

void App::PlaySoundEffect(SoundEffect effect) {
    // Stop and free the last loaded sound
	const auto id = g_app->m_sound_ids[effect];
    if (plsrPlayerIsPlaying(id)) {
        plsrPlayerStop(id);
        plsrPlayerWaitNextFrame();
    }
    if (effect == SoundEffect_Music && !App::GetThemeMusicEnable()) {
        return;
    }
    plsrPlayerPlay(id);
}

void App::DisplayThemeOptions(bool left_side) {
    ui::SidebarEntryArray::Items theme_items{};
    const auto theme_meta = App::GetThemeMetaList();
    for (auto& p : theme_meta) {
        theme_items.emplace_back(p.name);
    }

    auto options = std::make_shared<ui::Sidebar>("Theme Options"_i18n, left_side ? ui::Sidebar::Side::LEFT : ui::Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(options));

    options->Add(std::make_shared<ui::SidebarEntryArray>("Select Theme"_i18n, theme_items, [theme_items](s64& index_out){
        App::SetTheme(index_out);
    }, App::GetThemeIndex()));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Music"_i18n, App::GetThemeMusicEnable(), [](bool& enable){
        App::SetThemeMusicEnable(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("12 Hour Time"_i18n, App::Get12HourTimeEnable(), [](bool& enable){
        App::Set12HourTimeEnable(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));
}

void App::DisplayNetworkOptions(bool left_side) {

}

void App::DisplayMiscOptions(bool left_side) {
    auto options = std::make_shared<ui::Sidebar>("Misc Options"_i18n, left_side ? ui::Sidebar::Side::LEFT : ui::Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(options));

    options->Add(std::make_shared<ui::SidebarEntryCallback>("Themezer"_i18n, [](){
        App::Push(std::make_shared<ui::menu::themezer::Menu>());
    }));

    options->Add(std::make_shared<ui::SidebarEntryCallback>("GitHub"_i18n, [](){
        App::Push(std::make_shared<ui::menu::gh::Menu>());
    }));

    options->Add(std::make_shared<ui::SidebarEntryCallback>("Irs"_i18n, [](){
        App::Push(std::make_shared<ui::menu::irs::Menu>());
    }));

    if (App::IsApplication()) {
        options->Add(std::make_shared<ui::SidebarEntryCallback>("Web"_i18n, [](){
            WebShow("https://lite.duckduckgo.com/lite");
        }));
    }

    if (App::GetApp()->m_install.Get()) {
        if (App::GetFtpEnable()) {
            options->Add(std::make_shared<ui::SidebarEntryCallback>("Ftp Install"_i18n, [](){
                App::Push(std::make_shared<ui::menu::ftp::Menu>());
            }));
        }

        options->Add(std::make_shared<ui::SidebarEntryCallback>("Usb Install"_i18n, [](){
            App::Push(std::make_shared<ui::menu::usb::Menu>());
        }));

        options->Add(std::make_shared<ui::SidebarEntryCallback>("GameCard Install"_i18n, [](){
            App::Push(std::make_shared<ui::menu::gc::Menu>());
        }));
    }
}

void App::DisplayAdvancedOptions(bool left_side) {
    auto options = std::make_shared<ui::Sidebar>("Advanced Options"_i18n, left_side ? ui::Sidebar::Side::LEFT : ui::Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(options));

    ui::SidebarEntryArray::Items text_scroll_speed_items;
    text_scroll_speed_items.push_back("Slow"_i18n);
    text_scroll_speed_items.push_back("Normal"_i18n);
    text_scroll_speed_items.push_back("Fast"_i18n);

    options->Add(std::make_shared<ui::SidebarEntryBool>("Logging"_i18n, App::GetLogEnable(), [](bool& enable){
        App::SetLogEnable(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Replace hbmenu on exit"_i18n, App::GetReplaceHbmenuEnable(), [](bool& enable){
        App::SetReplaceHbmenuEnable(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryArray>("Text scroll speed"_i18n, text_scroll_speed_items, [](s64& index_out){
        App::SetTextScrollSpeed(index_out);
    }, (s64)App::GetTextScrollSpeed()));

    options->Add(std::make_shared<ui::SidebarEntryCallback>("Install options"_i18n, [left_side](){
        App::DisplayInstallOptions(left_side);
    }));
}

void App::DisplayInstallOptions(bool left_side) {
    auto options = std::make_shared<ui::Sidebar>("Install Options"_i18n, left_side ? ui::Sidebar::Side::LEFT : ui::Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(options));

    ui::SidebarEntryArray::Items install_items;
    install_items.push_back("System memory"_i18n);
    install_items.push_back("microSD card"_i18n);

    options->Add(std::make_shared<ui::SidebarEntryBool>("Enable"_i18n, App::GetInstallEnable(), [](bool& enable){
        App::SetInstallEnable(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Show install warning"_i18n, App::GetInstallPrompt(), [](bool& enable){
        App::SetInstallPrompt(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryArray>("Install location"_i18n, install_items, [](s64& index_out){
        App::SetInstallSdEnable(index_out);
    }, (s64)App::GetInstallSdEnable()));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Allow downgrade"_i18n, App::GetApp()->m_allow_downgrade.Get(), [](bool& enable){
        App::GetApp()->m_allow_downgrade.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip if already installed"_i18n, App::GetApp()->m_skip_if_already_installed.Get(), [](bool& enable){
        App::GetApp()->m_skip_if_already_installed.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Ticket only"_i18n, App::GetApp()->m_ticket_only.Get(), [](bool& enable){
        App::GetApp()->m_ticket_only.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip base"_i18n, App::GetApp()->m_skip_base.Get(), [](bool& enable){
        App::GetApp()->m_skip_base.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip Patch"_i18n, App::GetApp()->m_skip_patch.Get(), [](bool& enable){
        App::GetApp()->m_skip_patch.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip addon"_i18n, App::GetApp()->m_skip_addon.Get(), [](bool& enable){
        App::GetApp()->m_skip_addon.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip data patch"_i18n, App::GetApp()->m_skip_data_patch.Get(), [](bool& enable){
        App::GetApp()->m_skip_data_patch.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip ticket"_i18n, App::GetApp()->m_skip_ticket.Get(), [](bool& enable){
        App::GetApp()->m_skip_ticket.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("skip NCA hash verify"_i18n, App::GetApp()->m_skip_nca_hash_verify.Get(), [](bool& enable){
        App::GetApp()->m_skip_nca_hash_verify.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip RSA header verify"_i18n, App::GetApp()->m_skip_rsa_header_fixed_key_verify.Get(), [](bool& enable){
        App::GetApp()->m_skip_rsa_header_fixed_key_verify.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Skip RSA NPDM verify"_i18n, App::GetApp()->m_skip_rsa_npdm_fixed_key_verify.Get(), [](bool& enable){
        App::GetApp()->m_skip_rsa_npdm_fixed_key_verify.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Ignore distribution bit"_i18n, App::GetApp()->m_ignore_distribution_bit.Get(), [](bool& enable){
        App::GetApp()->m_ignore_distribution_bit.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Convert to standard crypto"_i18n, App::GetApp()->m_convert_to_standard_crypto.Get(), [](bool& enable){
        App::GetApp()->m_convert_to_standard_crypto.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Lower master key"_i18n, App::GetApp()->m_lower_master_key.Get(), [](bool& enable){
        App::GetApp()->m_lower_master_key.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));

    options->Add(std::make_shared<ui::SidebarEntryBool>("Lower system version"_i18n, App::GetApp()->m_lower_system_version.Get(), [](bool& enable){
        App::GetApp()->m_lower_system_version.Set(enable);
    }, "Enabled"_i18n, "Disabled"_i18n));
}

App::~App() {
    log_write("starting to exit\n");

    i18n::exit();
    curl::Exit();

    ui::bubble::Exit();

    // this has to be called before any cleanup to ensure the lifetime of
    // nvg is still active as some widgets may need to free images.
    m_widgets.clear();
    nvgDeleteImage(vg, m_default_image);

    appletUnhook(&m_appletHookCookie);

    ini_puts("config", "theme", m_theme.meta.ini_path, CONFIG_PATH);

    CloseTheme();

    // Free any loaded sound from memory
    for (auto id : m_sound_ids) {
        if (id) {
            plsrPlayerFree(id);
        }
    }

	// De-initialize our player
    plsrPlayerExit();

    this->destroyFramebufferResources();
    nvgDeleteDk(this->vg);
    this->renderer.reset();

    // backup hbmenu if it is not sphaira
    if (App::GetReplaceHbmenuEnable() && !IsHbmenu()) {
        NacpStruct hbmenu_nacp;
        fs::FsNativeSd fs;
        Result rc;

        if (R_SUCCEEDED(rc = nro_get_nacp("/hbmenu.nro", hbmenu_nacp)) && std::strcmp(hbmenu_nacp.lang[0].name, "sphaira")) {
            log_write("backing up hbmenu.nro\n");
            if (R_FAILED(rc = fs.copy_entire_file("/switch/hbmenu.nro", "/hbmenu.nro"))) {
                log_write("failed to backup  hbmenu.nro\n");
            }
        } else {
            log_write("not backing up\n");
        }

        if (R_FAILED(rc = fs.copy_entire_file("/hbmenu.nro", GetExePath()))) {
            log_write("failed to copy entire file: %s 0x%X module: %u desc: %u\n", GetExePath().s, rc, R_MODULE(rc), R_DESCRIPTION(rc));
        } else {
            log_write("success with copying over root file!\n");
        }
    } else if (IsHbmenu()) {
        // check we have a version that's newer than current.
        NacpStruct hbmenu_nacp;
        fs::FsNativeSd fs;
        Result rc;

        // ensure that are still sphaira
        if (R_SUCCEEDED(rc = nro_get_nacp("/hbmenu.nro", hbmenu_nacp)) && !std::strcmp(hbmenu_nacp.lang[0].name, "sphaira")) {
            NacpStruct sphaira_nacp;
            fs::FsPath sphaira_path = "/switch/sphaira/sphaira.nro";

            rc = nro_get_nacp(sphaira_path, sphaira_nacp);
            if (R_FAILED(rc) || std::strcmp(sphaira_nacp.lang[0].name, "sphaira")) {
                sphaira_path = "/switch/sphaira.nro";
                rc = nro_get_nacp(sphaira_path, sphaira_nacp);
            }

            // found sphaira, now lets get compare version
            if (R_SUCCEEDED(rc) && !std::strcmp(sphaira_nacp.lang[0].name, "sphaira")) {
                if (std::strcmp(hbmenu_nacp.display_version, sphaira_nacp.display_version) < 0) {
                    if (R_FAILED(rc = fs.copy_entire_file(GetExePath(), sphaira_path))) {
                        log_write("failed to copy entire file: %s 0x%X module: %u desc: %u\n", sphaira_path.s, rc, R_MODULE(rc), R_DESCRIPTION(rc));
                    } else {
                        log_write("success with updating hbmenu!\n");
                    }
                }
            }
        } else {
            log_write("no longer hbmenu!\n");
        }
    }

    if (App::GetMtpEnable()) {
        log_write("closing mtp\n");
        hazeExit();
    }

    if (App::GetFtpEnable()) {
        log_write("closing ftp\n");
        ftpsrv::Exit();
    }

    if (App::GetNxlinkEnable()) {
        log_write("closing nxlink\n");
        nxlinkExit();
    }

    if (App::GetLogEnable()) {
        log_write("closing log\n");
        log_file_exit();
    }

    u64 timestamp;
    timeGetCurrentTime(TimeType_LocalSystemClock, &timestamp);
    ini_putl("paths", "timestamp", timestamp, App::CONFIG_PATH);
}

void App::createFramebufferResources() {
    this->swapchain = nullptr;

    // Create layout for the depth buffer
    dk::ImageLayout layout_depthbuffer;
    dk::ImageLayoutMaker{device}
        .setFlags(DkImageFlags_UsageRender | DkImageFlags_HwCompression)
        .setFormat(DkImageFormat_S8)
        .setDimensions(s_width, s_height)
        .initialize(layout_depthbuffer);

    // Create the depth buffer
    this->depthBuffer_mem = this->pool_images->allocate(layout_depthbuffer.getSize(), layout_depthbuffer.getAlignment());
    this->depthBuffer.initialize(layout_depthbuffer, this->depthBuffer_mem.getMemBlock(), this->depthBuffer_mem.getOffset());

    // Create layout for the framebuffers
    dk::ImageLayout layout_framebuffer;
    dk::ImageLayoutMaker{device}
        .setFlags(DkImageFlags_UsageRender | DkImageFlags_UsagePresent | DkImageFlags_HwCompression)
        .setFormat(DkImageFormat_RGBA8_Unorm)
        .setDimensions(s_width, s_height)
        .initialize(layout_framebuffer);

    // Create the framebuffers
    std::array<DkImage const*, NumFramebuffers> fb_array;
    const u64 fb_size  = layout_framebuffer.getSize();
    const uint32_t fb_align = layout_framebuffer.getAlignment();
    for (unsigned i = 0; i < fb_array.size(); i++) {
        // Allocate a framebuffer
        this->framebuffers_mem[i] = pool_images->allocate(fb_size, fb_align);
        this->framebuffers[i].initialize(layout_framebuffer, framebuffers_mem[i].getMemBlock(), framebuffers_mem[i].getOffset());

        // Generate a command list that binds it
        dk::ImageView colorTarget{ framebuffers[i] }, depthTarget{ depthBuffer };
        this->cmdbuf.bindRenderTargets(&colorTarget, &depthTarget);
        this->framebuffer_cmdlists[i] = cmdbuf.finishList();

        // Fill in the array for use later by the swapchain creation code
        fb_array[i] = &framebuffers[i];
    }

    // Create the swapchain using the framebuffers
    this->swapchain = dk::SwapchainMaker{device, nwindowGetDefault(), fb_array}.create();

    // Generate the main rendering cmdlist
    this->recordStaticCommands();
}

void App::destroyFramebufferResources() {
    // Return early if we have nothing to destroy
    if (!this->swapchain) {
        return;
    }

    this->queue.waitIdle();
    this->cmdbuf.clear();
    swapchain.destroy();

    // Destroy the framebuffers
    for (unsigned i = 0; i < NumFramebuffers; i++) {
        framebuffers_mem[i].destroy();
    }

    // Destroy the depth buffer
    this->depthBuffer_mem.destroy();
}

void App::recordStaticCommands() {
    // Initialize state structs with deko3d defaults
    dk::RasterizerState rasterizerState;
    dk::ColorState colorState;
    dk::ColorWriteState colorWriteState;
    dk::BlendState blendState;

    // Configure the viewport and scissor
    this->cmdbuf.setViewports(0, { { 0.0f, 0.0f, (float)s_width, (float)s_height, 0.0f, 1.0f } });
    this->cmdbuf.setScissors(0, { { 0, 0, (u32)s_width, (u32)s_height } });

    // Clear the color and depth buffers
    this->cmdbuf.clearColor(0, DkColorMask_RGBA, 0.2f, 0.3f, 0.3f, 1.0f);
    this->cmdbuf.clearDepthStencil(true, 1.0f, 0xFF, 0);

    // Bind required state
    this->cmdbuf.bindRasterizerState(rasterizerState);
    this->cmdbuf.bindColorState(colorState);
    this->cmdbuf.bindColorWriteState(colorWriteState);

    this->render_cmdlist = this->cmdbuf.finishList();
}

} // namespace sphaira
