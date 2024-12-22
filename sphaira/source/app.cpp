#include "ui/menus/main_menu.hpp"
#include "ui/error_box.hpp"

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

#include <nanovg_dk.h>
#include <minIni.h>
#include <pulsar.h>
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
            App::Notify("Switch-Handheld!");
            break;

        case AppletOperationMode_Console:
            log_write("[APPLET] AppletOperationMode_Console\n");
            App::Notify("Switch-Docked!");
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

void nxlink_callback(const NxlinkCallbackData *data) {
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

        auto events = evman::popall();
        // while (auto e = evman::pop()) {
        for (auto& e : events) {
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
                } else if constexpr(std::is_same_v<T, DownloadEventData>) {
                    log_write("[DownloadEventData] got event\n");
                    arg.callback(arg.data, arg.result);
                } else {
                    static_assert(false, "non-exhaustive visitor!");
                }
            }, e);
        }

        u32 w{},h{};
        switch (appletGetOperationMode()) {
            case AppletOperationMode_Handheld:
                w = 1280;
                h = 720;
                break;

            case AppletOperationMode_Console:
                w = 1920;
                h = 1080;
                break;
        }

        if (w != s_width || h != s_height) {
            s_width = w;
            s_height = h;
            m_scale.x = (float)s_width / SCREEN_WIDTH;
            m_scale.y = (float)s_height / SCREEN_HEIGHT;
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

auto App::GetThemeMetaList() -> std::span<ThemeMeta> {
    return g_app->m_theme_meta_entries;
}

void App::SetTheme(u64 theme_index) {
    g_app->LoadTheme(g_app->m_theme_meta_entries[theme_index].ini_path.c_str());
    g_app->m_theme_index = theme_index;
}

auto App::GetThemeIndex() -> u64 {
    return g_app->m_theme_index;
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

auto App::GetThemeShuffleEnable() -> bool {
    return g_app->m_theme_shuffle.Get();
}

auto App::GetThemeMusicEnable() -> bool {
    return g_app->m_theme_music.Get();
}

auto App::GetLanguage() -> long {
    return g_app->m_language.Get();
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
    g_app->m_replace_hbmenu.Set(enable);
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

void App::SetThemeShuffleEnable(bool enable) {
    g_app->m_theme_shuffle.Set(enable);
}

void App::SetThemeMusicEnable(bool enable) {
    g_app->m_theme_music.Set(enable);
    PlaySoundEffect(SoundEffect::SoundEffect_Music);
}

void App::SetLanguage(long index) {
    if (App::GetLanguage() != index) {
        g_app->m_language.Set(index);
        on_i18n_change();
    }
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

    padUpdate(&m_pad);
    m_controller.m_kdown = padGetButtonsDown(&m_pad);
    m_controller.m_kheld = padGetButtons(&m_pad);
    m_controller.m_kup = padGetButtonsUp(&m_pad);

    // dpad
    m_controller.UpdateButtonHeld(HidNpadButton_Left);
    m_controller.UpdateButtonHeld(HidNpadButton_Right);
    m_controller.UpdateButtonHeld(HidNpadButton_Down);
    m_controller.UpdateButtonHeld(HidNpadButton_Up);

    // ls
    m_controller.UpdateButtonHeld(HidNpadButton_StickLLeft);
    m_controller.UpdateButtonHeld(HidNpadButton_StickLRight);
    m_controller.UpdateButtonHeld(HidNpadButton_StickLDown);
    m_controller.UpdateButtonHeld(HidNpadButton_StickLUp);

    // rs
    m_controller.UpdateButtonHeld(HidNpadButton_StickRLeft);
    m_controller.UpdateButtonHeld(HidNpadButton_StickRRight);
    m_controller.UpdateButtonHeld(HidNpadButton_StickRDown);
    m_controller.UpdateButtonHeld(HidNpadButton_StickRUp);

    HidTouchScreenState touch_state{};
    hidGetTouchScreenStates(&touch_state, 1);

    if (touch_state.count == 1 && !m_touch_info.is_touching) {
        m_touch_info.initial_x = m_touch_info.prev_x = m_touch_info.cur_x = touch_state.touches[0].x;
        m_touch_info.initial_y = m_touch_info.prev_y = m_touch_info.cur_y = touch_state.touches[0].y;
        m_touch_info.finger_id = touch_state.touches[0].finger_id;
        m_touch_info.is_touching = true;
        m_touch_info.is_tap = true;
        // PlaySoundEffect(SoundEffect_Limit);
    } else if (touch_state.count >= 1 && m_touch_info.is_touching && m_touch_info.finger_id == touch_state.touches[0].finger_id) {
        m_touch_info.prev_x = m_touch_info.cur_x;
        m_touch_info.prev_y = m_touch_info.cur_y;

        m_touch_info.cur_x = touch_state.touches[0].x;
        m_touch_info.cur_y = touch_state.touches[0].y;

        if (m_touch_info.is_tap &&
            (std::abs(m_touch_info.initial_x - m_touch_info.cur_x) > 20 ||
            std::abs(m_touch_info.initial_y - m_touch_info.cur_y) > 20)) {
            m_touch_info.is_tap = false;
        }
    } else if (m_touch_info.is_touching) {
        m_touch_info.is_touching = false;

        // check if we clicked on anything, if so, handle it
        if (m_touch_info.is_tap) {
            // todo:
        }
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

    // NOTE: widgets should never pop themselves from drawing!
    for (auto& p : m_widgets) {
        if (!p->IsHidden()) {
            p->Draw(vg, &m_theme);
        }
    }

    m_notif_manager.Draw(vg, &m_theme);

    nvgResetTransform(vg);
    nvgEndFrame(this->vg);
    this->queue.presentImage(this->swapchain, slot);
}

auto App::GetVg() -> NVGcontext* {
    return g_app->vg;
}

void DrawElement(float x, float y, float w, float h, ThemeEntryID id) {
    const auto& e = g_app->m_theme.elements[id];

    switch (e.type) {
        case ElementType::None: {
        } break;
        case ElementType::Texture: {
            const auto paint = nvgImagePattern(g_app->vg, x, y, w, h, 0, e.texture, 1.f);
            ui::gfx::drawRect(g_app->vg, x, y, w, h, paint);
        } break;
        case ElementType::Colour: {
            ui::gfx::drawRect(g_app->vg, x, y, w, h, e.colour);
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
    } else if (value.starts_with('#')) {
        value = value.substr(1);
    }

    const u32 c = std::strtol(value.data(), nullptr, 16);
    if (c) {
        entry.colour = nvgRGBA((c >> 24) & 0xFF, (c >> 16) & 0xFF, (c >> 8) & 0xFF, c & 0xFF);
        entry.type = ElementType::Colour;
    }

    return entry;
}

auto App::LoadElement(std::string_view value) -> ElementEntry {
    if (value.size() <= 1) {
        return {};
    }

    if (auto e = LoadElementImage(value); e.type != ElementType::None) {
        return e;
    }

    if (auto e = LoadElementColour(value); e.type != ElementType::None) {
        return e;
    }

    return {};
}

void App::CloseTheme() {
    m_theme.name.clear();
    m_theme.author.clear();
    m_theme.version.clear();
    m_theme.path.clear();
    if (m_sound_ids[SoundEffect_Music]) {
        plsrPlayerFree(m_sound_ids[SoundEffect_Music]);
        m_sound_ids[SoundEffect_Music] = nullptr;
        plsrBFSTMClose(&m_theme.music);
    }
    for (auto& e : m_theme.elements) {
        if (e.type == ElementType::Texture) {
            nvgDeleteImage(vg, e.texture);
        }
        e.type = ElementType::None;
    }
}

void App::LoadTheme(const fs::FsPath& path) {
    // reset theme
    CloseTheme();
    m_theme.path = path;

    const auto cb = [](const mTCHAR *Section, const mTCHAR *Key, const mTCHAR *Value, void *UserData) -> int {
        auto app = static_cast<App*>(UserData);
        auto& theme = app->m_theme;
        std::string_view section{Section};
        std::string_view key{Key};
        std::string_view value{Value};

        if (section == "meta") {
            if (key == "name") {
                theme.name = key;
            } else if (key == "author") {
                theme.author = key;
            } else if (key == "version") {
                theme.version = key;
            }
        } else if (section == "theme") {
            if (key == "background") {
                theme.elements[ThemeEntryID_BACKGROUND] = app->LoadElement(value);
            } else if (key == "music") {
                if (R_SUCCEEDED(plsrBFSTMOpen(Value, &theme.music))) {
                    if (R_SUCCEEDED(plsrPlayerLoadStream(&theme.music, &app->m_sound_ids[SoundEffect_Music]))) {
                        app->PlaySoundEffect(SoundEffect_Music);
                    }
                }
            } else if (key == "grid") {
                theme.elements[ThemeEntryID_GRID] = app->LoadElement(value);
            } else if (key == "selected") {
                theme.elements[ThemeEntryID_SELECTED] = app->LoadElement(value);
            } else if (key == "selected_overlay") {
                theme.elements[ThemeEntryID_SELECTED_OVERLAY] = app->LoadElement(value);
            } else if (key == "text") {
                theme.elements[ThemeEntryID_TEXT] = app->LoadElementColour(value);
            } else if (key == "text_selected") {
                theme.elements[ThemeEntryID_TEXT_SELECTED] = app->LoadElementColour(value);
            } else if (key == "icon_audio") {
                theme.elements[ThemeEntryID_ICON_AUDIO] = app->LoadElement(value);
            } else if (key == "icon_video") {
                theme.elements[ThemeEntryID_ICON_VIDEO] = app->LoadElement(value);
            } else if (key == "icon_image") {
                theme.elements[ThemeEntryID_ICON_IMAGE] = app->LoadElement(value);
            } else if (key == "icon_file") {
                theme.elements[ThemeEntryID_ICON_FILE] = app->LoadElement(value);
            } else if (key == "icon_folder") {
                theme.elements[ThemeEntryID_ICON_FOLDER] = app->LoadElement(value);
            } else if (key == "icon_zip") {
                theme.elements[ThemeEntryID_ICON_ZIP] = app->LoadElement(value);
            } else if (key == "icon_game") {
                theme.elements[ThemeEntryID_ICON_GAME] = app->LoadElement(value);
            } else if (key == "icon_nro") {
                theme.elements[ThemeEntryID_ICON_NRO] = app->LoadElement(value);
            }
        }

        return 1;
    };

    if (R_SUCCEEDED(romfsInit())) {
        ON_SCOPE_EXIT(romfsExit());
        if (!ini_browse(cb, this, path)) {
            log_write("failed to open ini: %s\n", path);
        } else {
            log_write("opened ini: %s\n", path);
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

        const std::string name = d->d_name;
        if (!name.ends_with(".ini")) {
            continue;
        }

        const auto full_path = path + name;

        if (!ini_haskey("meta", "name", full_path.c_str())) {
            continue;
        }
        if (!ini_haskey("meta", "author", full_path.c_str())) {
            continue;
        }
        if (!ini_haskey("meta", "version", full_path.c_str())) {
            continue;
        }

        ThemeMeta meta{};

        char buf[FS_MAX_PATH]{};
        int len{};
        len = ini_gets("meta", "name", "", buf, sizeof(buf) - 1, full_path.c_str());
        if (len <= 1) {
            continue;
        }
        meta.name = buf;

        len = ini_gets("meta", "author", "", buf, sizeof(buf) - 1, full_path.c_str());
        if (len <= 1) {
            continue;
        }
        meta.author = buf;

        len = ini_gets("meta", "version", "", buf, sizeof(buf) - 1, full_path.c_str());
        if (len <= 1) {
            continue;
        }
        meta.version = buf;

        meta.ini_path = full_path;
        m_theme_meta_entries.emplace_back(meta);
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
        g_app->m_replace_hbmenu.Set(true);
    } else {
        g_app->m_replace_hbmenu.Set(false);
    }
    fs::FsNativeSd fs;
    fs.CreateDirectoryRecursively("/config/sphaira/assoc");
    fs.CreateDirectoryRecursively("/config/sphaira/themes");

    if (App::GetLogEnable()) {
        log_file_init();
        log_write("hello world\n");
    }

    if (App::GetNxlinkEnable()) {
        nxlinkInitialize(nxlink_callback);
    }

    DownloadInit();

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

    this->renderer.emplace(SCREEN_WIDTH, SCREEN_HEIGHT, this->device, this->queue, *this->pool_images, *this->pool_code, *this->pool_data);
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

    if (R_SUCCEEDED(romfsMountDataStorageFromProgram(0x0100000000001000, "qlaunch"))) {
        ON_SCOPE_EXIT(romfsUnmount("qlaunch"));
        plsrPlayerInit();
        plsrBFSAROpen("qlaunch:/sound/qlaunch.bfsar", &m_qlaunch_bfsar);

        plsrPlayerLoadSoundByName(&m_qlaunch_bfsar, "SeGameIconFocus", &m_sound_ids[SoundEffect_Focus]);
        plsrPlayerLoadSoundByName(&m_qlaunch_bfsar, "SeGameIconScroll", &m_sound_ids[SoundEffect_Scroll]);
        plsrPlayerLoadSoundByName(&m_qlaunch_bfsar, "SeGameIconLimit", &m_sound_ids[SoundEffect_Limit]);
        plsrPlayerLoadSoundByName(&m_qlaunch_bfsar, "SeStartupMenu_game", &m_sound_ids[SoundEffect_Startup]);
        plsrPlayerLoadSoundByName(&m_qlaunch_bfsar, "SeGameIconAdd", &m_sound_ids[SoundEffect_Install]);
        plsrPlayerLoadSoundByName(&m_qlaunch_bfsar, "SeInsertError", &m_sound_ids[SoundEffect_Error]);

        plsrPlayerSetVolume(m_sound_ids[SoundEffect_Limit], 2.0f);
        plsrPlayerSetVolume(m_sound_ids[SoundEffect_Focus], 0.5f);
        PlaySoundEffect(SoundEffect_Startup);
    } else {
        log_write("failed to mount romfs 0x0100000000001000\n");
    }

    ScanThemeEntries();

    fs::FsPath theme_path{};
    if (App::GetThemeShuffleEnable() && m_theme_meta_entries.size()) {
        theme_path = m_theme_meta_entries[randomGet64() % m_theme_meta_entries.size()].ini_path;
    } else {
        ini_gets("config", "theme", "romfs:/themes/abyss_theme.ini", theme_path, sizeof(theme_path), CONFIG_PATH);
    }
    LoadTheme(theme_path);

    // find theme index using the path of the theme.ini
    for (u64 i = 0; i < m_theme_meta_entries.size(); i++) {
        if (m_theme.path == m_theme_meta_entries[i].ini_path) {
            m_theme_index = i;
            break;
        }
    }

    appletHook(&m_appletHookCookie, appplet_hook_calback, this);

    hidInitializeTouchScreen();
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
            log_write("launching from unknown forwader: %.*s size: %zu\n", loader_info_size, envGetLoaderInfo(), loader_info_size);
        }
    } else {
        log_write("not launching from forwarder\n");
    }

    ini_putl(GetExePath(), "timestamp", m_start_timestamp, App::PLAYLOG_PATH);
    const long old_launch_count = ini_getl(GetExePath(), "launch_count", 0, App::PLAYLOG_PATH);
    ini_putl(GetExePath(), "launch_count", old_launch_count + 1, App::PLAYLOG_PATH);

    s64 sd_free_space;
    if (R_SUCCEEDED(fs.GetFreeSpace("/", &sd_free_space))) {
        log_write("sd_free_space: %zd\n", sd_free_space);
    }
    s64 sd_total_space;
    if (R_SUCCEEDED(fs.GetTotalSpace("/", &sd_total_space))) {
        log_write("sd_total_space: %zd\n", sd_total_space);
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

App::~App() {
    log_write("starting to exit\n");

    i18n::exit();
    DownloadExit();

    // this has to be called before any cleanup to ensure the lifetime of
    // nvg is still active as some widgets may need to free images.
    m_widgets.clear();

    appletUnhook(&m_appletHookCookie);

    ini_puts("config", "theme", m_theme.path, CONFIG_PATH);

    CloseTheme();

    // Free any loaded sound from memory
    for (auto id : m_sound_ids) {
        if (id) {
            plsrPlayerFree(id);
        }
    }

	// Close the archive
	plsrBFSARClose(&m_qlaunch_bfsar);

	// De-initialize our player
	plsrPlayerExit();

    this->destroyFramebufferResources();
    nvgDeleteDk(this->vg);
    this->renderer.reset();

    // backup hbmenu if it is not sphaira
    if (App::GetReplaceHbmenuEnable() && !IsHbmenu()) {
        NacpStruct nacp;
        fs::FsNativeSd fs;
        if (R_SUCCEEDED(nro_get_nacp("/hbmenu.nro", nacp)) && std::strcmp(nacp.lang[0].name, "nx-hbmenu") == 0) {
            log_write("backing up hbmenu\n");
            if (R_FAILED(fs.copy_entire_file("/switch/hbmenu.nro", "/hbmenu.nro", true))) {
                log_write("failed to copy hbmenu.nro\n");
            }
        } else {
            log_write("not backing up\n");
        }

        Result rc;
        if (R_FAILED(rc = fs.copy_entire_file("/hbmenu.nro", GetExePath(), true))) {
            log_write("failed to copy entire file: %s 0x%X module: %u desc: %u\n", GetExePath(), rc, R_MODULE(rc), R_DESCRIPTION(rc));
        } else {
            log_write("success with copying over root file!\n");
        }
    } else if (IsHbmenu()) {
        fs::FsNativeSd fs;
        if (!App::GetReplaceHbmenuEnable()) {
            log_write("Restoring Sphaira\n");
            if (R_FAILED(fs.copy_entire_file("/switch/sphaira/sphaira.nro", GetExePath(), true))) {
                log_write("failed to restore Sphaira\n");
            } else {
                log_write("success with restoring Sphaira!\n");
                NacpStruct nacp;
                if (R_SUCCEEDED(nro_get_nacp("/switch/hbmenu.nro", nacp)) && std::strcmp(nacp.lang[0].name, "nx-hbmenu") == 0) {
                    log_write("Restoring HBMenu\n");
                    if (R_FAILED(fs.copy_entire_file("/hbmenu.nro", "/switch/hbmenu.nro", true))) {
                        log_write("failed to restore hbmenu.nro\n");
                    }
                } else {
                    log_write("not restoring HBMenu\n");
                }
            }
        } else {
            // check we have a version that's newer than current.
            NacpStruct sphaira_nacp;
            fs::FsPath sphaira_path = "/switch/sphaira/sphaira.nro";
            Result rc;

            rc = nro_get_nacp(sphaira_path, sphaira_nacp);
            if (R_FAILED(rc) || std::strcmp(sphaira_nacp.lang[0].name, "sphaira") != 0) {
                sphaira_path = "/switch/sphaira.nro";
                rc = nro_get_nacp(sphaira_path, sphaira_nacp);
            }

            // found sphaira, now lets get compare version
            if (R_SUCCEEDED(rc) && !std::strcmp(sphaira_nacp.lang[0].name, "sphaira") == 0) {
                if (std::strcmp(APP_VERSION, sphaira_nacp.display_version) < 0) {
                    if (R_FAILED(rc = fs.copy_entire_file(GetExePath(), sphaira_path, true))) {
                        log_write("failed to copy entire file: %s 0x%X module: %u desc: %u\n", sphaira_path, rc, R_MODULE(rc), R_DESCRIPTION(rc));
                    } else {
                        log_write("success with updating hbmenu!\n");
                    }
                }
            }
        }
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
