#pragma once

#include "nanovg.h"
#include "nanovg/dk_renderer.hpp"
#include "pulsar.h"
#include "ui/widget.hpp"
#include "ui/notification.hpp"
#include "owo.hpp"
#include "option.hpp"
#include "fs.hpp"

#include <switch.h>
#include <vector>
#include <string>
#include <span>
#include <optional>

namespace sphaira {

enum SoundEffect {
    SoundEffect_Music,
    SoundEffect_Focus,
    SoundEffect_Scroll,
    SoundEffect_Limit,
    SoundEffect_Startup,
    SoundEffect_Install,
    SoundEffect_Error,
    SoundEffect_MAX,
};

enum class LaunchType {
    Normal,
    Forwader_Unknown,
    Forwader_Sphaira,
};

// todo: why is this global???
void DrawElement(float x, float y, float w, float h, ThemeEntryID id);
void DrawElement(const Vec4&, ThemeEntryID id);

class App {
public:
    App(const char* argv0);
    ~App();
    void Loop();

    static void Exit();
    static void ExitRestart();
    static auto GetVg() -> NVGcontext*;
    static void Push(std::shared_ptr<ui::Widget>);
    // pops all widgets above a menu
    static void PopToMenu();

    // this is thread safe
    static void Notify(std::string text, ui::NotifEntry::Side side = ui::NotifEntry::Side::RIGHT);
    static void Notify(ui::NotifEntry entry);
    static void NotifyPop(ui::NotifEntry::Side side = ui::NotifEntry::Side::RIGHT);
    static void NotifyClear(ui::NotifEntry::Side side = ui::NotifEntry::Side::RIGHT);
    static void NotifyFlashLed();

    static auto GetThemeMetaList() -> std::span<ThemeMeta>;
    static void SetTheme(s64 theme_index);
    static auto GetThemeIndex() -> s64;

    static auto GetDefaultImage(int* w = nullptr, int* h = nullptr) -> int;

    // returns argv[0]
    static auto GetExePath() -> fs::FsPath;
    // returns true if we are hbmenu.
    static auto IsHbmenu() -> bool;

    static auto GetMtpEnable() -> bool;
    static auto GetFtpEnable() -> bool;
    static auto GetNxlinkEnable() -> bool;
    static auto GetLogEnable() -> bool;
    static auto GetReplaceHbmenuEnable() -> bool;
    static auto GetInstallEnable() -> bool;
    static auto GetInstallSdEnable() -> bool;
    static auto GetInstallPrompt() -> bool;
    static auto GetThemeShuffleEnable() -> bool;
    static auto GetThemeMusicEnable() -> bool;
    static auto Get12HourTimeEnable() -> bool;
    static auto GetLanguage() -> long;
    static auto GetTextScrollSpeed() -> long;

    static void SetMtpEnable(bool enable);
    static void SetFtpEnable(bool enable);
    static void SetNxlinkEnable(bool enable);
    static void SetLogEnable(bool enable);
    static void SetReplaceHbmenuEnable(bool enable);
    static void SetInstallEnable(bool enable);
    static void SetInstallSdEnable(bool enable);
    static void SetInstallPrompt(bool enable);
    static void SetThemeShuffleEnable(bool enable);
    static void SetThemeMusicEnable(bool enable);
    static void Set12HourTimeEnable(bool enable);
    static void SetLanguage(long index);
    static void SetTextScrollSpeed(long index);

    static auto Install(OwoConfig& config) -> Result;
    static auto Install(ui::ProgressBox* pbox, OwoConfig& config) -> Result;

    static void PlaySoundEffect(SoundEffect effect);

    void Draw();
    void Update();
    void Poll();

    // void DrawElement(float x, float y, float w, float h, ui::ThemeEntryID id);
    auto LoadElementImage(std::string_view value) -> ElementEntry;
    auto LoadElementColour(std::string_view value) -> ElementEntry;
    auto LoadElement(std::string_view data, ElementType type) -> ElementEntry;

    void LoadTheme(const ThemeMeta& meta);
    void CloseTheme();
    void ScanThemes(const std::string& path);
    void ScanThemeEntries();

    static auto IsApplication() -> bool {
        const auto type = appletGetAppletType();
        return type == AppletType_Application || type == AppletType_SystemApplication;
    }

    static auto IsApplet() -> bool {
        return !IsApplication();
    }

    // returns true if launched in applet mode with a title suspended in the background.
    static auto IsAppletWithSuspendedApp() -> bool {
        R_UNLESS(IsApplet(), false);
        R_TRY_RESULT(pmdmntInitialize(), false);
        ON_SCOPE_EXIT(pmdmntExit());

        u64 pid;
        return R_SUCCEEDED(pmdmntGetApplicationProcessId(&pid));
    }


// private:
    static constexpr inline auto CONFIG_PATH = "/config/sphaira/config.ini";
    static constexpr inline auto PLAYLOG_PATH = "/config/sphaira/playlog.ini";
    static constexpr inline auto INI_SECTION = "config";

    fs::FsPath m_app_path;
    u64 m_start_timestamp{};
    u64 m_prev_timestamp{};
    fs::FsPath m_prev_last_launch{};
    int m_default_image{};

    bool m_is_launched_via_sphaira_forwader{};

    NVGcontext* vg{};
    PadState m_pad{};
    TouchInfo m_touch_info{};
    Controller m_controller{};
    std::vector<ThemeMeta> m_theme_meta_entries;

    Vec2 m_scale{1, 1};

    std::vector<std::shared_ptr<ui::Widget>> m_widgets;
    u32 m_pop_count{};
    ui::NotifMananger m_notif_manager{};

    AppletHookCookie m_appletHookCookie{};

    Theme m_theme{};
    fs::FsPath theme_path{};
    s64 m_theme_index{};

    bool m_quit{};

    option::OptionBool m_nxlink_enabled{INI_SECTION, "nxlink_enabled", true};
    option::OptionBool m_mtp_enabled{INI_SECTION, "mtp_enabled", false};
    option::OptionBool m_ftp_enabled{INI_SECTION, "ftp_enabled", false};
    option::OptionBool m_log_enabled{INI_SECTION, "log_enabled", false};
    option::OptionBool m_replace_hbmenu{INI_SECTION, "replace_hbmenu", false};
    option::OptionBool m_install{INI_SECTION, "install", false};
    option::OptionBool m_install_sd{INI_SECTION, "install_sd", true};
    option::OptionLong m_install_prompt{INI_SECTION, "install_prompt", true};
    option::OptionBool m_theme_shuffle{INI_SECTION, "theme_shuffle", false};
    option::OptionBool m_theme_music{INI_SECTION, "theme_music", true};
    option::OptionBool m_12hour_time{INI_SECTION, "12hour_time", false};
    option::OptionLong m_language{INI_SECTION, "language", 0}; // auto
    // todo: move this into it's own menu
    option::OptionLong m_text_scroll_speed{"accessibility", "text_scroll_speed", 1}; // normal

    PLSR_PlayerSoundId m_sound_ids[SoundEffect_MAX]{};

private: // from nanovg decko3d example by adubbz
    static constexpr unsigned NumFramebuffers = 2;
    static constexpr unsigned StaticCmdSize = 0x1000;
    unsigned s_width{1280};
    unsigned s_height{720};
    dk::UniqueDevice device;
    dk::UniqueQueue queue;
    std::optional<CMemPool> pool_images;
    std::optional<CMemPool> pool_code;
    std::optional<CMemPool> pool_data;
    dk::UniqueCmdBuf cmdbuf;
    CMemPool::Handle depthBuffer_mem;
    CMemPool::Handle framebuffers_mem[NumFramebuffers];
    dk::Image depthBuffer;
    dk::Image framebuffers[NumFramebuffers];
    DkCmdList framebuffer_cmdlists[NumFramebuffers];
    dk::UniqueSwapchain swapchain;
    DkCmdList render_cmdlist;
    std::optional<nvg::DkRenderer> renderer;
    void createFramebufferResources();
    void destroyFramebufferResources();
    void recordStaticCommands();
};

} // namespace sphaira
