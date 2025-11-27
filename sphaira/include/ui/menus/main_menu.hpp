#pragma once

#include "ui/widget.hpp"
#include "ui/menus/menu_base.hpp"
#include <span>

namespace sphaira::ui::menu::main {

enum class UpdateState {
    // still downloading json from github
    Pending,
    // no update available.
    None,
    // update available!
    Update,
    // there was an error whilst checking for updates.
    Error,
};

using MiscMenuFunction = std::function<std::unique_ptr<MenuBase>(u32 flags)>;

enum MiscMenuFlag : u8 {
    // can be set as the rightside menu.
    MiscMenuFlag_Shortcut = 1 << 0,
    // needs install option to be enabled.
    MiscMenuFlag_Install = 1 << 1,
};

struct MiscMenuEntry {
    const char* name;
    const char* title;
    MiscMenuFunction func;
    u8 flag;
    const char* info;

    auto IsShortcut() const -> bool {
        return flag & MiscMenuFlag_Shortcut;
    }

    auto IsInstall() const -> bool {
        return flag & MiscMenuFlag_Install;
    }
};

auto GetMenuMenuEntries() -> std::span<const MiscMenuEntry>;

// this holds 2 menus and allows for switching between them
struct MainMenu final : Widget {
    MainMenu();
    ~MainMenu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;
    void OnFocusLost() override;

    auto IsMenu() const -> bool override {
        return true;
    }

private:
    void OnLRPress(MenuBase* menu, Button b);
    void AddOnLRPress();

private:
    std::unique_ptr<MenuBase> m_centre_menu{};
    std::unique_ptr<MenuBase> m_left_menu{};
    std::unique_ptr<MenuBase> m_right_menu{};
    MenuBase* m_current_menu{};

    std::string m_update_url{};
    std::string m_update_version{};
    std::string m_update_description{};
    UpdateState m_update_state{UpdateState::Pending};
};

} // namespace sphaira::ui::menu::main
