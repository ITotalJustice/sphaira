#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/scrolling_text.hpp"
#include "ui/list.hpp"
#include "fs.hpp"
#include "option.hpp"
#include <memory>

namespace sphaira::ui::menu::game {

enum class NacpLoadStatus {
    // not yet attempted to be loaded.
    None,
    // loaded, ready to parse.
    Loaded,
    // failed to load, do not attempt to load again!
    Error,
};

struct Entry {
    u64 app_id{};
    char display_version[0x10]{};
    NacpLanguageEntry lang{};
    int image{};

    std::unique_ptr<NsApplicationControlData> control{};
    u64 control_size{};
    NacpLoadStatus status{NacpLoadStatus::None};

    auto GetName() const -> const char* {
        return lang.name;
    }

    auto GetAuthor() const -> const char* {
        return lang.author;
    }

    auto GetDisplayVersion() const -> const char* {
        return display_version;
    }
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

private:
    void SetIndex(s64 index);
    void ScanHomebrew();
    void FreeEntries();

private:
    std::vector<Entry> m_entries{};
    s64 m_index{}; // where i am in the array
    std::unique_ptr<List> m_list{};

    ScrollingText m_scroll_name{};
    ScrollingText m_scroll_author{};
    ScrollingText m_scroll_version{};
};

} // namespace sphaira::ui::menu::game
