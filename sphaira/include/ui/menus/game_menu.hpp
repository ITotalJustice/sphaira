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
    s64 size{};
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

enum SortType {
    SortType_Updated,
};

enum OrderType {
    OrderType_Descending,
    OrderType_Ascending,
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "Games"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

private:
    void SetIndex(s64 index);
    void ScanHomebrew();
    void Sort();
    void SortAndFindLastFile();
    void FreeEntries();

private:
    static constexpr inline const char* INI_SECTION = "games";

    std::vector<Entry> m_entries{};
    s64 m_index{}; // where i am in the array
    std::unique_ptr<List> m_list{};
    bool m_is_reversed{};
    bool m_dirty{};

    ScrollingText m_scroll_name{};
    ScrollingText m_scroll_author{};
    ScrollingText m_scroll_version{};

    option::OptionLong m_sort{INI_SECTION, "sort", SortType::SortType_Updated};
    option::OptionLong m_order{INI_SECTION, "order", OrderType::OrderType_Descending};
    option::OptionBool m_hide_forwarders{INI_SECTION, "hide_forwarders", false};
};

} // namespace sphaira::ui::menu::game
