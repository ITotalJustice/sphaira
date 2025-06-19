#pragma once

#include "ui/menus/grid_menu_base.hpp"
#include "ui/list.hpp"
#include "nro.hpp"
#include "fs.hpp"
#include "option.hpp"

namespace sphaira::ui::menu::homebrew {

enum SortType {
    SortType_Updated,
    SortType_Alphabetical,
    SortType_Size,
    SortType_UpdatedStar,
    SortType_AlphabeticalStar,
    SortType_SizeStar,
};

enum OrderType {
    OrderType_Descending,
    OrderType_Ascending,
};

using LayoutType = grid::LayoutType;

auto GetNroEntries() -> std::span<const NroEntry>;
void SignalChange();

struct Menu final : grid::Menu {
    Menu();
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "Apps"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

    auto GetHomebrewList() const -> const std::vector<NroEntry>& {
        return m_entries;
    }

    static Result InstallHomebrew(const fs::FsPath& path, const std::vector<u8>& icon);
    static Result InstallHomebrewFromPath(const fs::FsPath& path);

private:
    void SetIndex(s64 index);
    void InstallHomebrew();
    void ScanHomebrew();
    void Sort();
    void SortAndFindLastFile(bool scan = false);
    void FreeEntries();
    void OnLayoutChange();

    auto IsStarEnabled() -> bool {
        return m_sort.Get() >= SortType_UpdatedStar;
    }

private:
    static constexpr inline const char* INI_SECTION = "homebrew";

    std::vector<NroEntry> m_entries{};
    s64 m_index{}; // where i am in the array
    std::unique_ptr<List> m_list{};
    bool m_dirty{};

    option::OptionLong m_sort{INI_SECTION, "sort", SortType::SortType_AlphabeticalStar};
    option::OptionLong m_order{INI_SECTION, "order", OrderType::OrderType_Descending};
    option::OptionLong m_layout{INI_SECTION, "layout", LayoutType::LayoutType_GridDetail};
    option::OptionBool m_hide_sphaira{INI_SECTION, "hide_sphaira", false};
};

} // namespace sphaira::ui::menu::homebrew
