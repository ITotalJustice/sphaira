#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/scrollable_text.hpp"
#include "fs.hpp"

namespace sphaira::ui::menu::fileview {

struct Menu final : MenuBase {
    Menu(const fs::FsPath& path);
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "File"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

private:
    const fs::FsPath m_path;
    fs::FsNativeSd m_fs{};
    FsFile m_file{};
    s64 m_file_size{};
    s64 m_file_offset{};

    std::unique_ptr<ScrollableText> m_scroll_text{};

    s64 m_start{};
    s64 m_index{}; // where i am in the array
};

} // namespace sphaira::ui::menu::fileview
