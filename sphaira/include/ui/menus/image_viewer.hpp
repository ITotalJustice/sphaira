#pragma once

#include "ui/widget.hpp"
#include "fs.hpp"
#include <vector>

namespace sphaira::ui::menu::imageview {

struct Menu final : Widget {
    Menu(fs::Fs* fs, const fs::FsPath& path);
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;

    auto IsMenu() const -> bool override {
        return true;
    }

    void UpdateSize();

private:
    const fs::FsPath m_path;
    int m_image{};
    float m_image_width{};
    float m_image_height{};

    // for zoom, 0.1 - 1.0
    float m_zoom{1};

    // for pan.
    float m_xoff{};
    float m_yoff{};
};

} // namespace sphaira::ui::menu::imageview
