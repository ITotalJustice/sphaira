#pragma once

#include "ui/widget.hpp"
#include "ui/scrolling_text.hpp"
#include "fs.hpp"
#include "utils/audio.hpp"
#include <memory>

namespace sphaira::ui::music {

struct Menu final : Widget {
    Menu(fs::Fs* fs, const fs::FsPath& path);
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;

private:
    void PauseToggle();
    void SeekForward();
    void SeekBack();

    void IncreaseVolume();
    void DecreaseVolume();

private:
    audio::SongID m_song{};
    audio::Info m_info{};
    audio::Meta m_meta{};
    // only set if metadata was loaded.
    int m_icon{};

    ScrollingText m_scroll_title{};
    ScrollingText m_scroll_artist{};
    ScrollingText m_scroll_album{};

    // from movienx
    static constexpr Vec4 osd_progress_bar{400.f, 550, 1280.f - (400.f * 2.f), 10.f};
    // static constexpr Vec4 osd_progress_bar{300.f, SCREEN_HEIGHT / 2 - 15 / 2, 1280.f - (300.f * 2.f), 10.f};
    static constexpr Vec2 osd_time_text_left{osd_progress_bar.x - 12.f, osd_progress_bar.y - 2.f};
    static constexpr Vec2 osd_time_text_right{osd_progress_bar.x + osd_progress_bar.w + 12.f, osd_progress_bar.y - 2.f};
    static constexpr Vec4 osd_bar_outline{osd_time_text_left.x - 80, osd_progress_bar.y - 30, osd_progress_bar.w + 80 * 2 + 30, osd_progress_bar.h + 30 + 30};
};

} // namespace sphaira::ui::music
