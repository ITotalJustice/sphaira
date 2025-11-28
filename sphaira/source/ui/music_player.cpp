#include "ui/music_player.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"
#include "i18n.hpp"
#include "image.hpp"

namespace sphaira::ui::music {
namespace {

constexpr u64 MAX_SEEK_DELTA = 30;
constexpr float VOLUME_DELTA = 0.20;

// returns seconds as: hh:mm:ss (from movienx)
inline auto TimeFormat(u64 sec) -> std::string {
    char buf[9];

    if (sec < 60) {
        if (!sec) {
            return "0:00";
        }
        std::sprintf(buf, "0:%02lu", sec % 60);
    } else if (sec < 3600) {
        std::sprintf(buf, "%lu:%02lu", ((sec / 60) % 60), sec % 60);
    } else {
        std::sprintf(buf, "%lu:%02lu:%02lu", ((sec / 3600) % 24), ((sec / 60) % 60), sec % 60);
    }

    return std::string{buf};
}

} // namespace

Menu::Menu(fs::Fs* fs, const fs::FsPath& path) {
    SetAction(Button::B, Action{[this](){
        SetPop();
    }});

    SetAction(Button::A, Action{[this](){
        PauseToggle();
    }});

    SetAction(Button::LEFT, Action{[this](){
        SeekBack();
    }});

    SetAction(Button::RIGHT, Action{[this](){
        SeekForward();
    }});

    SetAction(Button::RS_UP, Action{[this](){
        IncreaseVolume();
    }});
    SetAction(Button::RS_DOWN, Action{[this](){
        DecreaseVolume();
    }});

    App::SetBackgroundMusicPause(true);
    App::SetAutoSleepDisabled(true);

    if (auto rc = lblInitialize(); R_FAILED(rc)) {
        log_write("lblInitialize() failed: 0x%X\n", rc);
    }

    if (auto rc = audio::OpenSong(fs, path, 0, &m_song); R_FAILED(rc)) {
        App::PushErrorBox(rc, "Failed to load music"_i18n);
        SetPop();
        return;
    }

    audio::GetInfo(m_song, &m_info);
    audio::GetMeta(m_song, &m_meta);

    if (!m_meta.image.empty()) {
        m_icon = nvgCreateImageMem(App::GetVg(), 0, m_meta.image.data(), m_meta.image.size());
    }

    if (m_icon > 0) {
        if (m_meta.title.empty()) {
            m_meta.title = path.toString();

            // only keep file name.
            if (auto i = m_meta.title.find_last_of('/'); i != std::string::npos) {
                m_meta.title = m_meta.title.substr(i + 1);
            }

            // remove extension.
            if (auto i = m_meta.title.find_last_of('.'); i != std::string::npos) {
                m_meta.title = m_meta.title.substr(0, i);
            }
        }

        if (m_meta.artist.empty()) {
            m_meta.artist = "Artist: Unknown"_i18n;
        }

        if (m_meta.album.empty()) {
            m_meta.album = "Album: Unknown"_i18n;
        }
    }

    audio::PlaySong(m_song);
}

Menu::~Menu() {
    if (m_song) {
        audio::CloseSong(&m_song);
    }

    if (m_icon) {
        nvgDeleteImage(App::GetVg(), m_icon);
    }

    App::SetAutoSleepDisabled(false);
    App::SetBackgroundMusicPause(false);

    // restore backlight.
    appletSetLcdBacklightOffEnabled(false);
    lblExit();
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    Widget::Update(controller, touch);

    if (controller->m_kdown) {
        LblBacklightSwitchStatus status;
        // if any button was pressed and the screen is disabled, restore it.
        if (R_SUCCEEDED(lblGetBacklightSwitchStatus(&status))) {
            if (status != LblBacklightSwitchStatus_Enabled) {
                appletSetLcdBacklightOffEnabled(false);
            } else if (controller->GotDown(Button::Y)) {
                // use applet here because it handles restoring backlight
                // when pressing the home / power button.
                appletSetLcdBacklightOffEnabled(true);
            }
        }
    }
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    audio::Progress song_progress{};
    audio::State song_state;
    if (R_FAILED(audio::GetProgress(m_song, &song_progress, &song_state))) {
        log_write("failed get song_progress\n");
        SetPop();
        return;
    }

    if (song_state == audio::State::Finished || song_state == audio::State::Error) {
        log_write("got finished, doing pop now\n");
        SetPop();
        return;
    }

    const auto duration = (float)m_info.sample_count / (float)m_info.sample_rate;
    const auto progress = (float)song_progress.played / (float)m_info.sample_rate;
    const auto remaining = duration - progress;

    const auto get_inner = [](const Vec4& bar, float progress, float duration) {
        auto bar_inner = bar;
        bar_inner.y += 2;
        bar_inner.h -= 2 * 2;
        bar_inner.x += 2;
        bar_inner.w -= 2 * 2;

        bar_inner.w *= progress / duration;
        return bar_inner;
    };

    gfx::dimBackground(vg);

    if (m_icon) {
        const float icon_size = 220;
        // draw background grid.
        const auto pad = 30;
        const auto gridy = (SCREEN_HEIGHT / 2) - (icon_size / 2) - pad;
        const auto gridh = icon_size + pad * 2;
        const Vec4 grid{this->osd_bar_outline.x, gridy, this->osd_bar_outline.w, gridh};
        gfx::drawRect(vg, grid, theme->GetColour(ThemeEntryID_GRID), 15);

        nvgSave(vg);
        nvgIntersectScissor(vg, grid.x + pad, grid.y + pad, grid.w - pad * 2, grid.h - pad * 2);
        ON_SCOPE_EXIT(nvgRestore(vg));

        // draw icon.
        const Vec4 icon{grid.x + pad, grid.y + pad, icon_size, icon_size};
        gfx::drawImage(vg, icon, m_icon, 0);

        // draw meta info.
        const auto xoff = icon.x + icon_size + pad;
        const auto wend = grid.w - (xoff - grid.x) - 30;
        m_scroll_title.Draw(vg, true, xoff, icon.y + 50, wend, 22, NVG_ALIGN_LEFT|NVG_ALIGN_BOTTOM, theme->GetColour(ThemeEntryID_TEXT), m_meta.title);
        m_scroll_artist.Draw(vg, true, xoff, icon.y + 90, wend, 20, NVG_ALIGN_LEFT|NVG_ALIGN_BOTTOM, theme->GetColour(ThemeEntryID_TEXT_INFO), m_meta.artist);
        m_scroll_album.Draw(vg, true, xoff, icon.y + 130, wend, 20, NVG_ALIGN_LEFT|NVG_ALIGN_BOTTOM, theme->GetColour(ThemeEntryID_TEXT_INFO), m_meta.album);

        // draw progress bar.
        const Vec4 progress_bar{xoff, grid.y + grid.h - 30 - 60, osd_bar_outline.w - (xoff - osd_bar_outline.x) - 30, 10.f};
        const auto inner = get_inner(progress_bar, progress, duration);
        gfx::drawRect(vg, progress_bar, theme->GetColour(ThemeEntryID_PROGRESSBAR_BACKGROUND), 3);
        gfx::drawRect(vg, inner, theme->GetColour(ThemeEntryID_PROGRESSBAR), 3);

        // draw progress time text.
        const Vec2 time_text_left{progress_bar.x, progress_bar.y + progress_bar.h + 20};
        const Vec2 time_text_right{progress_bar.x + progress_bar.w, progress_bar.y + progress_bar.h + 20};
        gfx::drawText(vg, time_text_left, 18.f, theme->GetColour(ThemeEntryID_TEXT), TimeFormat(progress).c_str(), NVG_ALIGN_LEFT | NVG_ALIGN_TOP);
        gfx::drawText(vg, time_text_right, 18.f, theme->GetColour(ThemeEntryID_TEXT), TimeFormat(duration).c_str(), NVG_ALIGN_RIGHT | NVG_ALIGN_TOP);
    } else {
        // draw background grid.
        gfx::drawRect(vg, this->osd_bar_outline, theme->GetColour(ThemeEntryID_POPUP), 15);

        // draw progress bar.
        const auto inner = get_inner(this->osd_progress_bar, progress, duration);
        gfx::drawRect(vg, this->osd_progress_bar, theme->GetColour(ThemeEntryID_PROGRESSBAR_BACKGROUND), 3);
        gfx::drawRect(vg, inner, theme->GetColour(ThemeEntryID_PROGRESSBAR), 3);

        // draw chapter markers (if any)
        if (m_info.looping) {
            const auto loop = (float)m_info.loop_start / (float)m_info.sample_rate;
            const auto marker = Vec4{osd_progress_bar.x + (osd_progress_bar.w * loop / duration), osd_progress_bar.y - 4.f, 3.f, osd_progress_bar.h + 8.f};
            gfx::drawRect(vg, marker, theme->GetColour(ThemeEntryID_TEXT_INFO));
        }

        // draw progress time text.
        gfx::drawText(vg, this->osd_time_text_left, 20.f, theme->GetColour(ThemeEntryID_TEXT), TimeFormat((progress)).c_str(), NVG_ALIGN_RIGHT | NVG_ALIGN_TOP);
        gfx::drawText(vg, this->osd_time_text_right, 20.f, theme->GetColour(ThemeEntryID_TEXT), ('-' + TimeFormat((remaining))).c_str(), NVG_ALIGN_LEFT | NVG_ALIGN_TOP);
    }
}

void Menu::PauseToggle() {
    audio::State state{};
    audio::GetProgress(m_song, nullptr, &state);

    if (state == audio::State::Playing) {
        audio::PauseSong(m_song);
    } else if (state == audio::State::Paused) {
        audio::PlaySong(m_song);
    }
}

void Menu::SeekForward() {
    audio::Progress progress{};
    audio::GetProgress(m_song, &progress, nullptr);

    const u64 max_delta = m_info.sample_rate * MAX_SEEK_DELTA;
    u64 next = std::min<u64>(progress.played + (m_info.sample_count / 10), m_info.sample_count);
    next = std::min<u64>(next, progress.played + max_delta);

    audio::SeekSong(m_song, next);
}

void Menu::SeekBack() {
    audio::Progress progress{};
    audio::GetProgress(m_song, &progress, nullptr);

    const s64 max_delta = m_info.sample_rate * MAX_SEEK_DELTA;
    u64 next = std::max(s64(progress.played) - s64(m_info.sample_count / 10), s64(0));
    next = std::max<s64>(next, s64(progress.played) - max_delta);

    audio::SeekSong(m_song, next);
}

void Menu::IncreaseVolume() {
    float volume;
    if (R_SUCCEEDED(audio::GetVolumeSong(m_song, &volume))) {
        audio::SetVolumeSong(m_song, volume + VOLUME_DELTA);
        log_write("volume: %.2f\n", volume);
    }
}

void Menu::DecreaseVolume() {
    float volume;
    if (R_SUCCEEDED(audio::GetVolumeSong(m_song, &volume))) {
        audio::SetVolumeSong(m_song, volume - VOLUME_DELTA);
        log_write("volume: %.2f\n", volume);
    }
}

} // namespace sphaira::ui::music
