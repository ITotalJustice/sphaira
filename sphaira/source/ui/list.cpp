#include "ui/list.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"
#include "log.hpp"
#include <algorithm>

namespace sphaira::ui {

List::List(s64 row, s64 page, const Vec4& pos, const Vec4& v, const Vec2& pad)
: m_row{row}
, m_page{page}
, m_v{v}
, m_pad{pad} {
    m_pos = pos;
    // SetScrollBarPos(SCREEN_WIDTH - 50, 100, SCREEN_HEIGHT-200);
    SetScrollBarPos(pos.x + pos.w, 100, SCREEN_HEIGHT-200);
}

auto List::ClampX(float x, s64 count) const -> float {
    const float x_max = count * GetMaxX();
    return std::clamp(x, 0.F, x_max);
}

auto List::ClampY(float y, s64 count) const -> float {
    float y_max = 0;

    if (count >= m_page) {
        // round up
        if (count % m_row) {
            count = count + (m_row - count % m_row);
        }
        y_max = (count - m_page) / m_row * GetMaxY();
    }

    return std::clamp(y, 0.F, y_max);
}

void List::OnUpdate(Controller* controller, TouchInfo* touch, s64 index, s64 count, const TouchCallback& callback) {
    switch (m_layout) {
        case Layout::HOME:
            OnUpdateHome(controller, touch, index, count, callback);
            break;
        case Layout::GRID:
            OnUpdateGrid(controller, touch, index, count, callback);
            break;
    }
}

void List::Draw(NVGcontext* vg, Theme* theme, s64 count, const Callback& callback) const {
    switch (m_layout) {
        case Layout::HOME:
            DrawHome(vg, theme, count, callback);
            break;
        case Layout::GRID:
            DrawGrid(vg, theme, count, callback);
            break;
    }
}

auto List::ScrollDown(s64& index, s64 step, s64 count) -> bool {
    const auto old_index = index;
    const auto max = m_layout == Layout::GRID ? GetMaxY() : GetMaxX();

    if (!count) {
        return false;
    }

    index = std::min(index + step, count - 1);

    if (index != old_index) {
        App::PlaySoundEffect(SoundEffect::Scroll);
        s64 delta = index - old_index;
        s64 start = m_yoff / max * m_row;

        while (index < start) {
            start -= m_row;
            m_yoff -= max;
        }

        if (index - start >= m_page) {
            do {
                start += m_row;
                delta -= m_row;
                m_yoff += max;
            } while (delta > 0 && start + m_page < count);
        }

        return true;
    }

    return false;
}

auto List::ScrollUp(s64& index, s64 step, s64 count) -> bool {
    const auto old_index = index;
    const auto max = m_layout == Layout::GRID ? GetMaxY() : GetMaxX();

    if (!count) {
        return false;
    }

    index = std::max<s64>(0, index - step);

    if (index != old_index) {
        App::PlaySoundEffect(SoundEffect::Scroll);
        s64 start = m_yoff / max * m_row;

        while (index < start) {
            start -= m_row;
            m_yoff -= max;
        }

        while (index - start >= m_page && start + m_page < count) {
            start += m_row;
            m_yoff += max;
        }

        return true;
    }

    return false;
}

void List::OnUpdateHome(Controller* controller, TouchInfo* touch, s64 index, s64 count, const TouchCallback& callback) {
    if (controller->GotDown(Button::RIGHT)) {
        if (ScrollDown(index, m_row, count)) {
            callback(false, index);
        }
    } else if (controller->GotDown(Button::LEFT)) {
        if (ScrollUp(index, m_row, count)) {
            callback(false, index);
        }
    } else if (touch->is_clicked && touch->in_range(GetPos())) {
        auto v = m_v;
        v.x -= ClampX(m_yoff + m_y_prog, count);

        for (s64 i = 0; i < count; i++, v.x += v.w + m_pad.x) {
            if (v.x > GetX() + GetW()) {
                break;
            }

            Vec4 vv = v;
            // if not drawing, only return clipped v as its used for touch
            vv.w = std::min(v.x + v.w, m_pos.x + m_pos.w) - v.x;
            vv.h = std::min(v.y + v.h, m_pos.y + m_pos.h) - v.y;

            if (touch->in_range(vv)) {
                callback(true, i);
                return;
            }
        }
    } else if (touch->is_scroll && touch->in_range(GetPos())) {
        m_y_prog = (float)touch->initial.x - (float)touch->cur.x;
    } else if (touch->is_end) {
        m_yoff = ClampX(m_yoff + m_y_prog, count);
        m_y_prog = 0;
    }
}

void List::OnUpdateGrid(Controller* controller, TouchInfo* touch, s64 index, s64 count, const TouchCallback& callback) {
    const auto page_up_button = GetPageJump() ? (m_row == 1 ? Button::DPAD_LEFT : Button::L2) : (Button::NONE);
    const auto page_down_button = GetPageJump() ? (m_row == 1 ? Button::DPAD_RIGHT : Button::R2) : (Button::NONE);

    const auto hotkey = m_row == 1 ? controller->GotHeld(Button::R2) : false;
    const auto end_page = INT32_MAX;
    const auto hot_page = m_page * 4;

    if (controller->GotDown(Button::DOWN)) {
        if (ScrollDown(index, hotkey ? end_page : m_row, count)) {
            callback(false, index);
        }
    } else if (controller->GotDown(Button::UP)) {
        if (ScrollUp(index, hotkey ? end_page : m_row, count)) {
            callback(false, index);
        }
    } else if (controller->GotDown(page_down_button)) {
        if (ScrollDown(index, hotkey ? hot_page : m_page, count)) {
            callback(false, index);
        }
    } else if (controller->GotDown(page_up_button)) {
        if (ScrollUp(index, hotkey ? hot_page : m_page, count)) {
            callback(false, index);
        }
    } else if (m_row > 1 && controller->GotDown(Button::RIGHT)) {
        if (count && index < (count - 1) && (index + 1) % m_row != 0) {
            callback(false, index + 1);
        }
    } else if (m_row > 1 && controller->GotDown(Button::LEFT)) {
        if (count && index != 0 && (index % m_row) != 0) {
            callback(false, index - 1);
        }
    } else if (touch->is_clicked && touch->in_range(GetPos())) {
        auto v = m_v;
        v.y -= ClampY(m_yoff + m_y_prog, count);

        for (s64 i = 0; i < count; v.y += v.h + m_pad.y) {
            if (v.y > GetY() + GetH()) {
                break;
            }

            const auto x = v.x;

            for (; i < count; i++, v.x += v.w + m_pad.x) {
                // only draw if full x is in bounds
                if (v.x + v.w > GetX() + GetW()) {
                    break;
                }

                // skip anything not visible
                if (v.y + v.h < GetY()) {
                    continue;
                }

                Vec4 vv = v;
                // if not drawing, only return clipped v as its used for touch
                vv.w = std::min(v.x + v.w, m_pos.x + m_pos.w) - v.x;
                vv.h = std::min(v.y + v.h, m_pos.y + m_pos.h) - v.y;

                if (touch->in_range(vv)) {
                    callback(true, i);
                    return;
                }
            }

            v.x = x;
        }
    } else if (touch->is_scroll && touch->in_range(GetPos())) {
        m_y_prog = (float)touch->initial.y - (float)touch->cur.y;
    } else if (touch->is_end) {
        m_yoff = ClampY(m_yoff + m_y_prog, count);
        m_y_prog = 0;
    }
}

void List::DrawHome(NVGcontext* vg, Theme* theme, s64 count, const Callback& callback) const {
    const auto yoff = ClampX(m_yoff + m_y_prog, count);
    auto v = m_v;
    v.x -= yoff;

    nvgSave(vg);
    nvgIntersectScissor(vg, GetX(), GetY(), GetW(), GetH());

    for (s64 i = 0; i < count; i++, v.x += v.w + m_pad.x) {
        // skip anything not visible
        if (v.x + v.w < GetX()) {
            continue;
        }

        if (v.x > GetX() + GetW()) {
            break;
        }

        callback(vg, theme, v, i);
    }

    nvgRestore(vg);
}

void List::DrawGrid(NVGcontext* vg, Theme* theme, s64 count, const Callback& callback) const {
    const auto yoff = ClampY(m_yoff + m_y_prog, count);
    const s64 start = yoff / GetMaxY() * m_row;
    gfx::drawScrollbar2(vg, theme, m_scrollbar.x, m_scrollbar.y, m_scrollbar.h, start, count, m_row, m_page);

    auto v = m_v;
    v.y -= yoff;

    nvgSave(vg);
    nvgIntersectScissor(vg, GetX(), GetY(), GetW(), GetH());

    for (s64 i = 0; i < count; v.y += v.h + m_pad.y) {
        if (v.y > GetY() + GetH()) {
            break;
        }

        const auto x = v.x;

        for (s64 row = 0; i < count; row++, i++, v.x += v.w + m_pad.x) {
            if (row >= m_row) {
                break;
            }

            // only draw if full x is in bounds
            if (v.x + v.w > GetX() + GetW()) {
                break;
            }

            // skip anything not visible
            if (v.y + v.h < GetY()) {
                continue;
            }

            callback(vg, theme, v, i);
        }

        v.x = x;
    }

    nvgRestore(vg);
}

} // namespace sphaira::ui
