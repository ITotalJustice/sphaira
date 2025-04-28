#pragma once

#include "ui/object.hpp"

namespace sphaira::ui {

struct List final : Object {
    using Callback = std::function<void(NVGcontext* vg, Theme* theme, Vec4 v, s64 index)>;
    using TouchCallback = std::function<void(bool touch, s64 index)>;

    List(s64 row, s64 page, const Vec4& pos, const Vec4& v, const Vec2& pad = {});

    void OnUpdate(Controller* controller, TouchInfo* touch, s64 index, s64 count, TouchCallback callback);

    void Draw(NVGcontext* vg, Theme* theme, s64 count, Callback callback) const;

    auto SetScrollBarPos(float x, float y, float h) {
        m_scrollbar.x = x;
        m_scrollbar.y = y;
        m_scrollbar.h = h;
    }

    auto ScrollDown(s64& index, s64 step, s64 count) -> bool;
    auto ScrollUp(s64& index, s64 step, s64 count) -> bool;

    auto GetYoff() const {
        return m_yoff;
    }

    void SetYoff(float y = 0) {
        m_yoff = y;
    }

    auto GetMaxY() const {
        return m_v.h + m_pad.y;
    }

private:
    auto Draw(NVGcontext* vg, Theme* theme) -> void override {}
    auto ClampY(float y, s64 count) const -> float;

private:
    const s64 m_row;
    const s64 m_page;

    Vec4 m_v{};
    Vec2 m_pad{};

    Vec4 m_scrollbar{};

    // current y offset.
    float m_yoff{};
    // in progress y offset, used when scrolling.
    float m_y_prog{};
};

} // namespace sphaira::ui
