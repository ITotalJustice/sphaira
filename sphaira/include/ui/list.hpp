#pragma once

#include "ui/object.hpp"

namespace sphaira::ui {

struct List final : Object {
    enum class Layout {
        HOME,
        GRID,
    };

    using Callback = std::function<void(NVGcontext* vg, Theme* theme, const Vec4& v, s64 index)>;
    using TouchCallback = std::function<void(bool touch, s64 index)>;

    List(s64 row, s64 page, const Vec4& pos, const Vec4& v, const Vec2& pad = {});

    void OnUpdate(Controller* controller, TouchInfo* touch, s64 index, s64 count, const TouchCallback& callback);

    void Draw(NVGcontext* vg, Theme* theme, s64 count, const Callback& callback) const;

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

    auto GetMaxX() const {
        return m_v.w + m_pad.x;
    }

    auto GetLayout() const {
        return m_layout;
    }

    void SetLayout(Layout layout) {
        m_layout = layout;
    }

    auto GetRow() const {
        return m_row;
    }

    auto GetPage() const {
        return m_page;
    }

    auto SetPageJump(bool enable) {
        m_page_jump = enable;
    }

    auto GetPageJump() const {
        return m_page_jump;
    }

private:
    auto Draw(NVGcontext* vg, Theme* theme) -> void override {}
    auto ClampX(float x, s64 count) const -> float;
    auto ClampY(float y, s64 count) const -> float;

    void OnUpdateHome(Controller* controller, TouchInfo* touch, s64 index, s64 count, const TouchCallback& callback);
    void OnUpdateGrid(Controller* controller, TouchInfo* touch, s64 index, s64 count, const TouchCallback& callback);
    void DrawHome(NVGcontext* vg, Theme* theme, s64 count, const Callback& callback) const;
    void DrawGrid(NVGcontext* vg, Theme* theme, s64 count, const Callback& callback) const;

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

    Layout m_layout{Layout::GRID};
    bool m_page_jump{true};
};

} // namespace sphaira::ui
