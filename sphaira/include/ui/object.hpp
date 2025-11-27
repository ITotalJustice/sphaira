#pragma once

#include "types.hpp"
#include <stop_token>

namespace sphaira::ui {

class Object {
public:
    Object() = default;
    virtual ~Object() {
        m_stop_source.request_stop();
    }

    virtual auto Draw(NVGcontext* vg, Theme* theme) -> void = 0;

    auto GetPos() const noexcept {
        return m_pos;
    }

    auto GetX() const noexcept {
        return m_pos.x;
    }

    auto GetY() const noexcept {
        return m_pos.y;
    }

    auto GetW() const noexcept {
        return m_pos.w;
    }

    auto GetH() const noexcept {
        return m_pos.h;
    }

    auto SetX(float a) noexcept {
        return m_pos.x = a;
    }

    auto SetY(float a) noexcept {
        return m_pos.y = a;
    }

    auto SetW(float a) noexcept {
        return m_pos.w = a;
    }

    auto SetH(float a) noexcept {
        return m_pos.h = a;
    }

    auto SetPos(float x, float y, float w, float h) noexcept -> void {
        m_pos = { x, y, w, h };
    }

    auto SetPos(const Vec4& v) noexcept -> void {
        m_pos = v;
    }

    auto InXBounds(float x) const -> bool {
        return x >= m_pos.x && x <= m_pos.x + m_pos.w;
    }

    auto InYBounds(float y) const -> bool {
        return y >= m_pos.y && y <= m_pos.y + m_pos.h;
    }

    auto IsHidden() const noexcept -> bool {
        return m_hidden;
    }

    auto SetHidden(bool value = true) noexcept -> void {
        m_hidden = value;
    }

    auto GetToken() const {
        return m_stop_source.get_token();
    }

protected:
    Vec4 m_pos{};
    // used for lifetime management across threads.
    std::stop_source m_stop_source{};
    bool m_hidden{false};
};

} // namespace sphaira::ui
