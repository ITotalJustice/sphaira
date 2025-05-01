#include "ui/scrolling_text.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"
#include <cstdarg>

namespace sphaira::ui {
namespace {

auto GetTextScrollSpeed() -> float {
    switch (App::GetTextScrollSpeed()) {
        case 0: return 0.5;
        default: case 1: return 1.0;
        case 2: return 1.5;
    }
}

} // namespace

void ScrollingText::Draw(NVGcontext* vg, bool focus, float x, float y, float w, float size, int align, const NVGcolor& colour, const std::string& text_entry) {
    if (!focus) {
        gfx::drawText(vg, x, y, size, colour, text_entry.c_str(), align);
        return;
    }

    if (m_str != text_entry) {
        m_str = text_entry;
        m_tick = 0;
        m_text_xoff = 0;
    }

    float bounds[4];
    auto value_str = text_entry;
    nvgFontSize(vg, size);
    nvgTextAlign(vg, align);
    nvgTextBounds(vg, 0, 0, value_str.c_str(), nullptr, bounds);

    if (focus) {
        const auto scroll_amount = GetTextScrollSpeed();
        if (bounds[2] > w) {
            value_str += "        ";
            nvgTextBounds(vg, 0, 0, value_str.c_str(), nullptr, bounds);

            if (!m_text_xoff) {
                m_tick++;
                if (m_tick >= 60) {
                    m_tick = 0;
                    m_text_xoff += scroll_amount;
                }
            } else if (bounds[2] > m_text_xoff) {
                m_text_xoff += std::min(scroll_amount, bounds[2] - m_text_xoff);
            } else {
                m_text_xoff = 0;
            }

            value_str += text_entry;
        }
    }

    const Vec2 pos{x - m_text_xoff, y};
    gfx::drawText(vg, pos, size, colour, value_str.c_str(), align);
}

void ScrollingText::DrawArgs(NVGcontext* vg, bool focus, float x, float y, float w, float size, int align, const NVGcolor& colour, const char* s, ...) {
    std::va_list v{};
    va_start(v, s);
    char buffer[0x100];
    std::vsnprintf(buffer, sizeof(buffer), s, v);
    va_end(v);
    Draw(vg, focus, x, y, w, size, align, colour, buffer);
}

} // namespace sphaira::ui
