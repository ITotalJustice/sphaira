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

void DrawClipped(NVGcontext* vg, const Vec4& clip, float x, float y, float size, int align, const NVGcolor& colour, const std::string& str) {
        nvgSave(vg);
    nvgIntersectScissor(vg, clip.x, clip.y, clip.w, clip.h); // clip
        gfx::drawText(vg, x, y, size, colour, str.c_str(), align);
    nvgRestore(vg);
}

} // namespace

void ScrollingText::Draw(NVGcontext* vg, bool focus, float x, float y, float w, float size, int align, const NVGcolor& colour, const std::string& text_entry) {
    const Vec4 clip{x, 0, w, 720};

    if (!focus) {
        DrawClipped(vg, clip, x, y, size, align, colour, text_entry);
        return;
    }

    if (m_str != text_entry) {
        Reset(text_entry);
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

    x -= m_text_xoff;
    DrawClipped(vg, clip, x, y, size, align, colour, value_str);
}

void ScrollingText::DrawArgs(NVGcontext* vg, bool focus, float x, float y, float w, float size, int align, const NVGcolor& colour, const char* s, ...) {
    std::va_list v{};
    va_start(v, s);
    char buffer[0x100];
    std::vsnprintf(buffer, sizeof(buffer), s, v);
    va_end(v);
    Draw(vg, focus, x, y, w, size, align, colour, buffer);
}

void ScrollingText::Reset(const std::string& text_entry) {
    m_str = text_entry;
    m_tick = 0;
    m_text_xoff = 0;
}

} // namespace sphaira::ui
