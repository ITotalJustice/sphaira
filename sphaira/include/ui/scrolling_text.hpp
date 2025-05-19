#pragma once

#include "ui/widget.hpp"
#include <string>

namespace sphaira::ui {

struct ScrollingText final {
public:
    void Draw(NVGcontext*, bool focus, float x, float y, float w, float size, int align, const NVGcolor& colour, const std::string& text_entry);
    void DrawArgs(NVGcontext*, bool focus, float x, float y, float w, float size, int align, const NVGcolor& colour, const char* s, ...) __attribute__ ((format (printf, 10, 11)));
    void Reset(const std::string& text_entry = "");

private:
    std::string m_str;
    s64 m_tick;
    float m_text_xoff;
};

} // namespace sphaira::ui
