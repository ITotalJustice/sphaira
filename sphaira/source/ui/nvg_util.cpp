#include "ui/nvg_util.hpp"
#include <cstddef>
#include <cstdio>
#include <cstdarg>
#include <array>
#include <utility>
#include <algorithm>
#include <cmath>

namespace sphaira::ui::gfx {
namespace {

static constexpr std::array buttons = {
    std::pair{Button::A, "\uE0E0"},
    std::pair{Button::B, "\uE0E1"},
    std::pair{Button::X, "\uE0E2"},
    std::pair{Button::Y, "\uE0E3"},
    std::pair{Button::L, "\uE0E4"},
    std::pair{Button::R, "\uE0E5"},
    std::pair{Button::L, "\uE0E6"},
    std::pair{Button::R, "\uE0E7"},
    std::pair{Button::L2, "\uE0E8"},
    std::pair{Button::R2, "\uE0E9"},
    std::pair{Button::UP, "\uE0EB"},
    std::pair{Button::DOWN, "\uE0EC"},
    std::pair{Button::LEFT, "\uE0ED"},
    std::pair{Button::RIGHT, "\uE0EE"},
    std::pair{Button::START, "\uE0EF"},
    std::pair{Button::SELECT, "\uE0F0"},
    // std::pair{Button::LS, "\uE101"},
    // std::pair{Button::RS, "\uE102"},
    std::pair{Button::L3, "\uE104"},
    std::pair{Button::R3, "\uE105"},
};

#define F(a) (a/255.f) // turn range 0-255 to 0.f-1.f range
constexpr std::array COLOURS = {
    std::pair<Colour, NVGcolor>{Colour::BLACK, { F(45.f), F(45.f), F(45.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::LIGHT_BLACK, { F(50.f), F(50.f), F(50.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::SILVER, { F(128.f), F(128.f), F(128.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::DARK_GREY, { F(70.f), F(70.f), F(70.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::GREY, { F(77.f), F(77.f), F(77.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::WHITE, { F(251.f), F(251.f), F(251.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::CYAN, { F(0.f), F(255.f), F(200.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::TEAL, { F(143.f), F(253.f), F(252.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::BLUE, { F(36.f), F(141.f), F(199.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::LIGHT_BLUE, { F(26.f), F(188.f), F(252.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::YELLOW, { F(255.f), F(177.f), F(66.f), F(255.f) }},
    std::pair<Colour, NVGcolor>{Colour::RED, { F(250.f), F(90.f), F(58.f), F(255.f) }}
};
#undef F

// NEW ---------------------
inline void drawRectIntenal(NVGcontext* vg, const Vec4& vec, const NVGcolor& c, bool rounded) {
    nvgBeginPath(vg);
    if (rounded) {
        nvgRoundedRect(vg, vec.x, vec.y, vec.w, vec.h, 15);
    } else {
        nvgRect(vg, vec.x, vec.y, vec.w, vec.h);
    }
    nvgFillColor(vg, c);
    nvgFill(vg);
}

inline void drawRectIntenal(NVGcontext* vg, const Vec4& vec, const NVGpaint& p, bool rounded) {
    nvgBeginPath(vg);
    if (rounded) {
        nvgRoundedRect(vg, vec.x, vec.y, vec.w, vec.h, 15);
    } else {
        nvgRect(vg, vec.x, vec.y, vec.w, vec.h);
    }
    nvgFillPaint(vg, p);
    nvgFill(vg);
}

inline void drawRectOutlineInternal(NVGcontext* vg, float size, const NVGcolor& out_col, Vec4 vec, const NVGcolor& c) {
    float gradientX, gradientY, color;
    getHighlightAnimation(&gradientX, &gradientY, &color);

    const auto strokeWidth = 5.0;
    auto v2 = vec;
    v2.x -= strokeWidth / 2.0;
    v2.y -= strokeWidth / 2.0;
    v2.w += strokeWidth;
    v2.h += strokeWidth;
    const auto corner_radius = 0.5;

    nvgSave(vg);
    nvgResetScissor(vg);

    // const auto stroke_width = 5.0f;
    // const auto shadow_corner_radius = 6.0f;
    const auto shadow_width = 2.0f;
    const auto shadow_offset = 10.0f;
    const auto shadow_feather = 10.0f;
    const auto shadow_opacity = 128.0f;

    // Shadow
    NVGpaint shadowPaint = nvgBoxGradient(vg,
        v2.x, v2.y + shadow_width,
        v2.w, v2.h,
        corner_radius * 2, shadow_feather,
        nvgRGBA(0, 0, 0, shadow_opacity * 1.f), nvgRGBA(0, 0, 0, 0));

    nvgBeginPath(vg);
    nvgRect(vg, v2.x - shadow_offset, v2.y - shadow_offset,
        v2.w + shadow_offset * 2, v2.h + shadow_offset * 3);
    nvgRoundedRect(vg, v2.x, v2.y, v2.w, v2.h, corner_radius);
    nvgPathWinding(vg, NVG_HOLE);
    nvgFillPaint(vg, shadowPaint);
    nvgFill(vg);

    const auto color1 = nvgRGB(25, 138, 198);
    const auto color2 = nvgRGB(137, 241, 242);
    const auto borderColor = nvgRGBAf(color2.r, color2.g, color2.b, 0.5);
    const auto transparent = nvgRGBA(0, 0, 0, 0);

    const auto pulsationColor = nvgRGBAf((color * color1.r) + (1 - color) * color2.r,
        (color * color1.g) + (1 - color) * color2.g,
        (color * color1.b) + (1 - color) * color2.b,
        1.f);

    const auto border1Paint = nvgRadialGradient(vg,
        v2.x + gradientX * v2.w, v2.y + gradientY * v2.h,
        strokeWidth * 10, strokeWidth * 40,
        borderColor, transparent);

    const auto border2Paint = nvgRadialGradient(vg,
        v2.x + (1 - gradientX) * v2.w, v2.y + (1 - gradientY) * v2.h,
        strokeWidth * 10, strokeWidth * 40,
        borderColor, transparent);

    nvgBeginPath(vg);
    nvgStrokeColor(vg, pulsationColor);
    nvgStrokeWidth(vg, strokeWidth);
    nvgRoundedRect(vg, v2.x, v2.y, v2.w, v2.h, corner_radius);
    nvgStroke(vg);

    nvgBeginPath(vg);
    nvgStrokePaint(vg, border1Paint);
    nvgStrokeWidth(vg, strokeWidth);
    nvgRoundedRect(vg, v2.x, v2.y, v2.w, v2.h, corner_radius);
    nvgStroke(vg);

    nvgBeginPath(vg);
    nvgStrokePaint(vg, border2Paint);
    nvgStrokeWidth(vg, strokeWidth);
    nvgRoundedRect(vg, v2.x, v2.y, v2.w, v2.h, corner_radius);
    nvgStroke(vg);

    drawRectIntenal(vg, {vec.x-size,vec.y-size,vec.w+(size*2.f),vec.h+(size * 2.f)}, pulsationColor, false);
    drawRectIntenal(vg, vec, c, true);
    nvgBeginPath(vg);
    nvgRoundedRect(vg, vec.x, vec.y, vec.w, vec.h, corner_radius);
    nvgFillColor(vg, c);
    nvgFill(vg);

    nvgRestore(vg);
}

inline void drawRectOutlineInternal(NVGcontext* vg, float size, const NVGcolor& out_col, Vec4 vec, const NVGpaint& p) {
    float gradientX, gradientY, color;
    getHighlightAnimation(&gradientX, &gradientY, &color);

    NVGcolor pulsationColor = nvgRGBAf((color * out_col.r) + (1 - color) * out_col.r,
            (color * out_col.g) + (1 - color) * out_col.g,
            (color * out_col.b) + (1 - color) * out_col.b,
            out_col.a);

    drawRectIntenal(vg, {vec.x-size,vec.y-size,vec.w+(size*2.f),vec.h+(size * 2.f)}, pulsationColor, false);
    drawRectIntenal(vg, vec, p, false);
}

inline void drawTriangleInternal(NVGcontext* vg, float aX, float aY, float bX, float bY, float cX, float cY, const NVGcolor& c) {
    nvgBeginPath(vg);
    nvgMoveTo(vg, aX, aY);
    nvgLineTo(vg, bX, bY);
    nvgLineTo(vg, cX, cY);
    nvgFillColor(vg, c);
    nvgFill(vg);
}

inline void drawTriangleInternal(NVGcontext* vg, float aX, float aY, float bX, float bY, float cX, float cY, const NVGpaint& p) {
    nvgBeginPath(vg);
    nvgMoveTo(vg, aX, aY);
    nvgLineTo(vg, bX, bY);
    nvgLineTo(vg, cX, cY);
    nvgFillPaint(vg, p);
    nvgFill(vg);
}

inline void drawTextIntenal(NVGcontext* vg, Vec2 vec, float size, const char* str, const char* end, int align, const NVGcolor& c) {
    nvgBeginPath(vg);
    nvgFontSize(vg, size);
    nvgTextAlign(vg, align);
    nvgFillColor(vg, c);
    nvgText(vg, vec.x, vec.y, str, end);
}

} // namespace

const char* getButton(const Button want) {
    for (auto& [key, val] : buttons) {
        if (key == want) {
            return val;
        }
    }
    std::unreachable();
}

NVGcolor getColour(Colour want) {
    for (auto& [key, val] : COLOURS) {
        if (key == want) {
            return val;
        }
    }
    std::unreachable();
}

void drawTextArgs(NVGcontext* vg, float x, float y, float size, int align, const NVGcolor& c, const char* str, ...) {
    std::va_list v;
    va_start(v, str);
    char buffer[0x100];
    std::vsnprintf(buffer, sizeof(buffer), str, v);
    va_end(v);
    drawText(vg, x, y, size, buffer, nullptr, align, c);
}

static void drawImageInternal(NVGcontext* vg, Vec4 v, int texture, int rounded = 0) {
    const auto paint = nvgImagePattern(vg, v.x, v.y, v.w, v.h, 0, texture, 1.f);
    // drawRect(vg, x, y, w, h, paint);
    nvgBeginPath(vg);
    // nvgRect(vg, x, y, w, h);
    if (rounded == 0) {
        nvgRect(vg, v.x, v.y, v.w, v.h);
    } else {
        nvgRoundedRect(vg, v.x, v.y, v.w, v.h, rounded);
    }
    nvgFillPaint(vg, paint);
    nvgFill(vg);
}

void drawImage(NVGcontext* vg, Vec4 v, int texture) {
    const auto paint = nvgImagePattern(vg, v.x, v.y, v.w, v.h, 0, texture, 1.f);
    drawRect(vg, v, paint, false);
}

void drawImage(NVGcontext* vg, float x, float y, float w, float h, int texture) {
    drawImage(vg, Vec4(x, y, w, h), texture);
}

void drawImageRounded(NVGcontext* vg, Vec4 v, int texture) {
    const auto paint = nvgImagePattern(vg, v.x, v.y, v.w, v.h, 0, texture, 1.f);
    nvgBeginPath(vg);
    nvgRoundedRect(vg, v.x, v.y, v.w, v.h, 15);
    nvgFillPaint(vg, paint);
    nvgFill(vg);
}

void drawImageRounded(NVGcontext* vg, float x, float y, float w, float h, int texture) {
    drawImageRounded(vg, Vec4(x, y, w, h), texture);
}

void drawTextBox(NVGcontext* vg, float x, float y, float size, float bound, NVGcolor& c, const char* str, int align, const char* end) {
    nvgBeginPath(vg);
    nvgFontSize(vg, size);
    nvgTextAlign(vg, align);
    nvgFillColor(vg, c);
    nvgTextBox(vg, x, y, bound, str, end);
}

void drawTextBox(NVGcontext* vg, float x, float y, float size, float bound, NVGcolor&& c, const char* str, int align, const char* end) {
    drawTextBox(vg, x, y, size, bound, c, str, align, end);
}

void drawTextBox(NVGcontext* vg, float x, float y, float size, float bound, Colour c, const char* str, int align, const char* end) {
    drawTextBox(vg, x, y, size, bound, getColour(c), str, align, end);
}

void textBounds(NVGcontext* vg, float x, float y, float *bounds, const char* str, ...) {
    char buf[0x100];
    va_list v;
    va_start(v, str);
    std::vsnprintf(buf, sizeof(buf), str, v);
    va_end(v);
    nvgTextBounds(vg, x, y, buf, nullptr, bounds);
}

// NEW-----------

void dimBackground(NVGcontext* vg) {
    // drawRectIntenal(vg, {0.f,0.f,1280.f,720.f}, nvgRGBA(30,30,30,180));
    // drawRectIntenal(vg, {0.f,0.f,1920.f,1080.f}, nvgRGBA(20, 20, 20, 225), false);
    drawRectIntenal(vg, {0.f,0.f,1920.f,1080.f}, nvgRGBA(0, 0, 0, 220), false);
}

void drawRect(NVGcontext* vg, float x, float y, float w, float h, Colour c, bool rounded) {
    drawRectIntenal(vg, {x,y,w,h}, getColour(c), rounded);
}

void drawRect(NVGcontext* vg, Vec4 vec, Colour c, bool rounded) {
    drawRectIntenal(vg, vec, getColour(c), rounded);
}

void drawRect(NVGcontext* vg, float x, float y, float w, float h, const NVGcolor& c, bool rounded) {
    drawRectIntenal(vg, {x,y,w,h}, c, rounded);
}

void drawRect(NVGcontext* vg, float x, float y, float w, float h, const NVGcolor&& c, bool rounded) {
    drawRectIntenal(vg, {x,y,w,h}, c, rounded);
}

void drawRect(NVGcontext* vg, Vec4 vec, const NVGcolor& c, bool rounded) {
    drawRectIntenal(vg, vec, c, rounded);
}

void drawRect(NVGcontext* vg, Vec4 vec, const NVGcolor&& c, bool rounded) {
    drawRectIntenal(vg, vec, c, rounded);
}

void drawRect(NVGcontext* vg, float x, float y, float w, float h, const NVGpaint& p, bool rounded) {
    drawRectIntenal(vg, {x,y,w,h}, p, rounded);
}

void drawRect(NVGcontext* vg, float x, float y, float w, float h, const NVGpaint&& p, bool rounded) {
    drawRectIntenal(vg, {x,y,w,h}, p, rounded);
}

void drawRect(NVGcontext* vg, Vec4 vec, const NVGpaint& p, bool rounded) {
    drawRectIntenal(vg, vec, p, rounded);
}

void drawRect(NVGcontext* vg, Vec4 vec, const NVGpaint&& p, bool rounded) {
    drawRectIntenal(vg, vec, p, rounded);
}


void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, float x, float y, float w, float h, Colour c) {
    drawRectOutlineInternal(vg, size, out_col, {x,y,w,h}, getColour(c));
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, Vec4 vec, Colour c) {
    drawRectOutlineInternal(vg, size, out_col, vec, getColour(c));
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, float x, float y, float w, float h, const NVGcolor& c) {
    drawRectOutlineInternal(vg, size, out_col, {x,y,w,h}, c);
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, float x, float y, float w, float h, const NVGcolor&& c) {
    drawRectOutlineInternal(vg, size, out_col, {x,y,w,h}, c);
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, Vec4 vec, const NVGcolor& c) {
    drawRectOutlineInternal(vg, size, out_col, vec, c);
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, Vec4 vec, const NVGcolor&& c) {
    drawRectOutlineInternal(vg, size, out_col, vec, c);
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, float x, float y, float w, float h, const NVGpaint& p) {
    drawRectOutlineInternal(vg, size, out_col, {x,y,w,h}, p);
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, float x, float y, float w, float h, const NVGpaint&& p) {
    drawRectOutlineInternal(vg, size, out_col, {x,y,w,h}, p);
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, Vec4 vec, const NVGpaint& p) {
    drawRectOutlineInternal(vg, size, out_col, vec, p);
}

void drawRectOutline(NVGcontext* vg, float size, const NVGcolor& out_col, Vec4 vec, const NVGpaint&& p) {
    drawRectOutlineInternal(vg, size, out_col, vec, p);
}


void drawTriangle(NVGcontext* vg, float aX, float aY, float bX, float bY, float cX, float cY, Colour c) {
    drawTriangleInternal(vg, aX, aY, bX, bY, cX, cY, getColour(c));
}

void drawTriangle(NVGcontext* vg, float aX, float aY, float bX, float bY, float cX, float cY, const NVGcolor& c) {
    drawTriangleInternal(vg, aX, aY, bX, bY, cX, cY, c);
}

void drawTriangle(NVGcontext* vg, float aX, float aY, float bX, float bY, float cX, float cY, const NVGcolor&& c) {
    drawTriangleInternal(vg, aX, aY, bX, bY, cX, cY, c);
}

void drawTriangle(NVGcontext* vg, float aX, float aY, float bX, float bY, float cX, float cY, const NVGpaint& p) {
    drawTriangleInternal(vg, aX, aY, bX, bY, cX, cY, p);
}

void drawTriangle(NVGcontext* vg, float aX, float aY, float bX, float bY, float cX, float cY, const NVGpaint&& p) {
    drawTriangleInternal(vg, aX, aY, bX, bY, cX, cY, p);
}

void drawText(NVGcontext* vg, float x, float y, float size, const char* str, const char* end, int align, Colour c) {
    drawTextIntenal(vg, {x,y}, size, str, end, align, getColour(c));
}

void drawText(NVGcontext* vg, float x, float y, float size, Colour c, const char* str, int align, const char* end) {
    drawTextIntenal(vg, {x,y}, size, str, end, align, getColour(c));
}

void drawText(NVGcontext* vg, Vec2 vec, float size, const char* str, const char* end, int align, Colour c) {
    drawTextIntenal(vg, vec, size, str, end, align, getColour(c));
}

void drawText(NVGcontext* vg, Vec2 vec, float size, Colour c, const char* str, int align, const char* end) {
    drawTextIntenal(vg, vec, size, str, end, align, getColour(c));
}

void drawText(NVGcontext* vg, float x, float y, float size, const char* str, const char* end, int align, const NVGcolor& c) {
    drawTextIntenal(vg, {x,y}, size, str, end, align, c);
}

void drawText(NVGcontext* vg, float x, float y, float size, const char* str, const char* end, int align, const NVGcolor&& c) {
    drawTextIntenal(vg, {x,y}, size, str, end, align, c);
}

void drawText(NVGcontext* vg, float x, float y, float size, const NVGcolor& c, const char* str, int align, const char* end) {
    drawTextIntenal(vg, {x,y}, size, str, end, align, c);
}

void drawText(NVGcontext* vg, float x, float y, float size, const NVGcolor&& c, const char* str, int align, const char* end) {
    drawTextIntenal(vg, {x,y}, size, str, end, align, c);
}

void drawText(NVGcontext* vg, Vec2 vec, float size, const char* str, const char* end, int align, const NVGcolor& c) {
    drawTextIntenal(vg, vec, size, str, end, align, c);
}

void drawText(NVGcontext* vg, Vec2 vec, float size, const char* str, const char* end, int align, const NVGcolor&& c) {
    drawTextIntenal(vg, vec, size, str, end, align, c);
}

void drawText(NVGcontext* vg, Vec2 vec, float size, const NVGcolor& c, const char* str, int align, const char* end) {
    drawTextIntenal(vg, vec, size, str, end, align, c);
}

void drawText(NVGcontext* vg, Vec2 vec, float size, const NVGcolor&& c, const char* str, int align, const char* end) {
    drawTextIntenal(vg, vec, size, str, end, align, c);
}

void drawTextArgs(NVGcontext* vg, float x, float y, float size, int align, Colour c, const char* str, ...) {
    std::va_list v;
    va_start(v, str);
    char buffer[0x100];
    std::vsnprintf(buffer, sizeof(buffer), str, v);
    va_end(v);
    drawTextIntenal(vg, {x, y}, size, buffer, nullptr, align, getColour(c));
}

void drawButton(NVGcontext* vg, float x, float y, float size, Button button) {
    drawText(vg, x, y, size, getButton(button), nullptr, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, getColour(Colour::WHITE));
}

void drawButtons(NVGcontext* vg, const Widget::Actions& _actions, const NVGcolor& c, float start_x) {
    nvgBeginPath(vg);
    nvgFontSize(vg, 24.f);
    nvgTextAlign(vg, NVG_ALIGN_RIGHT | NVG_ALIGN_TOP);
    nvgFillColor(vg, c);

    float x = start_x;
    const float y = 675.f;
    float bounds[4]{};

    // swaps L/R position, idc how shit this is, it's called once per frame.
    std::vector<std::pair<Button, Action>> actions;
    actions.reserve(_actions.size());

    for (const auto a: _actions) {
        // swap
        if (a.first == Button::R && actions.size() && actions.back().first == Button::L) {
            const auto s = actions.back();
            actions.back() = a;
            actions.emplace_back(s);

        } else {
            actions.emplace_back(a);
        }
    }

    for (const auto& [button, action] : actions) {
        if (action.IsHidden() || action.m_hint.empty()) {
            continue;
        }
        nvgFontSize(vg, 20.f);
        nvgTextBounds(vg, x, y, action.m_hint.c_str(), nullptr, bounds);
        auto len = bounds[2] - bounds[0];
        nvgText(vg, x, y, action.m_hint.c_str(), nullptr);

        x -= len + 8.f;
        nvgFontSize(vg, 26.f);
        nvgTextBounds(vg, x, y - 7.f, getButton(button), nullptr, bounds);
        len = bounds[2] - bounds[0];
        nvgText(vg, x, y - 4.f, getButton(button), nullptr);
        x -= len + 34.f;
    }
}

// from gc installer
void drawDimBackground(NVGcontext* vg) {
    // drawRect(vg, 0, 0, 1920, 1080, nvgRGBA(20, 20, 20, 225));
    drawRect(vg, 0, 0, 1920, 1080, nvgRGBA(0, 0, 0, 220));
}

#define HIGHLIGHT_SPEED 350.0

static double highlightGradientX = 0;
static double highlightGradientY = 0;
static double highlightColor     = 0;

void updateHighlightAnimation() {
    const auto currentTime = svcGetSystemTick() * 10 / 192 / 1000;

    // Update variables
    highlightGradientX = (std::cos((double)currentTime / HIGHLIGHT_SPEED / 3.0) + 1.0) / 2.0;
    highlightGradientY = (std::sin((double)currentTime / HIGHLIGHT_SPEED / 3.0) + 1.0) / 2.0;
    highlightColor     = (std::sin((double)currentTime / HIGHLIGHT_SPEED * 2.0) + 1.0) / 2.0;
}

void getHighlightAnimation(float* gradientX, float* gradientY, float* color) {
    if (gradientX)
        *gradientX = (float)highlightGradientX;

    if (gradientY)
        *gradientY = (float)highlightGradientY;

    if (color)
        *color = (float)highlightColor;
}

} // namespace sphaira::ui::gfx
