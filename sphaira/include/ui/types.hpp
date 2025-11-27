#pragma once

#include "nanovg.h"
#include "fs.hpp"

#include <switch.h>
#include <string>
#include <functional>
#include <variant>

namespace sphaira {

#define SCREEN_WIDTH 1280.f
#define SCREEN_HEIGHT 720.f

struct Vec2 {
    constexpr Vec2() = default;
    constexpr Vec2(float _x, float _y) : x{_x}, y{_y} {}

    float& operator[](std::size_t idx) {
        switch (idx) {
            case 0: return x;
            case 1: return y;
        }
        __builtin_unreachable();
        // throw;
    }

    constexpr const float& operator[](std::size_t idx) const {
        switch (idx) {
            case 0: return x;
            case 1: return y;
        }
        __builtin_unreachable();
        // throw;
    }

    constexpr Vec2 operator+(const Vec2& v) const noexcept {
        return {x + v.x, y + v.y};
    }

    constexpr Vec2& operator+=(const Vec2& v) noexcept {
        x += v.x;
        y += v.y;
        return *this;
    }

    constexpr bool operator==(const Vec2& v) const noexcept {
        return x == v.x && y == v.y;
    }

    float x{}, y{};
};

struct Vec4 {
    constexpr Vec4() = default;
    constexpr Vec4(float _x, float _y, float _w, float _h) : x{_x}, y{_y}, w{_w}, h{_h} {}
    constexpr Vec4(const Vec2& vec0, const Vec2& vec1) : x{vec0.x}, y{vec0.y}, w{vec1.x}, h{vec1.y} {}
    constexpr Vec4(const Vec4& vec0, const Vec4& vec1) : x{vec0.x}, y{vec0.y}, w{vec1.w}, h{vec1.h} {}

    float& operator[](std::size_t idx) {
        switch (idx) {
            case 0: return x;
            case 1: return y;
            case 2: return w;
            case 3: return h;
        }
        __builtin_unreachable();
        // throw;
    }

    constexpr const float& operator[](std::size_t idx) const {
        switch (idx) {
            case 0: return x;
            case 1: return y;
            case 2: return w;
            case 3: return h;
        }
        __builtin_unreachable();
        // throw;
    }

    constexpr Vec2 operator+(const Vec2& v) const noexcept {
        return {x + v.x, y + v.y};
    }

    constexpr Vec4 operator+(const Vec4& v) const noexcept {
        return {x + v.x, y + v.y, w + v.w, h + v.h};
    }

    constexpr Vec4& operator+=(const Vec2& v) noexcept {
        x += v.x;
        y += v.y;
        return *this;
    }

    constexpr Vec4& operator+=(const Vec4& v) noexcept {
        x += v.x;
        y += v.y;
        return *this;
    }

    constexpr bool operator==(const Vec2& v) const noexcept {
        return x == v.x && y == v.y;
    }

    constexpr bool operator==(const Vec4& v) const noexcept {
        return x == v.x && y == v.y && w == v.w && h == v.h;
    }

    float x{}, y{}, w{}, h{};
};

struct TimeStamp {
    TimeStamp() {
        Update();
    }

    void Update() {
        start = armGetSystemTick();
    }

    auto GetNs() const -> u64 {
        const auto end_ticks = armGetSystemTick();
        return armTicksToNs(end_ticks) - armTicksToNs(start);
    }

    auto GetMs() const -> u64 {
        const auto ns = GetNs();
        return ns/1000/1000;
    }

    auto GetSeconds() const -> u64 {
        const auto ns = GetNs();
        return ns/1000/1000/1000;
    }

    auto GetMsD() const -> double {
        const double ns = GetNs();
        return ns/1000.0/1000.0;
    }

    auto GetSecondsD() const -> double {
        const double ns = GetNs();
        return ns/1000.0/1000.0/1000.0;
    }

    u64 start;
};

enum class ElementType {
    None,
    Texture,
    Colour,
};

struct ElementEntry {
    ElementType type;
    int texture;
    NVGcolor colour;
};

enum ThemeEntryID {
    // colour of the background, can be an image.
    ThemeEntryID_BACKGROUND,
    // colour of the grid background (homebrew, appstore), can be an image.
    ThemeEntryID_GRID,
    // background colour of a popup.
    ThemeEntryID_POPUP,
    // colour of the error text / button.
    ThemeEntryID_ERROR,

    // colour of all text.
    ThemeEntryID_TEXT,
    // colour of text info and subheaders.
    ThemeEntryID_TEXT_INFO,
    // colour of selected item text.
    ThemeEntryID_TEXT_SELECTED,
    // background colour of a selected item, can be an image (not recommended).
    ThemeEntryID_SELECTED_BACKGROUND,
    // colour of the split screen and selected item.
    ThemeEntryID_FOCUS,

    // colour of line separators in a list.
    ThemeEntryID_LINE,
    ThemeEntryID_LINE_SEPARATOR,

    // colour of the sidebar backrgound.
    ThemeEntryID_SIDEBAR,

    // colour of the scrollbar (full portion).
    ThemeEntryID_SCROLLBAR,
    // colour of the scrollbar background (empty portion).
    ThemeEntryID_SCROLLBAR_BACKGROUND,

    // colour of the progressbar (full portion).
    ThemeEntryID_PROGRESSBAR,
    // colour of the progressbar background (empty portion).
    ThemeEntryID_PROGRESSBAR_BACKGROUND,

    // the colours of the pulsing effect, from 1 -> 2.
    ThemeEntryID_HIGHLIGHT_1,
    ThemeEntryID_HIGHLIGHT_2,

    // changes the colours of the internal icons used below.
    ThemeEntryID_ICON_COLOUR,

    // images used in the filebrowser.
    ThemeEntryID_ICON_AUDIO,
    ThemeEntryID_ICON_VIDEO,
    ThemeEntryID_ICON_IMAGE,
    ThemeEntryID_ICON_FILE,
    ThemeEntryID_ICON_FOLDER,
    ThemeEntryID_ICON_ZIP,
    ThemeEntryID_ICON_NRO,

    ThemeEntryID_MAX,
};

struct ThemeMeta {
    std::string name;
    std::string author;
    std::string version;
    fs::FsPath inherit;
    fs::FsPath ini_path;
};

struct Theme {
    ThemeMeta meta;
    ElementEntry elements[ThemeEntryID_MAX];
    fs::FsPath music_path;

    auto GetColour(ThemeEntryID id) const {
        return elements[id].colour;
    }
};

// enum class TouchGesture {
//     None,
//     Tap,
//     Scroll,
// };

struct TouchInfo {
    HidTouchState initial;
    HidTouchState cur;

    auto in_range(const Vec4& v) const -> bool {
        return cur.x >= v.x && cur.x <= v.x + v.w && cur.y >= v.y && cur.y <= v.y + v.h;
    }

    auto in_range(s32 x, s32 y, s32 w, s32 h) const -> bool {
        return in_range(Vec4(x, y, w, h));
    }

    bool is_touching;
    bool is_tap;
    bool is_scroll;
    bool is_clicked;
    bool is_end;
};

enum class Button : u64 {
    A = static_cast<u64>(HidNpadButton_A),
    B = static_cast<u64>(HidNpadButton_B),
    X = static_cast<u64>(HidNpadButton_X),
    Y = static_cast<u64>(HidNpadButton_Y),
    L = static_cast<u64>(HidNpadButton_L),
    R = static_cast<u64>(HidNpadButton_R),
    L2 = static_cast<u64>(HidNpadButton_ZL),
    R2 = static_cast<u64>(HidNpadButton_ZR),
    L3 = static_cast<u64>(HidNpadButton_StickL),
    R3 = static_cast<u64>(HidNpadButton_StickR),
    START = static_cast<u64>(HidNpadButton_Plus),
    SELECT = static_cast<u64>(HidNpadButton_Minus),

    SL_LEFT = static_cast<u64>(HidNpadButton_LeftSL),
    SR_LEFT = static_cast<u64>(HidNpadButton_LeftSR),
    SL_RIGHT = static_cast<u64>(HidNpadButton_RightSL),
    SR_RIGHT = static_cast<u64>(HidNpadButton_RightSR),
    SL_ANY = SL_LEFT | SL_RIGHT,
    SR_ANY = SR_LEFT | SR_RIGHT,

    // todo:
    DPAD_LEFT = static_cast<u64>(HidNpadButton_Left),
    DPAD_RIGHT = static_cast<u64>(HidNpadButton_Right),
    DPAD_UP = static_cast<u64>(HidNpadButton_Up),
    DPAD_DOWN = static_cast<u64>(HidNpadButton_Down),

    LS_LEFT = static_cast<u64>(HidNpadButton_StickLLeft),
    LS_RIGHT = static_cast<u64>(HidNpadButton_StickLRight),
    LS_UP = static_cast<u64>(HidNpadButton_StickLUp),
    LS_DOWN = static_cast<u64>(HidNpadButton_StickLDown),
    LS_ANY = LS_LEFT | LS_RIGHT | LS_UP | LS_DOWN,

    RS_LEFT = static_cast<u64>(HidNpadButton_StickRLeft),
    RS_RIGHT = static_cast<u64>(HidNpadButton_StickRRight),
    RS_UP = static_cast<u64>(HidNpadButton_StickRUp),
    RS_DOWN = static_cast<u64>(HidNpadButton_StickRDown),
    RS_ANY = RS_LEFT | RS_RIGHT | RS_UP | RS_DOWN,

    ANY_LEFT = static_cast<u64>(HidNpadButton_AnyLeft),
    ANY_RIGHT = static_cast<u64>(HidNpadButton_AnyRight),
    ANY_UP = static_cast<u64>(HidNpadButton_AnyUp),
    ANY_DOWN = static_cast<u64>(HidNpadButton_AnyDown),

    // todo: remove these old buttons
    LEFT = static_cast<u64>(HidNpadButton_AnyLeft),
    RIGHT = static_cast<u64>(HidNpadButton_AnyRight),
    UP = static_cast<u64>(HidNpadButton_AnyUp),
    DOWN = static_cast<u64>(HidNpadButton_AnyDown),

    NONE = 0,
    ANY_BUTTON = A | B | X | Y | L | R | L2 | R2 | L3 | R3 | START | SELECT,
    ANY_HORIZONTAL = LEFT | RIGHT,
    ANY_VERTICAL = UP | DOWN,
    ANY_DIRECTION = ANY_HORIZONTAL | ANY_VERTICAL,
    ANY = ANY_BUTTON | ANY_DIRECTION
};

inline Button operator|(Button a, Button b) {
    return static_cast<Button>(static_cast<u64>(a) | static_cast<u64>(b));
}

// when the callback we be called, can be xord
enum ActionType : u8 {
    DOWN = 1 << 0,
    UP = 1 << 1,
    HELD = 1 << 2,
};

inline ActionType operator|(ActionType a, ActionType b) {
    return static_cast<ActionType>(static_cast<u64>(a) | static_cast<u64>(b));
}

struct Action final {
    using CallbackEmpty = std::function<void()>;
    using CallbackWithBool = std::function<void(bool)>;
    using Callback = std::variant<
        CallbackEmpty,
        CallbackWithBool
    >;

    explicit Action(const Callback& cb) : Action{ActionType::DOWN, "", cb} {}
    explicit Action(const std::string& hint, const Callback& cb) : Action{ActionType::DOWN, hint, cb} {}
    explicit Action(u8 type, const Callback& cb) : Action{type, "", cb} {}
    explicit Action(u8 type, const std::string& hint, const Callback& cb) : m_type{type}, m_callback{cb}, m_hint{hint} {}

    auto IsHidden() const noexcept { return m_hint.empty(); }

    auto Invoke(bool down) const {
        std::visit([down](auto&& arg){
            using T = std::decay_t<decltype(arg)>;
            if constexpr(std::is_same_v<T, CallbackEmpty>) {
                arg();
            } else if constexpr(std::is_same_v<T, CallbackWithBool>) {
                arg(down);
            } else {
                static_assert(false, "non-exhaustive visitor!");
            }
        }, m_callback);
    }

    u8 m_type{};
    Callback m_callback{};
    std::string m_hint{};
};

struct GenericHidState {
    GenericHidState() {
        Reset();
    }

    void Reset() {
        buttons_cur = 0;
        buttons_old = 0;
    }

    u64 GetButtons() const {
        return buttons_cur;
    }

    u64 GetButtonsDown() const {
        return buttons_cur & ~buttons_old;
    }

    u64 GetButtonsUp() const {
        return ~buttons_cur & buttons_old;
    }

    virtual void Update() = 0;

protected:
    u64 buttons_cur;
    u64 buttons_old;
};

struct KeyboardState final : GenericHidState {
    struct MapEntry {
        HidKeyboardKey key;
        u64 button;
    };
    using Map = std::span<const MapEntry>;

    void Init(Map map) {
        m_map = map;
        Reset();
    }

    void Update() override {
        buttons_old = buttons_cur;
        buttons_cur = 0;

        if (!hidGetKeyboardStates(&m_state, 1)) {
            return;
        }

        const auto ctrl = m_state.modifiers & HidKeyboardModifier_Control;
        const auto shift = m_state.modifiers & HidKeyboardModifier_Shift;

        for (const auto& map : m_map) {
            if (hidKeyboardStateGetKey(&m_state, map.key)) {
                if (shift && map.button == static_cast<u64>(Button::L)) {
                    buttons_cur |= static_cast<u64>(Button::L2);
                } else if (shift && map.button == static_cast<u64>(Button::R)) {
                    buttons_cur |= static_cast<u64>(Button::R2);
                } else if (ctrl && map.button == static_cast<u64>(Button::L)) {
                    buttons_cur |= static_cast<u64>(Button::L3);
                } else if (ctrl && map.button == static_cast<u64>(Button::R)) {
                    buttons_cur |= static_cast<u64>(Button::R3);
                } else {
                    buttons_cur |= map.button;
                }

            }
        }
    }

private:
    Map m_map{};
    HidKeyboardState m_state{};
};

struct Controller {
    u64 m_kdown{};
    u64 m_kheld{};
    u64 m_kup{};

    constexpr auto Set(Button button, bool down) noexcept -> void {
        m_kdown = static_cast<u64>(down ? m_kdown | static_cast<u64>(button) : m_kdown & ~static_cast<u64>(button));
    }

    constexpr auto Got(u64 k, Button button) const noexcept -> bool {
        return (k & static_cast<u64>(button)) > 0;
        // return (k & static_cast<u64>(button)) == static_cast<u64>(button);
    }

    constexpr auto GotDown(Button button) const noexcept -> bool {
        return Got(m_kdown, button);
    }

    constexpr auto GotHeld(Button button) const noexcept -> bool {
        return Got(m_kheld, button);
    }

    constexpr auto GotUp(Button button) const noexcept -> bool {
        return (m_kup & static_cast<u64>(button)) > 0;
    }

    constexpr auto Reset() noexcept -> void {
        m_kdown = 0;
        m_kup = 0;
    }

    void UpdateButtonHeld(u64 buttons, double delta) {
        if (m_kdown & buttons) {
            m_step_max = m_MAX_STEP;
            m_step = m_INC_STEP;
            m_counter = 0;
            m_step_max_counter = 0;
        } else if (m_kheld & buttons) {
            m_counter += m_step * delta;

            // if we are at the max, ignore the delta and go as fast as the frame rate.
            if (m_step_max == m_MAX) {
                m_counter = m_MAX;
            }

            if (m_counter >= m_MAX) {
                m_kdown |= m_kheld & buttons;
                m_counter = 0;
                m_step = std::min(m_step + m_INC_STEP, m_step_max);

                // slowly speed up until we reach 1 button down per frame.
                m_step_max_counter++;
                if (m_step_max_counter >= 5) {
                    m_step_max_counter = 0;
                    m_step_max = std::min(m_step_max + m_INC_STEP, m_MAX);
                }
            }
        }
    }

private:
    static constexpr double m_MAX = 1000;
    static constexpr double m_MAX_STEP = 250;
    static constexpr double m_INC_STEP = 50;

    double m_step_max = m_MAX_STEP;
    double m_step = m_INC_STEP;
    double m_counter = 0;
    int m_step_max_counter = 0;
};

} // namespace sphaira
