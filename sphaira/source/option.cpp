#include <minIni.h>
#include <type_traits>
#include "option.hpp"
#include "app.hpp"

namespace sphaira::option {

template<typename T>
auto OptionBase<T>::Get() -> T {
    if (!m_value.has_value()) {
        if constexpr(std::is_same_v<T, bool>) {
            m_value = ini_getbool(m_section.c_str(), m_name.c_str(), m_default_value, App::CONFIG_PATH);
        } else if constexpr(std::is_same_v<T, long>) {
            m_value = ini_getl(m_section.c_str(), m_name.c_str(), m_default_value, App::CONFIG_PATH);
        } else if constexpr(std::is_same_v<T, std::string>) {
            char buf[FS_MAX_PATH];
            ini_gets(m_section.c_str(), m_name.c_str(), m_default_value.c_str(), buf, sizeof(buf), App::CONFIG_PATH);
            m_value = buf;
        }
    }
    return m_value.value();
}

template<typename T>
void OptionBase<T>::Set(T value) {
    m_value = value;
    if constexpr(std::is_same_v<T, bool>) {
        ini_putl(m_section.c_str(), m_name.c_str(), value, App::CONFIG_PATH);
    } else if constexpr(std::is_same_v<T, long>) {
        ini_putl(m_section.c_str(), m_name.c_str(), value, App::CONFIG_PATH);
    } else if constexpr(std::is_same_v<T, std::string>) {
        ini_puts(m_section.c_str(), m_name.c_str(), value.c_str(), App::CONFIG_PATH);
    }
}

template struct OptionBase<bool>;
template struct OptionBase<long>;
template struct OptionBase<std::string>;

template<typename T>
auto OptionBaseNoIni<T>::Get() -> T {
    return m_value.value();
}

template<typename T>
void OptionBaseNoIni<T>::Set(T value) {
    m_value = value;
}

template struct OptionBaseNoIni<bool>;
template struct OptionBaseNoIni<long>;
template struct OptionBaseNoIni<std::string>;

} //  namespace sphaira::option
