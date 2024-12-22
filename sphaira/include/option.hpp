#pragma once

#include <optional>
#include <string>

namespace sphaira::option {

template<typename T>
struct OptionBase {
    OptionBase(const std::string& section, const std::string& name, T default_value)
    : m_section{section}
    , m_name{name}
    , m_default_value{default_value}
    {}

    auto Get() -> T;
    void Set(T value);

private:
    const std::string m_section;
    const std::string m_name;
    const T m_default_value;
    std::optional<T> m_value;
};

template<typename T>
struct OptionBaseNoIni {
    OptionBaseNoIni(const std::string& name, T default_value)
    : m_name{name}
    , m_default_value{default_value}
    {}

    auto Get() -> T;
    void Set(T value);

private:
    const std::string m_name;
    const T m_default_value;
    std::optional<T> m_value;
};

using OptionBool = OptionBase<bool>;
using OptionLong = OptionBase<long>;
using OptionString = OptionBase<std::string>;

using OptionNoIniBool = OptionBaseNoIni<bool>;
using OptionNoIniLong = OptionBaseNoIni<long>;
using OptionNoIniString = OptionBaseNoIni<std::string>;

} // namespace sphaira::option
