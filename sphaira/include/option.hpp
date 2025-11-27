#pragma once

#include <optional>
#include <string>

namespace sphaira::option {

template<typename T>
struct OptionBase {
    OptionBase(const std::string& section, const std::string& name, T default_value, bool file = true)
    : m_section{section}
    , m_name{name}
    , m_default_value{default_value}
    , m_file{file}
    {}

    auto Get() -> T;
    auto GetOr(const char* name) -> T;
    void Set(T value);

    // returns true if loaded.
    auto LoadFrom(const char* section, const char* name, const char* value) -> bool;
    // same as above, but only checks the name.
    auto LoadFrom(const char* name, const char* value) -> bool;

private:
    auto GetInternal(const char* name) -> T;

private:
    const std::string m_section;
    const std::string m_name;
    const T m_default_value;
    const bool m_file;
    std::optional<T> m_value;
};

using OptionBool = OptionBase<bool>;
using OptionLong = OptionBase<long>;
using OptionFloat = OptionBase<float>;
using OptionString = OptionBase<std::string>;

} // namespace sphaira::option
