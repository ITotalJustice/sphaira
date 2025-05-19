#pragma once

#include <string>
#include <string_view>

namespace sphaira::i18n {

bool init(long index);
void exit();

std::string get(std::string_view str);

} // namespace sphaira::i18n

inline namespace literals {

std::string operator""_i18n(const char* str, size_t len);

} // namespace literals
