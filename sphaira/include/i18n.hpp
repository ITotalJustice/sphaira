#pragma once

#include <string>
#include <string_view>

namespace sphaira::i18n {

enum class WordOrder {
    PhraseName,  // default: SVO (English, French, German, etc.)
    NamePhrase   // SOV (Japanese, Korean)
};

bool init(long index);
void exit();

std::string get(std::string_view str);
std::string get(std::string_view str, std::string_view fallback);

WordOrder GetWordOrder();
bool WordOrderLocale();

std::string Reorder(std::string_view phrase, std::string_view name);

} // namespace sphaira::i18n

inline namespace literals {

std::string operator""_i18n(const char* str, size_t len);

} // namespace literals
