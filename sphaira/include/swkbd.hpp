#pragma once

#include <switch.h>
#include <string>
#include <sys/syslimits.h>

namespace sphaira::swkbd {

Result ShowText(std::string& out, const char* header = nullptr, const char* guide = nullptr, const char* initial = nullptr, s64 len_min = -1, s64 len_max = PATH_MAX);
Result ShowNumPad(s64& out, const char* header = nullptr, const char* guide = nullptr, const char* initial = nullptr, s64 len_min = -1, s64 len_max = PATH_MAX);

} // namespace sphaira::swkbd
