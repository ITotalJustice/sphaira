#pragma once

#include <switch.h>
#include <string>

namespace sphaira::swkbd {

Result ShowText(std::string& out, const char* guide = nullptr, const char* initial = nullptr, s64 len_min = -1, s64 len_max = FS_MAX_PATH);
Result ShowNumPad(s64& out, const char* guide = nullptr, const char* initial = nullptr, s64 len_min = -1, s64 len_max = FS_MAX_PATH);

} // namespace sphaira::swkbd
