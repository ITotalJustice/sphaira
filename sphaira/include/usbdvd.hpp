#pragma once

#include <switch.h>
#include "location.hpp"

namespace sphaira::usbdvd {

Result MountAll();
void UnmountAll();

bool GetMountPoint(location::StdioEntry& out);

} // namespace sphaira::usbdvd
