#pragma once

#include <switch.h>

namespace sphaira::usb::tinfoil {

enum Magic : u32 {
    Magic_List0 = 0x304C5554, // TUL0 (Tinfoil Usb List 0)
    Magic_Command0 = 0x30435554, // TUC0 (Tinfoil USB Command 0)
};

enum USBCmdType : u8 {
    REQUEST = 0,
    RESPONSE = 1
};

enum USBCmdId : u32 {
    EXIT = 0,
    FILE_RANGE = 1
};

struct TUSHeader {
    u32 magic; // TUL0 (Tinfoil Usb List 0)
    u32 nspListSize;
    u64 padding;
};

struct NX_PACKED USBCmdHeader {
    u32 magic; // TUC0 (Tinfoil USB Command 0)
    USBCmdType type;
    u8 padding[0x3];
    u32 cmdId;
    u64 dataSize;
    u8 reserved[0xC];
};

struct FileRangeCmdHeader {
    u64 size;
    u64 offset;
    u64 nspNameLen;
    u64 padding;
};

static_assert(sizeof(TUSHeader) == 0x10, "TUSHeader must be 0x10!");
static_assert(sizeof(USBCmdHeader) == 0x20, "USBCmdHeader must be 0x20!");

} // namespace sphaira::usb::tinfoil
