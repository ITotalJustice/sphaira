#pragma once

#include <switch.h>
#include <array>
#include <cstring>
#include "defines.hpp"

namespace sphaira::keys {

struct KeyEntry {
    u8 key[AES_128_KEY_SIZE]{};

    auto IsValid() const -> bool {
        const KeyEntry empty{};
        return std::memcmp(key, &empty, sizeof(key));
    }
};

using KeySection = std::array<KeyEntry, 0x20>;
struct Keys {
    u8 header_key[0x20]{};
    // the below are only found if read_from_file=true
    KeySection key_area_key[0x3]{}; // index
    KeySection titlekek{};
    KeySection master_key{};
    KeyEntry eticket_rsa_kek{};
    SetCalRsa2048DeviceKey eticket_device_key{};

    static auto FixKey(u8 key) -> u8 {
        if (key) {
            return key - 1;
        }
        return key;
    }

    auto HasNcaKeyArea(u8 key, u8 index) const -> bool {
        return key_area_key[index][FixKey(key)].IsValid();
    }

    auto HasTitleKek(u8 key) const -> bool {
        return titlekek[FixKey(key)].IsValid();
    }

    auto HasMasterKey(u8 key) const -> bool {
        return master_key[FixKey(key)].IsValid();
    }

    auto GetNcaKeyArea(KeyEntry* out, u8 key, u8 index) const -> Result {
        R_UNLESS(HasNcaKeyArea(key, index), Result_KeyMissingNcaKeyArea);
        *out = key_area_key[index][FixKey(key)];
        R_SUCCEED();
    }

    auto GetTitleKek(KeyEntry* out, u8 key) const -> Result {
        R_UNLESS(HasTitleKek(key), Result_KeyMissingTitleKek);
        *out = titlekek[FixKey(key)];
        R_SUCCEED();
    }

    auto GetMasterKey(KeyEntry* out, u8 key) const -> Result {
        R_UNLESS(HasMasterKey(key), Result_KeyMissingMasterKey);
        *out = master_key[FixKey(key)];
        R_SUCCEED();
    }
};

void parse_hex_key(void* key, const char* hex);
Result parse_keys(Keys& out, bool read_from_file);

} // namespace sphaira::keys
