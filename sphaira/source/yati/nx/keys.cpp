#include "yati/nx/keys.hpp"
#include "yati/nx/nca.hpp"
#include "yati/nx/es.hpp"
#include "yati/nx/crypto.hpp"
#include "defines.hpp"
#include "log.hpp"
#include <minIni.h>
#include <memory>
#include <bit>
#include <cstring>

namespace sphaira::keys {
namespace {

constexpr u8 HEADER_KEK_SRC[0x10] = {
    0x1F, 0x12, 0x91, 0x3A, 0x4A, 0xCB, 0xF0, 0x0D, 0x4C, 0xDE, 0x3A, 0xF6, 0xD5, 0x23, 0x88, 0x2A
};

constexpr u8 HEADER_KEY_SRC[0x20] = {
    0x5A, 0x3E, 0xD8, 0x4F, 0xDE, 0xC0, 0xD8, 0x26, 0x31, 0xF7, 0xE2, 0x5D, 0x19, 0x7B, 0xF5, 0xD0,
    0x1C, 0x9B, 0x7B, 0xFA, 0xF6, 0x28, 0x18, 0x3D, 0x71, 0xF6, 0x4D, 0x73, 0xF1, 0x50, 0xB9, 0xD2
};

} // namespace

void parse_hex_key(void* key, const char* hex) {
    char low[0x11]{};
    char upp[0x11]{};
    std::memcpy(low, hex, 0x10);
    std::memcpy(upp, hex + 0x10, 0x10);
    *(u64*)key = std::byteswap(std::strtoul(low, nullptr, 0x10));
    *(u64*)((u8*)key + 8) = std::byteswap(std::strtoul(upp, nullptr, 0x10));
}

Result parse_keys(Keys& out, bool read_from_file) {
    static constexpr auto find_key = [](const char* key, const char* value, const char* search_key, KeySection& key_section) -> bool {
        if (!std::strncmp(key, search_key, std::strlen(search_key))) {
            // get key index.
            char* end;
            const auto key_value_str = key + std::strlen(search_key);
            const auto index = std::strtoul(key_value_str, &end, 0x10);
            if (end && end != key_value_str && index < 0x20) {
                KeyEntry keak;
                parse_hex_key(std::addressof(keak), value);
                key_section[index] = keak;
                return true;
            }
        }

        return false;
    };

    static constexpr auto find_key_single = [](const char* key, const char* value, const char* search_key, KeyEntry& key_entry) -> bool {
        if (!std::strcmp(key, search_key)) {
            parse_hex_key(std::addressof(key_entry), value);
            return true;
        }

        return false;
    };

    static constexpr auto cb = [](const mTCHAR *Section, const mTCHAR *Key, const mTCHAR *Value, void *UserData) -> int {
        auto keys = static_cast<Keys*>(UserData);

        auto key_text_key_area_key_app = "key_area_key_application_";
        auto key_text_key_area_key_oce = "key_area_key_ocean_";
        auto key_text_key_area_key_sys = "key_area_key_system_";
        auto key_text_titlekek = "titlekek_";
        auto key_text_master_key = "master_key_";
        auto key_text_eticket_rsa_kek = keys->eticket_device_key.generation ? "eticket_rsa_kek_personalized" : "eticket_rsa_kek";

        if (find_key(Key, Value, key_text_key_area_key_app, keys->key_area_key[nca::KeyAreaEncryptionKeyIndex_Application])) {
            return 1;
        } else if (find_key(Key, Value, key_text_key_area_key_oce, keys->key_area_key[nca::KeyAreaEncryptionKeyIndex_Ocean])) {
            return 1;
        } else if (find_key(Key, Value, key_text_key_area_key_sys, keys->key_area_key[nca::KeyAreaEncryptionKeyIndex_System])) {
            return 1;
        } else if (find_key(Key, Value, key_text_titlekek, keys->titlekek)) {
            return 1;
        } else if (find_key(Key, Value, key_text_master_key, keys->master_key)) {
            return 1;
        } else if (find_key_single(Key, Value, key_text_eticket_rsa_kek, keys->eticket_rsa_kek)) {
            log_write("found key single: key: %s value %s\n", Key, Value);
            return 1;
        }

        return 1;
    };

    R_TRY(splCryptoInitialize());
    ON_SCOPE_EXIT(splCryptoExit());

    u8 header_kek[0x20];
    R_TRY(splCryptoGenerateAesKek(HEADER_KEK_SRC, 0, 0, header_kek));
    R_TRY(splCryptoGenerateAesKey(header_kek, HEADER_KEY_SRC, out.header_key));
    R_TRY(splCryptoGenerateAesKey(header_kek, HEADER_KEY_SRC + 0x10, out.header_key + 0x10));

    if (read_from_file) {
        // get eticket device key, needed for decrypting personalised tickets.
        R_TRY(setcalInitialize());
        ON_SCOPE_EXIT(setcalExit());
        R_TRY(setcalGetEticketDeviceKey(std::addressof(out.eticket_device_key)));

        // it doesn't matter if this fails, its just that title decryption will also fail.
        if (ini_browse(cb, std::addressof(out), "/switch/prod.keys")) {
            // decrypt eticket device key.
            if (out.eticket_rsa_kek.IsValid()) {
                auto rsa_key = (es::EticketRsaDeviceKey*)out.eticket_device_key.key;

                Aes128CtrContext eticket_aes_ctx{};
                aes128CtrContextCreate(&eticket_aes_ctx, &out.eticket_rsa_kek, rsa_key->ctr);
                aes128CtrCrypt(&eticket_aes_ctx, &(rsa_key->private_exponent), &(rsa_key->private_exponent), sizeof(es::EticketRsaDeviceKey) - sizeof(rsa_key->ctr));

                const auto public_exponent = std::byteswap(rsa_key->public_exponent);
                if (public_exponent != 0x10001) {
                    log_write("etick decryption fail: 0x%X\n", public_exponent);
                    if (public_exponent == 0) {
                        log_write("eticket device id is NULL\n");
                    }
                    R_THROW(Result_KeyFailedDecyptETicketDeviceKey);
                } else {
                    log_write("eticket match\n");
                }
            }
        }
    }

    R_SUCCEED();
}

} // namespace sphaira::keys
