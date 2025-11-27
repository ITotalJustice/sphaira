#pragma once

#include <switch.h>

namespace sphaira::crypto {

struct Aes128 {
    Aes128(const void *key, bool is_encryptor) {
        m_is_encryptor = is_encryptor;
        aes128ContextCreate(&m_ctx, key, is_encryptor);
    }

    void Run(void *dst, const void *src) {
        if (m_is_encryptor) {
            aes128EncryptBlock(&m_ctx, dst, src);
        } else {
            aes128DecryptBlock(&m_ctx, dst, src);
        }
    }

private:
    Aes128Context m_ctx;
    bool m_is_encryptor;
};

struct Aes128Xts {
    Aes128Xts(const u8 *key, bool is_encryptor) : Aes128Xts{key, key + 0x10, is_encryptor} { }
    Aes128Xts(const void *key0, const void *key1, bool is_encryptor) {
        m_is_encryptor = is_encryptor;
        aes128XtsContextCreate(&m_ctx, key0, key1, is_encryptor);
    }

    void Run(void *dst, const void *src, u64 sector, u64 sector_size, u64 data_size) {
        for (u64 pos = 0; pos < data_size; pos += sector_size) {
            aes128XtsContextResetSector(&m_ctx, sector++, true);
            if (m_is_encryptor) {
                aes128XtsEncrypt(&m_ctx, static_cast<u8*>(dst) + pos, static_cast<const u8*>(src) + pos, sector_size);
            } else {
                aes128XtsDecrypt(&m_ctx, static_cast<u8*>(dst) + pos, static_cast<const u8*>(src) + pos, sector_size);
            }
        }
    }

private:
    Aes128XtsContext m_ctx;
    bool m_is_encryptor;
};

static inline void cryptoAes128(const void *in, void *out, const void* key, bool is_encryptor) {
    Aes128(key, is_encryptor).Run(out, in);
}

static inline void cryptoAes128Xts(const void* in, void* out, const u8* key, u64 sector, u64 sector_size, u64 data_size, bool is_encryptor) {
    Aes128Xts(key, is_encryptor).Run(out, in, sector, sector_size, data_size);
}

static inline void UpdateCtr(u8* counter, u64 offset) {
    const u64 swp = __bswap64(offset >> 4);
    std::memcpy(&counter[0x8], &swp, 0x8);
}

static inline void SetCtr(u8* counter, u64 ctr, u64 offset = 0) {
    const u64 swp = __bswap64(ctr);
    std::memcpy(&counter[0x0], &swp, 0x8);
    UpdateCtr(counter, offset);
}

} // namespace sphaira::crypto
