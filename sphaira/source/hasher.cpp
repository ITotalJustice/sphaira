#include "hasher.hpp"
#include "app.hpp"
#include "threaded_file_transfer.hpp"
#include <mbedtls/md5.h>
#include <utility>
#include <array>
#include <algorithm>
#include <cstring>
#include <cstdio>

namespace sphaira::hash {
namespace {

consteval auto CalculateHashStrLen(s64 buf_size) {
    return buf_size * 2 + 1;
}

struct FileSource final : BaseSource {
    FileSource(fs::Fs* fs, const fs::FsPath& path) : m_fs{fs} {
        m_open_result = m_fs->OpenFile(path, FsOpenMode_Read, std::addressof(m_file));
        m_is_file_based_emummc = App::IsFileBaseEmummc();
    }

    Result Size(s64* out) override {
        if (R_FAILED(m_open_result)) return m_open_result;
        return m_file.GetSize(out);
    }

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override {
        if (R_FAILED(m_open_result)) {
            if (bytes_read) *bytes_read = 0;
            return m_open_result;
        }

        const auto rc = m_file.Read(off, buf, size, 0, bytes_read);
        if (m_fs->IsNative() && m_is_file_based_emummc) {
            svcSleepThread(2e+6); // 2ms
        }
        return rc;
    }

private:
    fs::Fs* m_fs{};
    fs::File m_file{};
    Result m_open_result{};
    bool m_is_file_based_emummc{};
};

struct MemSource final : BaseSource {
    MemSource(std::span<const u8> data) : m_data{data} { }

    Result Size(s64* out) override {
        *out = static_cast<s64>(m_data.size());
        R_SUCCEED();
    }

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override {
        if (off < 0) {
            if (bytes_read) *bytes_read = 0;
            R_SUCCEED();
        }

        const auto avail = static_cast<s64>(m_data.size()) - off;
        if (avail <= 0) {
            if (bytes_read) *bytes_read = 0;
            R_SUCCEED();
        }

        const auto to_read = std::min<s64>(size, avail);
        std::memcpy(buf, m_data.data() + off, static_cast<size_t>(to_read));
        if (bytes_read) *bytes_read = static_cast<u64>(to_read);
        R_SUCCEED();
    }

private:
    const std::span<const u8> m_data;
};

struct HashSource {
    virtual ~HashSource() = default;
    virtual void Update(const void* buf, s64 size) = 0;
    virtual void Get(std::string& out) = 0;
};

struct HashCrc32 final : HashSource {
    void Update(const void* buf, s64 size) override {
        m_seed = crc32CalculateWithSeed(m_seed, buf, size);
    }

    void Get(std::string& out) override {
        char str[CalculateHashStrLen(sizeof(m_seed))];
        const char* hex = "0123456789abcdef";
        for (size_t i = 0; i < sizeof(m_seed); ++i) {
            const auto byte = static_cast<unsigned char>((m_seed >> ((sizeof(m_seed) - 1 - i) * 8)) & 0xFF);
            str[i * 2] = hex[(byte >> 4) & 0xF];
            str[i * 2 + 1] = hex[byte & 0xF];
        }
        str[sizeof(m_seed) * 2] = '\0';
        out = str;
    }

private:
    u32 m_seed{};
};

struct HashMd5 final : HashSource {
    HashMd5() {
        mbedtls_md5_init(&m_ctx);
        (void)mbedtls_md5_starts_ret(&m_ctx);
    }

    ~HashMd5() {
        mbedtls_md5_free(&m_ctx);
    }

    void Update(const void* buf, s64 size) override {
        (void)mbedtls_md5_update_ret(&m_ctx, reinterpret_cast<const unsigned char*>(buf), static_cast<size_t>(size));
    }

    void Get(std::string& out) override {
        unsigned char hash[16];
        (void)mbedtls_md5_finish_ret(&m_ctx, hash);

        constexpr size_t N = sizeof(hash) * 2 + 1;
        std::array<char, N> str{};
        const char* hex = "0123456789abcdef";
        for (size_t i = 0; i < sizeof(hash); ++i) {
            str[i * 2] = hex[(hash[i] >> 4) & 0xF];
            str[i * 2 + 1] = hex[hash[i] & 0xF];
        }
        str[sizeof(hash) * 2] = '\0';
        out.assign(str.data());
    }

private:
    mbedtls_md5_context m_ctx{};
};

struct HashSha1 final : HashSource {
    HashSha1() {
        sha1ContextCreate(&m_ctx);
    }

    void Update(const void* buf, s64 size) override {
        sha1ContextUpdate(&m_ctx, buf, size);
    }

    void Get(std::string& out) override {
        u8 hash[SHA1_HASH_SIZE];
        sha1ContextGetHash(&m_ctx, hash);

        constexpr size_t N = sizeof(hash) * 2 + 1;
        std::array<char, N> str{};
        const char* hex = "0123456789abcdef";
        for (size_t i = 0; i < sizeof(hash); ++i) {
            str[i * 2] = hex[(hash[i] >> 4) & 0xF];
            str[i * 2 + 1] = hex[hash[i] & 0xF];
        }
        str[sizeof(hash) * 2] = '\0';
        out.assign(str.data());
    }

private:
    Sha1Context m_ctx{};
};

struct HashSha256 final : HashSource {
    HashSha256() {
        sha256ContextCreate(&m_ctx);
    }

    void Update(const void* buf, s64 size) override {
        sha256ContextUpdate(&m_ctx, buf, size);
    }

    void Get(std::string& out) override {
        u8 hash[SHA256_HASH_SIZE];
        sha256ContextGetHash(&m_ctx, hash);

        constexpr size_t N = sizeof(hash) * 2 + 1;
        std::array<char, N> str{};
        const char* hex = "0123456789abcdef";
        for (size_t i = 0; i < sizeof(hash); ++i) {
            str[i * 2] = hex[(hash[i] >> 4) & 0xF];
            str[i * 2 + 1] = hex[hash[i] & 0xF];
        }
        str[sizeof(hash) * 2] = '\0';
        out.assign(str.data());
    }

private:
    Sha256Context m_ctx{};
};

Result Hash(ui::ProgressBox* pbox, std::unique_ptr<HashSource> hash, BaseSource* source, std::string& out) {
    s64 file_size;
    R_TRY(source->Size(&file_size));

    R_TRY(thread::Transfer(pbox, file_size,
        [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
            return source->Read(data, off, size, bytes_read);
        },
        [&](const void* data, s64 off, s64 size) -> Result {
            hash->Update(data, size);
            R_SUCCEED();
        }
    ));

    hash->Get(out);
    R_SUCCEED();
}

} // namespace

auto GetTypeStr(Type type) -> const char* {
    switch (type) {
        case Type::Crc32: return "CRC32";
        case Type::Md5: return "MD5";
        case Type::Sha1: return "SHA1";
        case Type::Sha256: return "SHA256";
    }
    return "";
}

Result Hash(ui::ProgressBox* pbox, Type type, BaseSource* source, std::string& out) {
    switch (type) {
        case Type::Crc32: return Hash(pbox, std::make_unique<HashCrc32>(), source, out);
        case Type::Md5: return Hash(pbox, std::make_unique<HashMd5>(), source, out);
        case Type::Sha1: return Hash(pbox, std::make_unique<HashSha1>(), source, out);
        case Type::Sha256: return Hash(pbox, std::make_unique<HashSha256>(), source, out);
    }
    std::unreachable();
}

Result Hash(ui::ProgressBox* pbox, Type type, fs::Fs* fs, const fs::FsPath& path, std::string& out) {
    auto source = std::make_unique<FileSource>(fs, path);
    return Hash(pbox, type, source.get(), out);
}

Result Hash(ui::ProgressBox* pbox, Type type, std::span<const u8> data, std::string& out) {
    auto source = std::make_unique<MemSource>(data);
    return Hash(pbox, type, source.get(), out);
}

} // namespace sphaira::hash
