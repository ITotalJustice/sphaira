#pragma once

#include "fs.hpp"
#include "ui/progress_box.hpp"
#include <string>
#include <memory>
#include <span>
#include <switch.h>

namespace sphaira::hash {

enum class Type {
    Crc32,
    Md5,
    Sha1,
    Sha256,
    Null,
};

struct BaseSource {
    virtual ~BaseSource() = default;
    virtual Result Size(s64* out) = 0;
    virtual Result Read(void* buf, s64 off, s64 size, u64* bytes_read) = 0;
};

auto GetTypeStr(Type type) -> const char*;

// returns the hash string.
Result Hash(ui::ProgressBox* pbox, Type type, BaseSource* source, std::string& out);
Result Hash(ui::ProgressBox* pbox, Type type, fs::Fs* fs, const fs::FsPath& path, std::string& out);
Result Hash(ui::ProgressBox* pbox, Type type, std::span<const u8> data, std::string& out);

} // namespace sphaira::hash
