#pragma once

#include "base.hpp"
#include "fs.hpp"
#include <cstdio>
#include <switch.h>

namespace sphaira::yati::source {

struct Stdio final : Base {
    Stdio(const fs::FsPath& path);
    ~Stdio();

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override;

private:
    std::FILE* m_file{};
};

} // namespace sphaira::yati::source
