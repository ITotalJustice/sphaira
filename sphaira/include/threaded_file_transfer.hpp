#pragma once

#include "ui/progress_box.hpp"
#include <functional>
#include <switch.h>

namespace sphaira::thread {

enum class Mode {
    // default, always multi-thread.
    MultiThreaded,
    // always single-thread.
    SingleThreaded,
    // check buffer size, if smaller, single thread.
    SingleThreadedIfSmaller,
};

using DecompressWriteCallback = std::function<Result(const void* data, s64 size)>;

using ReadCallback = std::function<Result(void* data, s64 off, s64 size, u64* bytes_read)>;
using DecompressCallback = std::function<Result(void* data, s64 off, s64 size, const DecompressWriteCallback& callback)>;
using WriteCallback = std::function<Result(const void* data, s64 off, s64 size)>;

// used for pull api
using PullCallback = std::function<Result(void* data, s64 size, u64* bytes_read)>;
using StartThreadCallback = std::function<Result(void)>;

// called when threads are started.
// call pull() to receive data.
using StartCallback = std::function<Result(PullCallback pull)>;

// same as above, but the callee must call start() in order to start threads.
// this is for convenience as there may be race conditions otherwise, such as the read thread
// trying to read from the pull callback before it is set.
using StartCallback2 = std::function<Result(StartThreadCallback start, PullCallback pull)>;

// reads data from rfunc into wfunc.
Result Transfer(ui::ProgressBox* pbox, s64 size, const ReadCallback& rfunc, const WriteCallback& wfunc, Mode mode = Mode::MultiThreaded);
Result Transfer(ui::ProgressBox* pbox, s64 size, const ReadCallback& rfunc, const DecompressCallback& dfunc, const WriteCallback& wfunc, Mode mode = Mode::MultiThreaded);

// reads data from rfunc, pull data from provided pull() callback.
Result TransferPull(ui::ProgressBox* pbox, s64 size, const ReadCallback& rfunc, const StartCallback& sfunc, Mode mode = Mode::MultiThreaded);
Result TransferPull(ui::ProgressBox* pbox, s64 size, const ReadCallback& rfunc, const StartCallback2& sfunc, Mode mode = Mode::MultiThreaded);

// helper for extract zips.
// this will multi-thread unzip if size >= 512KiB, otherwise it'll single pass.
Result TransferUnzip(ui::ProgressBox* pbox, void* zfile, fs::Fs* fs, const fs::FsPath& path, s64 size, u32 crc32 = 0, Mode mode = Mode::SingleThreadedIfSmaller);

// same as above but for zipping files.
Result TransferZip(ui::ProgressBox* pbox, void* zfile, fs::Fs* fs, const fs::FsPath& path, u32* crc32 = nullptr, Mode mode = Mode::SingleThreadedIfSmaller);

// passes the name inside the zip an final output path.
using UnzipAllFilter = std::function<bool(const fs::FsPath& name, fs::FsPath& path)>;

// helper all-in-one unzip function that unzips a zip (either open or path provided).
// the filter function can be used to modify the path and filter out unwanted files.
Result TransferUnzipAll(ui::ProgressBox* pbox, void* zfile, fs::Fs* fs, const fs::FsPath& base_path, const UnzipAllFilter& filter = nullptr, Mode mode = Mode::SingleThreadedIfSmaller);
Result TransferUnzipAll(ui::ProgressBox* pbox, const fs::FsPath& zip_out, fs::Fs* fs, const fs::FsPath& base_path, const UnzipAllFilter& filter = nullptr, Mode mode = Mode::SingleThreadedIfSmaller);

} // namespace sphaira::thread
