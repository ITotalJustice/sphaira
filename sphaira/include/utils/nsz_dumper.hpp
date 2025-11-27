#pragma once

#include "fs.hpp"
#include "defines.hpp"
#include "dumper.hpp"

#include "ui/progress_box.hpp"
#include "yati/nx/keys.hpp"
#include "yati/nx/nca.hpp"
#include "yati/container/base.hpp"

#include <functional>

namespace sphaira::utils::nsz {

using Collection = yati::container::CollectionEntry;
using Collections = yati::container::Collections;

using NcaReaderCreator = std::function<std::unique_ptr<nca::NcaReader>(const nca::Header& header, const keys::KeyEntry& title_key, const Collection& collection)>;

Result NszExport(ui::ProgressBox* pbox, const NcaReaderCreator& nca_creator, s64& read_offset, s64& write_offset, Collections& collections, const keys::Keys& keys, dump::BaseSource* source, dump::WriteSource* writer, const fs::FsPath& path);

} // namespace sphaira::utils::nsz
