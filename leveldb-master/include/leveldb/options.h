// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
#define STORAGE_LEVELDB_INCLUDE_OPTIONS_H_

#include <stddef.h>
#include "leveldb/export.h"

namespace leveldb
{

  class Cache;
  class Comparator;
  class Env;
  class FilterPolicy;
  class Logger;
  class Snapshot;

  // DB contents are stored in a set of blocks, each of which holds a
  // sequence of key,value pairs.  Each block may be compressed before
  // being stored in a file.  The following enum describes which
  // compression method (if any) is used to compress a block.
  enum CompressionType
  {
    // NOTE: do not change the values of existing entries, as these are
    // part of the persistent format on disk.
    kNoCompression = 0x0,
    kSnappyCompression = 0x1
  };

  // Options to control the behavior of a database (passed to DB::Open)
  struct LEVELDB_EXPORT Options
  {
    const Comparator *comparator;
    bool create_if_missing;
    bool error_if_exists;
    bool paranoid_checks;

    Env *env;

    Logger *info_log;

    // Default: 4MB
    size_t write_buffer_size;

    // Default: 1000
    int max_open_files;

    // Default: NULL
    Cache *block_cache;

    // Default: 4K
    size_t block_size;

    // Default: 16
    int block_restart_interval;

    // Default: 2MB
    size_t max_file_size;

    CompressionType compression;

    // Default: currently false, but may become true later.
    bool reuse_logs;

    // Default: NULL
    const FilterPolicy *filter_policy;

    // Create an Options object with default values for all fields.
    Options();
  };

  // Options that control read operations
  struct LEVELDB_EXPORT ReadOptions
  {
    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    // Default: false
    bool verify_checksums;

    // Should the data read for this iteration be cached in memory?
    // Callers may wish to set this field to false for bulk scans.
    // Default: true
    bool fill_cache;

    // If "snapshot" is non-NULL, read as of the supplied snapshot
    // (which must belong to the DB that is being read and which must
    // not have been released).  If "snapshot" is NULL, use an implicit
    // snapshot of the state at the beginning of this read operation.
    // Default: NULL
    const Snapshot *snapshot;

    ReadOptions()
        : verify_checksums(false),
          fill_cache(true),
          snapshot(NULL)
    {
    }
  };

  // Options that control write operations
  struct LEVELDB_EXPORT WriteOptions
  {
    // If true, the write will be flushed from the operating system
    // buffer cache (by calling WritableFile::Sync()) before the write
    // is considered complete.  If this flag is true, writes will be
    // slower.
    //
    // If this flag is false, and the machine crashes, some recent
    // writes may be lost.  Note that if it is just the process that
    // crashes (i.e., the machine does not reboot), no writes will be
    // lost even if sync==false.
    //
    // In other words, a DB write with sync==false has similar
    // crash semantics as the "write()" system call.  A DB write
    // with sync==true has similar crash semantics to a "write()"
    // system call followed by "fsync()".
    //
    // Default: false
    bool sync;

    WriteOptions()
        : sync(false)
    {
    }
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
