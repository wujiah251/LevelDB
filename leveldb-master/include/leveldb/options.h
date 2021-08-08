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

  enum CompressionType
  {

    // 压缩类型
    kNoCompression = 0x0,
    kSnappyCompression = 0x1
  };

  struct LEVELDB_EXPORT Options
  {
    // 比较器
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

    // 过滤器策略
    const FilterPolicy *filter_policy;

    Options();
  };

  // 下面是读写的控制选项
  struct LEVELDB_EXPORT ReadOptions
  {
    bool verify_checksums; //是否检查校验和
    bool fill_cache;       // 快照
    const Snapshot *snapshot;
    ReadOptions()
        : verify_checksums(false),
          fill_cache(true),
          snapshot(NULL)
    {
    }
  };

  struct LEVELDB_EXPORT WriteOptions
  {
    bool sync;
    // 写入方式，默认为异步，即只写内存，后台compaction时写入磁盘
    WriteOptions()
        : sync(false)
    {
    }
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
