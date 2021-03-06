// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <string>
#include <stdint.h>
#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb
{

  class Env;

  class TableCache
  {
  public:
    TableCache(const std::string &dbname, const Options *options, int entries);
    ~TableCache();

    Iterator *NewIterator(const ReadOptions &options,
                          uint64_t file_number,
                          uint64_t file_size,
                          Table **tableptr = NULL);

    Status Get(const ReadOptions &options,
               uint64_t file_number,
               uint64_t file_size,
               const Slice &k,
               void *arg,
               void (*handle_result)(void *, const Slice &, const Slice &));

    void Evict(uint64_t file_number);

  private:
    Env *const env_;           // env_是leveldb所处的环境信息
    const std::string dbname_; // dbname_是数据库名字
    const Options *options_;
    // LRUCache类实例指针，这个LRUCache用来存放TableAndFile类实例，其key值是
    // table对应的file_number。
    Cache *cache_;

    Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle **);
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
