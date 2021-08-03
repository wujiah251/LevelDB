// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <stdint.h>
#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb
{

  class LEVELDB_EXPORT Cache;
  // 返回一个LRU缓存实例
  LEVELDB_EXPORT Cache *NewLRUCache(size_t capacity);

  // Cache类是一个抽象类，定义了缓存类的基本接口
  class LEVELDB_EXPORT Cache
  {
  public:
    // 构造函数
    Cache() {}
    // 析构函数
    virtual ~Cache();

    struct Handle
    {
    };
    // 插入查找
    virtual Handle *Insert(const Slice &key, void *value, size_t charge,
                           void (*deleter)(const Slice &key, void *value)) = 0;
    //
    virtual Handle *Lookup(const Slice &key) = 0;

    virtual void Release(Handle *handle) = 0;

    virtual void *Value(Handle *handle) = 0;

    virtual void Erase(const Slice &key) = 0;

    virtual uint64_t NewId() = 0;

    virtual void Prune() {}

    virtual size_t TotalCharge() const = 0;

  private:
    void LRU_Remove(Handle *e);
    void LRU_Append(Handle *e);
    void Unref(Handle *e);

    struct Rep;
    Rep *rep_;

    // No copying allowed
    Cache(const Cache &);
    void operator=(const Cache &);
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_CACHE_H_
