// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

#include <stdint.h>
#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb
{

  class BlockBuilder;
  class BlockHandle;
  class WritableFile;

  // 在leveldb中，一个Table类实例就对应了一个sstable文件内容，而TableBuilder类就是
  // 用来构建一个sstable文件的。
  class LEVELDB_EXPORT TableBuilder
  {
  public:
    // TableBuilder类的构造函数，其中的file对象封装了sstable文件。
    TableBuilder(const Options &options, WritableFile *file);

    ~TableBuilder();

    Status ChangeOptions(const Options &options);

    // Add()方法用于将参数中的key-value信息加入到当前的table中，其实也就是sstable
    // 文件中，当然这个key-value最终会存放到sstable中的data block中。
    void Add(const Slice &key, const Slice &value);

    // 将当前正在构建的data block写入到sstable文件中，表示一个data block构建完毕。
    void Flush();

    // Return non-ok iff some error has been detected.
    Status status() const;

    // Finish()方法用来结束当前sstable的构建，用于往data block(如果有meta block的话，
    // 则是meta block)后面写入metaindex block和index block，接着写入footer来完成
    // 整个sstable的构建。
    Status Finish();

    void Abandon();

    // 当前已经加入到sstable中的key-value记录个数。
    uint64_t NumEntries() const;

    // 返回当前文件的大小。
    uint64_t FileSize() const;

  private:
    bool ok() const { return status().ok(); }

    // WriteBlock()方法用于往
    void WriteBlock(BlockBuilder *block, BlockHandle *handle);
    void WriteRawBlock(const Slice &data, CompressionType, BlockHandle *handle);

    struct Rep;
    Rep *rep_;

    // No copying allowed
    TableBuilder(const TableBuilder &);
    void operator=(const TableBuilder &);
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
