// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <stdint.h>
#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb
{

  class WritableFile;

  namespace log
  {

    class Writer
    {
    public:
      explicit Writer(WritableFile *dest);

      Writer(WritableFile *dest, uint64_t dest_length);

      ~Writer();

      // 该函数用于向日志模块中添加一条操作日志。
      Status AddRecord(const Slice &slice);

    private:
      WritableFile *dest_;
      // 当前正在使用的block中未使用部分相对于block起始的偏移。
      int block_offset_; // Current offset in block

      uint32_t type_crc_[kMaxRecordType + 1];

      Status EmitPhysicalRecord(RecordType type, const char *ptr, size_t length);

      // No copying allowed
      Writer(const Writer &);
      void operator=(const Writer &);
    };

  } // namespace log
} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_LOG_WRITER_H_
