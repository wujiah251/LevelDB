// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <string>
#include <stdint.h>
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace leveldb
{

  class Block;
  class RandomAccessFile;
  struct ReadOptions;

  // BlockHandle类描述了一个block在文件中的偏移和大小。
  // 这个大小不包含紧随在block后面的type和crc的5个字节大小。
  class BlockHandle
  {
  public:
    BlockHandle();

    uint64_t offset() const { return offset_; }
    void set_offset(uint64_t offset) { offset_ = offset; }

    uint64_t size() const { return size_; }
    void set_size(uint64_t size) { size_ = size; }

    // EncodeTo()方法将offset_和size_以varint64的编码方式编码后追加到dst中。
    void EncodeTo(std::string *dst) const;

    // DecodeFrom()方法从Slice对象input中解码出整形数字，并存放到offset_和size_。
    Status DecodeFrom(Slice *input);

    // EncodeTo()函数编码BlockHandle类实例时，需要编码两个64位成员，
    // 采用varint64编码方式时最多需要20个字节。
    enum
    {
      kMaxEncodedLength = 10 + 10
    };

  private:
    uint64_t offset_;
    uint64_t size_;
  };

  class Footer
  {
  public:
    Footer() {}
    // 读取
    const BlockHandle &metaindex_handle() const { return metaindex_handle_; }
    void set_metaindex_handle(const BlockHandle &h) { metaindex_handle_ = h; }

    const BlockHandle &index_handle() const
    {
      return index_handle_;
    }
    void set_index_handle(const BlockHandle &h)
    {
      index_handle_ = h;
    }

    void EncodeTo(std::string *dst) const;
    Status DecodeFrom(Slice *input);

    // EncodeTo()方法在编码Footer类实例的时候，需要编码两个BlockHandle类实例，
    // 以及一个64位的魔术字，这个魔术字的编码方式没有采用varint64的格式，而
    // 是用普通的编码方式进行编码，需要8个字节。所以编码Footer类实例所需要的
    // 最大内存就是kEncodedLength字节。
    enum
    {
      // 48个字节
      kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8
    };

  private:
    BlockHandle metaindex_handle_;
    BlockHandle index_handle_;
  };

  static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

  // 1-byte type + 32-bit crc
  // ssttable文件的读写也是以一个块为单位的，一个块中分为三部分
  // block + type + crc，所以这里的kBlockTrailerSize就包括了
  // type和crc的大小。
  static const size_t kBlockTrailerSize = 5;

  // BlockContents用来存放Block相关的一些信息，在ReadBlock()时使用。
  struct BlockContents
  {
    Slice data;
    bool cachable;
    bool heap_allocated;
  };

  extern Status ReadBlock(RandomAccessFile *file,
                          const ReadOptions &options,
                          const BlockHandle &handle,
                          BlockContents *result);

  // Implementation details follow.  Clients should ignore,

  inline BlockHandle::BlockHandle()
      : offset_(~static_cast<uint64_t>(0)),
        size_(~static_cast<uint64_t>(0))
  {
  }

} // namespace leveldb

#endif // STORAGE_LEVELDB_TABLE_FORMAT_H_
