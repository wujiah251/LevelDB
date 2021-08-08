// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb
{

  class SnapshotList;

  // 快照类
  class SnapshotImpl : public Snapshot
  {
  public:
    // 序列号，唯一数据结构
    SequenceNumber number_;

  private:
    friend class SnapshotList;

    SnapshotImpl *prev_;
    SnapshotImpl *next_;

    SnapshotList *list_;
  };
  // 快照链表
  class SnapshotList
  {
  public:
    SnapshotList()
    {
      list_.prev_ = &list_;
      list_.next_ = &list_;
    }

    bool empty() const { return list_.next_ == &list_; }
    // 返回最旧的快照
    SnapshotImpl *oldest() const
    {
      assert(!empty());
      return list_.next_;
    }
    // 返回最新的快照
    SnapshotImpl *newest() const
    {
      assert(!empty());
      return list_.prev_;
    }
    // 根据序列号生成一个新的快照
    // 插入到末尾
    const SnapshotImpl *New(SequenceNumber seq)
    {
      SnapshotImpl *s = new SnapshotImpl;
      s->number_ = seq;
      s->list_ = this;
      s->next_ = &list_;
      s->prev_ = list_.prev_;
      s->prev_->next_ = s;
      s->next_->prev_ = s;
      return s;
    }
    // 删除一个快照
    void Delete(const SnapshotImpl *s)
    {
      assert(s->list_ == this);
      s->prev_->next_ = s->next_;
      s->next_->prev_ = s->prev_;
      delete s;
    }

  private:
    // 头节点
    SnapshotImpl list_;
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_SNAPSHOT_H_
