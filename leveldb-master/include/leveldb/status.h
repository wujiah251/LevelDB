// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_STATUS_H_
#define STORAGE_LEVELDB_INCLUDE_STATUS_H_

#include <string>
#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb
{

  class LEVELDB_EXPORT Status
  {
  public:
    // Create a success status.
    Status() : state_(NULL) {}
    ~Status() { delete[] state_; }

    Status(const Status &s);
    void operator=(const Status &s);

    static Status OK() { return Status(); }

    // 下面这一族函数用于编码对应状态码的状态信息。
    static Status NotFound(const Slice &msg, const Slice &msg2 = Slice())
    {
      return Status(kNotFound, msg, msg2);
    }
    static Status Corruption(const Slice &msg, const Slice &msg2 = Slice())
    {
      return Status(kCorruption, msg, msg2);
    }
    static Status NotSupported(const Slice &msg, const Slice &msg2 = Slice())
    {
      return Status(kNotSupported, msg, msg2);
    }
    static Status InvalidArgument(const Slice &msg, const Slice &msg2 = Slice())
    {
      return Status(kInvalidArgument, msg, msg2);
    }
    static Status IOError(const Slice &msg, const Slice &msg2 = Slice())
    {
      return Status(kIOError, msg, msg2);
    }

    // Returns true iff the status indicates success.
    bool ok() const { return (state_ == NULL); }

    // Returns true iff the status indicates a NotFound error.
    bool IsNotFound() const { return code() == kNotFound; }

    // Returns true iff the status indicates a Corruption error.
    bool IsCorruption() const { return code() == kCorruption; }

    // Returns true iff the status indicates an IOError.
    bool IsIOError() const { return code() == kIOError; }

    // Returns true iff the status indicates a NotSupportedError.
    bool IsNotSupportedError() const { return code() == kNotSupported; }

    // Returns true iff the status indicates an InvalidArgument.
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    std::string ToString() const;

  private:
    // OK status has a NULL state_.  Otherwise, state_ is a new[] array
    // of the following form:
    //    state_[0..3] == length of message
    //    state_[4]    == code
    //    state_[5..]  == message
    const char *state_;
    // 类型
    enum Code
    {
      kOk = 0,
      kNotFound = 1,
      kCorruption = 2,
      kNotSupported = 3,
      kInvalidArgument = 4,
      kIOError = 5
    };

    // 从state_字符数组中获取到状态码
    Code code() const
    {
      return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
    }

    Status(Code code, const Slice &msg, const Slice &msg2);
    static const char *CopyState(const char *s);
  };

  // 从Status类实例s中获取其状态信息，并设置到自己的state_成员中
  inline Status::Status(const Status &s)
  {
    state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
  }

  // 实现了Status类的赋值操作。
  inline void Status::operator=(const Status &s)
  {
    // The following condition catches both aliasing (when this == &s),
    // and the common case where both s and *this are ok.
    if (state_ != s.state_)
    {
      delete[] state_;
      state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
    }
  }

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_STATUS_H_
