// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the leveldb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_H_

#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb
{

  class FileLock;
  class Logger;
  class RandomAccessFile;
  class SequentialFile;
  class Slice;
  class WritableFile;

  // Env类是一个抽象类，定义了一些和环境相关的接口
  class LEVELDB_EXPORT Env
  {
  public:
    Env() {}
    virtual ~Env();

    static Env *Default();

    virtual Status NewSequentialFile(const std::string &fname,
                                     SequentialFile **result) = 0;

    virtual Status NewRandomAccessFile(const std::string &fname,
                                       RandomAccessFile **result) = 0;

    virtual Status NewWritableFile(const std::string &fname,
                                   WritableFile **result) = 0;

    virtual Status NewAppendableFile(const std::string &fname,
                                     WritableFile **result);

    virtual bool FileExists(const std::string &fname) = 0;

    virtual Status GetChildren(const std::string &dir,
                               std::vector<std::string> *result) = 0;

    virtual Status DeleteFile(const std::string &fname) = 0;

    virtual Status CreateDir(const std::string &dirname) = 0;

    virtual Status DeleteDir(const std::string &dirname) = 0;

    virtual Status GetFileSize(const std::string &fname, uint64_t *file_size) = 0;

    virtual Status RenameFile(const std::string &src,
                              const std::string &target) = 0;

    virtual Status LockFile(const std::string &fname, FileLock **lock) = 0;

    virtual Status UnlockFile(FileLock *lock) = 0;

    virtual void Schedule(
        void (*function)(void *arg),
        void *arg) = 0;

    virtual void StartThread(void (*function)(void *arg), void *arg) = 0;

    virtual Status GetTestDirectory(std::string *path) = 0;

    virtual Status NewLogger(const std::string &fname, Logger **result) = 0;

    virtual uint64_t NowMicros() = 0;

    virtual void SleepForMicroseconds(int micros) = 0;

  private:
    // No copying allowed
    Env(const Env &);
    void operator=(const Env &);
  };

  // A file abstraction for reading sequentially through a file
  class LEVELDB_EXPORT SequentialFile
  {
  public:
    SequentialFile() {}
    virtual ~SequentialFile();

    virtual Status Read(size_t n, Slice *result, char *scratch) = 0;

    virtual Status Skip(uint64_t n) = 0;

  private:
    SequentialFile(const SequentialFile &);
    void operator=(const SequentialFile &);
  };

  class LEVELDB_EXPORT RandomAccessFile
  {
  public:
    RandomAccessFile() {}
    virtual ~RandomAccessFile();

    // Read up to "n" bytes from the file starting at "offset".
    // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
    // to the data that was read (including if fewer than "n" bytes were
    // successfully read).  May set "*result" to point at data in
    // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
    // "*result" is used.  If an error was encountered, returns a non-OK
    // status.
    //
    // Safe for concurrent use by multiple threads.
    virtual Status Read(uint64_t offset, size_t n, Slice *result,
                        char *scratch) const = 0;

  private:
    // No copying allowed
    RandomAccessFile(const RandomAccessFile &);
    void operator=(const RandomAccessFile &);
  };

  // A file abstraction for sequential writing.  The implementation
  // must provide buffering since callers may append small fragments
  // at a time to the file.
  // WritableFile是一个抽象类，指明了写文件操作应该实现的接口。
  class LEVELDB_EXPORT WritableFile
  {
  public:
    WritableFile() {}
    virtual ~WritableFile();

    // Append()函数用于往内部缓冲区中写入数据
    virtual Status Append(const Slice &data) = 0;
    virtual Status Close() = 0;

    // Flush()函数用于将内部缓冲区的内容刷新到磁盘中，当然可能并没有
    // 直接到磁盘中，而是会在操作系统维护的文件缓冲区中。
    virtual Status Flush() = 0;
    virtual Status Sync() = 0;

  private:
    // No copying allowed
    WritableFile(const WritableFile &);
    void operator=(const WritableFile &);
  };

  // An interface for writing log messages.
  class LEVELDB_EXPORT Logger
  {
  public:
    Logger() {}
    virtual ~Logger();

    // Write an entry to the log file with the specified format.
    virtual void Logv(const char *format, va_list ap) = 0;

  private:
    // No copying allowed
    Logger(const Logger &);
    void operator=(const Logger &);
  };

  // Identifies a locked file.
  class LEVELDB_EXPORT FileLock
  {
  public:
    FileLock() {}
    virtual ~FileLock();

  private:
    // No copying allowed
    FileLock(const FileLock &);
    void operator=(const FileLock &);
  };

  // Log the specified data to *info_log if info_log is non-NULL.
  extern void Log(Logger *info_log, const char *format, ...)
#if defined(__GNUC__) || defined(__clang__)
      __attribute__((__format__(__printf__, 2, 3)))
#endif
      ;

  // A utility routine: write "data" to the named file.
  LEVELDB_EXPORT Status WriteStringToFile(Env *env, const Slice &data,
                                          const std::string &fname);

  // A utility routine: read contents of named file into *data
  LEVELDB_EXPORT Status ReadFileToString(Env *env, const std::string &fname,
                                         std::string *data);

  // An implementation of Env that forwards all calls to another Env.
  // May be useful to clients who wish to override just part of the
  // functionality of another Env.
  class LEVELDB_EXPORT EnvWrapper : public Env
  {
  public:
    // Initialize an EnvWrapper that delegates all calls to *t
    explicit EnvWrapper(Env *t) : target_(t) {}
    virtual ~EnvWrapper();

    // Return the target to which this Env forwards all calls
    Env *target() const { return target_; }

    // The following text is boilerplate that forwards all methods to target()
    Status NewSequentialFile(const std::string &f, SequentialFile **r)
    {
      return target_->NewSequentialFile(f, r);
    }
    Status NewRandomAccessFile(const std::string &f, RandomAccessFile **r)
    {
      return target_->NewRandomAccessFile(f, r);
    }
    Status NewWritableFile(const std::string &f, WritableFile **r)
    {
      return target_->NewWritableFile(f, r);
    }
    Status NewAppendableFile(const std::string &f, WritableFile **r)
    {
      return target_->NewAppendableFile(f, r);
    }
    bool FileExists(const std::string &f) { return target_->FileExists(f); }
    Status GetChildren(const std::string &dir, std::vector<std::string> *r)
    {
      return target_->GetChildren(dir, r);
    }
    Status DeleteFile(const std::string &f) { return target_->DeleteFile(f); }
    Status CreateDir(const std::string &d) { return target_->CreateDir(d); }
    Status DeleteDir(const std::string &d) { return target_->DeleteDir(d); }
    Status GetFileSize(const std::string &f, uint64_t *s)
    {
      return target_->GetFileSize(f, s);
    }
    Status RenameFile(const std::string &s, const std::string &t)
    {
      return target_->RenameFile(s, t);
    }
    Status LockFile(const std::string &f, FileLock **l)
    {
      return target_->LockFile(f, l);
    }
    Status UnlockFile(FileLock *l) { return target_->UnlockFile(l); }
    void Schedule(void (*f)(void *), void *a)
    {
      return target_->Schedule(f, a);
    }
    void StartThread(void (*f)(void *), void *a)
    {
      return target_->StartThread(f, a);
    }
    virtual Status GetTestDirectory(std::string *path)
    {
      return target_->GetTestDirectory(path);
    }
    virtual Status NewLogger(const std::string &fname, Logger **result)
    {
      return target_->NewLogger(fname, result);
    }
    uint64_t NowMicros()
    {
      return target_->NowMicros();
    }
    void SleepForMicroseconds(int micros)
    {
      target_->SleepForMicroseconds(micros);
    }

  private:
    Env *target_;
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_ENV_H_
