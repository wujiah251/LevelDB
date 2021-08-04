// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb
{

  namespace log
  {
    class Writer;
  }

  class Compaction;
  class Iterator;
  class MemTable;
  class TableBuilder;
  class TableCache;
  class Version;
  class VersionSet;
  class WritableFile;

  extern int FindFile(const InternalKeyComparator &icmp,
                      const std::vector<FileMetaData *> &files,
                      const Slice &key);

  extern bool SomeFileOverlapsRange(
      const InternalKeyComparator &icmp,
      bool disjoint_sorted_files,
      const std::vector<FileMetaData *> &files,
      const Slice *smallest_user_key,
      const Slice *largest_user_key);

  class Version
  {
  public:
    void AddIterators(const ReadOptions &, std::vector<Iterator *> *iters);

    struct GetStats
    {
      FileMetaData *seek_file;
      int seek_file_level;
    };
    Status Get(const ReadOptions &, const LookupKey &key, std::string *val,
               GetStats *stats);

    bool UpdateStats(const GetStats &stats);

    bool RecordReadSample(Slice key);

    void Ref();
    void Unref();
    // GetOverlappingInputs()用于将level层的所有和key值范围[begin,end]有
    // 重叠的sstable文件对应的文件元信息对象收集起来存放到inputs数组中。
    void GetOverlappingInputs(
        int level,
        const InternalKey *begin, // NULL means before all keys
        const InternalKey *end,   // NULL means after all keys
        std::vector<FileMetaData *> *inputs);

    bool OverlapInLevel(int level,
                        const Slice *smallest_user_key,
                        const Slice *largest_user_key);

    int PickLevelForMemTableOutput(const Slice &smallest_user_key,
                                   const Slice &largest_user_key);

    int NumFiles(int level) const { return files_[level].size(); }

    std::string DebugString() const;

  private:
    friend class Compaction;
    friend class VersionSet;

    class LevelFileNumIterator;
    Iterator *NewConcatenatingIterator(const ReadOptions &, int level) const;

    void ForEachOverlapping(Slice user_key, Slice internal_key,
                            void *arg,
                            bool (*func)(void *, int, FileMetaData *));

    // vset_是对应的VersionSet类实例
    VersionSet *vset_;

    // next_和prev_用于将多个Version类实例以链表方式管理起来
    Version *next_;
    Version *prev_;

    // refs_是版本的引用计数。
    int refs_;

    // List of files per level
    // files_存放了该版本中每一个层级的所有sstable文件对应的文件元信息对象
    std::vector<FileMetaData *> files_[config::kNumLevels];

    // 存放将要进行compact的sstable文件对应的文件元信息对象及所在的level。
    FileMetaData *file_to_compact_;
    int file_to_compact_level_;

    // compact的分值以及对应的需要进行compact的level。
    double compaction_score_;
    int compaction_level_;

    explicit Version(VersionSet *vset)
        : vset_(vset), next_(this), prev_(this), refs_(0),
          file_to_compact_(NULL),
          file_to_compact_level_(-1),
          compaction_score_(-1),
          compaction_level_(-1)
    {
    }

    ~Version();

    Version(const Version &);
    void operator=(const Version &);
  };

  class VersionSet
  {
  public:
    VersionSet(const std::string &dbname,
               const Options *options,
               TableCache *table_cache,
               const InternalKeyComparator *);
    ~VersionSet();

    Status LogAndApply(VersionEdit *edit, port::Mutex *mu)
        EXCLUSIVE_LOCKS_REQUIRED(mu);

    Status Recover(bool *save_manifest);

    Version *current() const { return current_; }

    uint64_t ManifestFileNumber() const { return manifest_file_number_; }

    uint64_t NewFileNumber() { return next_file_number_++; }

    void ReuseFileNumber(uint64_t file_number)
    {
      if (next_file_number_ == file_number + 1)
      {
        next_file_number_ = file_number;
      }
    }

    int NumLevelFiles(int level) const;

    int64_t NumLevelBytes(int level) const;

    uint64_t LastSequence() const { return last_sequence_; }

    void SetLastSequence(uint64_t s)
    {
      assert(s >= last_sequence_);
      last_sequence_ = s;
    }

    // Mark the specified file number as used.
    void MarkFileNumberUsed(uint64_t number);

    uint64_t LogNumber() const { return log_number_; }

    uint64_t PrevLogNumber() const { return prev_log_number_; }

    Compaction *PickCompaction();

    // Return a compaction object for compacting the range [begin,end] in
    // the specified level.  Returns NULL if there is nothing in that
    // level that overlaps the specified range.  Caller should delete
    // the result.
    Compaction *CompactRange(
        int level,
        const InternalKey *begin,
        const InternalKey *end);

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    int64_t MaxNextLevelOverlappingBytes();

    // Create an iterator that reads over the compaction inputs for "*c".
    // The caller should delete the iterator when no longer needed.
    Iterator *MakeInputIterator(Compaction *c);

    // Returns true iff some level needs a compaction.
    bool NeedsCompaction() const
    {
      Version *v = current_;
      return (v->compaction_score_ >= 1) || (v->file_to_compact_ != NULL);
    }

    // Add all files listed in any live version to *live.
    // May also mutate some internal state.
    void AddLiveFiles(std::set<uint64_t> *live);

    // Return the approximate offset in the database of the data for
    // "key" as of version "v".
    uint64_t ApproximateOffsetOf(Version *v, const InternalKey &key);

    // Return a human-readable short (single-line) summary of the number
    // of files per level.  Uses *scratch as backing store.
    struct LevelSummaryStorage
    {
      char buffer[100];
    };
    const char *LevelSummary(LevelSummaryStorage *scratch) const;

  private:
    class Builder;

    friend class Compaction;
    friend class Version;

    bool ReuseManifest(const std::string &dscname, const std::string &dscbase);

    void Finalize(Version *v);

    void GetRange(const std::vector<FileMetaData *> &inputs,
                  InternalKey *smallest,
                  InternalKey *largest);

    void GetRange2(const std::vector<FileMetaData *> &inputs1,
                   const std::vector<FileMetaData *> &inputs2,
                   InternalKey *smallest,
                   InternalKey *largest);

    void SetupOtherInputs(Compaction *c);

    // Save current contents to *log
    Status WriteSnapshot(log::Writer *log);

    void AppendVersion(Version *v);

    Env *const env_;
    const std::string dbname_;
    const Options *const options_;
    TableCache *const table_cache_;
    const InternalKeyComparator icmp_;
    uint64_t next_file_number_;
    uint64_t manifest_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;
    uint64_t prev_log_number_; // 0 or backing store for memtable being compacted

    // Opened lazily
    WritableFile *descriptor_file_;
    log::Writer *descriptor_log_;
    // 双向链表的第一个节点
    Version dummy_versions_; // Head of circular doubly-linked list of versions.
    Version *current_;       // == dummy_versions_.prev_

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    // 为了尽量均匀compact每个层级，所以会将这一次compact的end-key作为下一次
    // compact的start-key，compact_pointers_就保存了每一个level下一次compact的
    // start-key。
    std::string compact_pointer_[config::kNumLevels];

    // No copying allowed
    VersionSet(const VersionSet &);
    void operator=(const VersionSet &);
  };

  class Compaction
  {
  public:
    ~Compaction();

    int level() const { return level_; }

    VersionEdit *edit() { return &edit_; }

    int num_input_files(int which) const { return inputs_[which].size(); }

    FileMetaData *input(int which, int i) const { return inputs_[which][i]; }

    uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

    bool IsTrivialMove() const;

    void AddInputDeletions(VersionEdit *edit);

    bool IsBaseLevelForKey(const Slice &user_key);

    bool ShouldStopBefore(const Slice &internal_key);

    void ReleaseInputs();

  private:
    friend class Version;
    friend class VersionSet;

    Compaction(const Options *options, int level);

    int level_;
    uint64_t max_output_file_size_;
    // 输入版本
    Version *input_version_;
    VersionEdit edit_;

    std::vector<FileMetaData *> inputs_[2]; // The two sets of inputs

    std::vector<FileMetaData *> grandparents_;
    size_t grandparent_index_; // Index in grandparent_starts_
    bool seen_key_;            // Some output key has been seen
    int64_t overlapped_bytes_; // Bytes of overlap between current output
                               // and grandparent files

    size_t level_ptrs_[config::kNumLevels];
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_VERSION_SET_H_
