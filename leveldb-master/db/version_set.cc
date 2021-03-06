// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb
{

  // TargetFileSize()用于获取一个文件大小，这个文件大小是从Option实例中获取到的。
  static int TargetFileSize(const Options *options)
  {
    return options->max_file_size;
  }

  // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
  // stop building a single file in a level->level+1 compaction.
  // MaxGrandParentOverlapBytes()计算level+2层级和此次进行compact的key值范围重叠的
  // 最大字节数。
  static int64_t MaxGrandParentOverlapBytes(const Options *options)
  {
    return 10 * TargetFileSize(options);
  }

  // Maximum number of bytes in all compacted files.  We avoid expanding
  // the lower level file set of a compaction if it would make the
  // total compaction cover more than this many bytes.
  static int64_t ExpandedCompactionByteSizeLimit(const Options *options)
  {
    return 25 * TargetFileSize(options);
  }

  // MaxBytesForLevel()计算每个层级最大可以有的字节数
  static double MaxBytesForLevel(const Options *options, int level)
  {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    double result = 10. * 1048576.0;
    while (level > 1)
    {
      result *= 10;
      level--;
    }
    return result;
  }

  // MaxFileSizeForLevel()计算层级中每个文件的最大大小。
  static uint64_t MaxFileSizeForLevel(const Options *options, int level)
  {
    // We could vary per level to reduce number of files?
    return TargetFileSize(options);
  }

  // TotalFileSize()函数用于计算files动态数组中存放的文件元信息对应的文件总大小。
  static int64_t TotalFileSize(const std::vector<FileMetaData *> &files)
  {
    int64_t sum = 0;
    for (size_t i = 0; i < files.size(); i++)
    {
      sum += files[i]->file_size;
    }
    return sum;
  }

  Version::~Version()
  {
    assert(refs_ == 0);

    // Remove from linked list
    prev_->next_ = next_;
    next_->prev_ = prev_;

    // Drop references to files
    for (int level = 0; level < config::kNumLevels; level++)
    {
      for (size_t i = 0; i < files_[level].size(); i++)
      {
        FileMetaData *f = files_[level][i];
        assert(f->refs > 0);
        f->refs--;
        if (f->refs <= 0)
        {
          delete f;
        }
      }
    }
  }

  // 采用二分查找算法找到所存放的最大key值不小于参数key值的第一个文件元信息对象
  // 在files数组中的下标。这里的"第一个"的意思就是值在files数组中所有最大key值
  // 不小于参数key的元素中下标最小的那个。这个查找的结果也不一定就能说明参数key
  // 一定会落在最终找到的那个文件中。我们知道虽然在一个层级(非0层)中所有文件的
  // key是有序的，递增的，但文件和文件之间的key值是不连续的，如下图所示：
  // |  files0 |    |  files1    |  | files2   | files3 |
  // ----------------------------------------------------
  // 假设上面的图中区间就是对应文件元信息存放的key值范围，从图中可以看到文件
  // 和文件之间有可能存在部分key空间是不在两个文件中，如果某个key值不小于files2的
  // 最大key值，而又大于files1的最大key值，那么FindFile()函数返回的文件元信息下标
  // 就是图中files2对应的下标，但是不能说明参数key就一定会落在files2中，有可能会
  // 在files1和files2之间那部分的空白key空间中。
  int FindFile(const InternalKeyComparator &icmp,
               const std::vector<FileMetaData *> &files,
               const Slice &key)
  {
    uint32_t left = 0;

    // right初始存放的是files中文件元信息对象的个数。
    uint32_t right = files.size();

    // 采用二分查找算法找到所存放的最大key值不小于参数key值的第一个文件元信息对象
    // 在files数组中的下标。这里的"第一个"的意思就是值在files数组中所有最大key值
    // 不小于参数key的元素中下标最小的那个。这个查找的结果也不一定就能说明参数key
    // 一定会落在最终找到的那个文件中。我们知道虽然在一个层级(非0层)中所有文件的
    // key是有序的，递增的，但文件和文件之间的key值是不连续的，如下图所示：
    //
    // |  files0 |    |  files1    |  | files2   | files3 |
    // ----------------------------------------------------

    // 这里为什么采用二分查找算法呢？因为在一个层级(非0层)中，所有的sstable文件
    // 中的key都是不重合的，并且是递增的，所以在这种有序状态下可以采用二分法
    // 提高查找效率。
    while (left < right)
    {
      uint32_t mid = (left + right) / 2;
      const FileMetaData *f = files[mid];

      // 和f的最大key值比较，如果比最大key值大，说明key不可能落在f及f之前的那些文件中
      // 那么从f后一个文件继续查找。
      if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0)
      {
        left = mid + 1;
      }
      else
      {
        right = mid;
      }
    }
    return right;
  }

  // AfterFile()函数用于判断user_key是不是会落在文件元信息对象f对应的sstable文件后面，
  // 换句话说就是判断user_key是不是比f对应的sstable文件的最大key值还要大，如果是，那么
  // 就返回true;否则，返回false。
  static bool AfterFile(const Comparator *ucmp,
                        const Slice *user_key, const FileMetaData *f)
  {
    // NULL user_key occurs before all keys and is therefore never after *f
    return (user_key != NULL &&
            ucmp->Compare(*user_key, f->largest.user_key()) > 0);
  }

  // BeforeFile()函数用于判断user_key是不是会落在文件元信息f对应的sstable文件前面，
  // 换句话说就是判断user_key是不是比f对应的sstable文件中的最小key值还要小，如果是，那么
  // 就返回true;否则，返回false。
  static bool BeforeFile(const Comparator *ucmp,
                         const Slice *user_key, const FileMetaData *f)
  {
    // NULL user_key occurs after all keys and is therefore never before *f
    return (user_key != NULL &&
            ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
  }

  // SomeFileOverlapsRange()函数用于判断files文件中是否有文件的key值范围
  // 和[*smallest_user_key, *largest_user_key]有重叠，如果有的话，那么就返回
  // true。
  bool SomeFileOverlapsRange(
      const InternalKeyComparator &icmp,
      bool disjoint_sorted_files,
      const std::vector<FileMetaData *> &files,
      const Slice *smallest_user_key,
      const Slice *largest_user_key)
  {
    const Comparator *ucmp = icmp.user_comparator();

    // disjoint_sorted_files为false的话，需要检查第0层的所有文件。
    // 而第0层的文件由于互相之间有重叠，所以需要检查所有文件。
    if (!disjoint_sorted_files)
    {
      // Need to check against all files
      for (size_t i = 0; i < files.size(); i++)
      {
        const FileMetaData *f = files[i];

        // 如果smallest_user_key在f之后，或者largest_user_key在f之前，
        // 那么说明f和[*smallest_user_key, *largest_user_key]没有重叠
        // 否则的话，有重叠返回true。
        if (AfterFile(ucmp, smallest_user_key, f) ||
            BeforeFile(ucmp, largest_user_key, f))
        {
          // No overlap
        }
        else
        {
          return true; // Overlap
        }
      }
      return false;
    }

    // Binary search over file list
    // 因为从第1层开始，每一层的所有sstable文件的key是没有重叠的，所以
    // 可以用二分法来查找可能和[*smallest_user_key, *largest_user_key]有
    // 重叠的文件。
    uint32_t index = 0;

    // 这里用smallest_user_key通过调用FindFile()到每一层的所有sstable中进行查找，
    // FindFile()会采用二分查找算法找到所存放的最大key值不小于smallest_user_key的
    // 第一个文件元信息对象。如果找到了这样的文件元信息对象，说明其对应的文件可能
    // 会和[*smallest_user_key, *largest_user_key]有重叠，但还需要进一步判断，即通过
    // 判断largest_user_key是否比该文件存放的最小key值还小，如果小的话，说明该文件
    // 其实和[*smallest_user_key, *largest_user_key]并没有重叠;如果大的话，说明
    // 该文件和[*smallest_user_key, *largest_user_key]是有重叠部分的。举例如下：
    //
    // |  files0 |    |  files1    |  | files2   | files3 |
    // ----------------------------------------------------
    // 假设通过FindFile()找到的文件元信息对象是files2的，那么[*smallest_user_key,
    // *largest_user_key]范围此时可能在(files1.max_key, files2.max_key]之间，所以
    // 需要进一步看是否[*smallest_user_key, *largest_user_key]会落在(files1.max_key,
    // files2.min_key)之间，如果是的话，说明其实并没有重叠。

    if (smallest_user_key != NULL)
    {
      // Find the earliest possible internal key for smallest_user_key
      InternalKey small(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
      index = FindFile(icmp, files, small.Encode());
    }

    if (index >= files.size())
    {
      // beginning of range is after all files, so no overlap.
      return false;
    }

    return !BeforeFile(ucmp, largest_user_key, files[index]);
  }

  // Version::LevelFileNumIterator类是一个迭代器的实现类，用于迭代一个
  // 存放这文件元信息对象的动态数组，在实际使用的时候通常用于迭代某个
  // level中的所有sstable对应的文件元信息对象。
  class Version::LevelFileNumIterator : public Iterator
  {
  public:
    LevelFileNumIterator(const InternalKeyComparator &icmp,
                         const std::vector<FileMetaData *> *flist)
        : icmp_(icmp),
          flist_(flist),
          index_(flist->size())
    { // Marks as invalid
    }
    virtual bool Valid() const
    {
      return index_ < flist_->size();
    }

    // Seek()接口用于使迭代器指向所存放的最大key值大于等于target的
    // 第一个文件元信息对象，这里的"第一个"的意思就是值在files数组中
    // 所有最大key值不小于target的元素中下标最小的那个。
    virtual void Seek(const Slice &target)
    {
      index_ = FindFile(icmp_, *flist_, target);
    }

    // SeekToFirst()使迭代器指向flist_数组中的第一个文件元信息对象。
    virtual void SeekToFirst() { index_ = 0; }

    // SeekToLast()使迭代器指向flist_数组中的最后一个文件元信息对象。
    virtual void SeekToLast()
    {
      index_ = flist_->empty() ? 0 : flist_->size() - 1;
    }

    // Next()使迭代器指向flist_数组中的下一个文件元信息对象，具体做法
    // 就是将数组的索引加1.
    virtual void Next()
    {
      assert(Valid());
      index_++;
    }

    // Prev()使迭代器指向flist_数组中的前一个文件元信息对象，具体做法就是
    // 将数组索引减1.如果在减1之前就已经指向第一个文件元信息对象的话，那么
    // 执行Prev()就应该是一个无效的迭代器了，所以这里将索引设置成flist_->size()。
    // 因为索引的有效范围是[0, flist_->size() -1]。
    virtual void Prev()
    {
      assert(Valid());
      if (index_ == 0)
      {
        index_ = flist_->size(); // Marks as invalid
      }
      else
      {
        index_--;
      }
    }

    // key()方法用于获取迭代器指向的文件元信息对象中存放的最大key值。
    Slice key() const
    {
      assert(Valid());
      return (*flist_)[index_]->largest.Encode();
    }

    // value()方法用于获取迭代器指向的文件元信息对象中存放的FileNumber和
    // file size，并编码在一个16字节的字符数组中。
    Slice value() const
    {
      assert(Valid());
      EncodeFixed64(value_buf_, (*flist_)[index_]->number);
      EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
      return Slice(value_buf_, sizeof(value_buf_));
    }
    virtual Status status() const { return Status::OK(); }

  private:
    // 比较器。
    const InternalKeyComparator icmp_;

    // 存放文件元信息对象的动态数组。
    const std::vector<FileMetaData *> *const flist_;

    // 存放文件元信息对象的动态数组的索引。
    uint32_t index_;

    // Backing store for value().  Holds the file number and size.
    // 用于存放LevelFileNumberIterator迭代器的值信息，见value()方法。
    mutable char value_buf_[16];
  };

  // GetFileIterator()函数用于获取某个指定sstable文件的迭代器，这个迭代器
  // 可以用于获取sstable文件中的key-value信息，其实这个迭代器本身也是一个
  // 二级迭代器。
  static Iterator *GetFileIterator(void *arg,
                                   const ReadOptions &options,
                                   const Slice &file_value)
  {
    // arg参数存放的是上层调用者传入的参数，在这里是一个TableCache类实例，
    // 可以参考下面的Version::NewConcatentingIterator()方法。
    TableCache *cache = reinterpret_cast<TableCache *>(arg);
    if (file_value.size() != 16)
    {
      return NewErrorIterator(
          Status::Corruption("FileReader invoked with unexpected value"));
    }
    else
    {
      // file_value中存放了目标sstable文件的FileNumber和file size，这两个信息
      // 在LevelFileNumberIterator迭代器的value()方法中可以获取到。然后根据
      // 这两个信息可以获取到TableCache类实例的迭代器。
      return cache->NewIterator(options,
                                DecodeFixed64(file_value.data()),
                                DecodeFixed64(file_value.data() + 8));
    }
  }

  // NewConcatenatingIterator()方法用于获取一个二级迭代器，这个二级迭代器可以用于
  // 迭代器某一个层级(level>0)中的所有sstable，并从中获取到key-value信息。当然key需要
  // 由上层调用者传入，否则内部也不知道上层调用者需要Get什么key的value。
  Iterator *Version::NewConcatenatingIterator(const ReadOptions &options,
                                              int level) const
  {
    // GetFileIterator是内层迭代器
    return NewTwoLevelIterator(
        new LevelFileNumIterator(vset_->icmp_, &files_[level]),
        &GetFileIterator, vset_->table_cache_, options);
  }

  // AddIterators()方法用于某个版本(Version)中所有sstable的迭代器，这样就可以
  // 通过获取到的迭代器数组依次获取这个版本中的key-value信息。我们知道，level0
  // 中的sstable之间可能存在key重叠的情况，所以对level0中的所有sstable文件作为
  // 独立个体分别创建一个迭代器。而其余level在同level的sstable文件之间是不存在
  // key重叠的，所以可以通过NewConcatenatingIterator来创建一个对于整个level的
  // 迭代器。
  void Version::AddIterators(const ReadOptions &options,
                             std::vector<Iterator *> *iters)
  {
    for (size_t i = 0; i < files_[0].size(); i++)
    {
      iters->push_back(
          vset_->table_cache_->NewIterator(
              options, files_[0][i]->number, files_[0][i]->file_size));
    }

    for (int level = 1; level < config::kNumLevels; level++)
    {
      if (!files_[level].empty())
      {
        iters->push_back(NewConcatenatingIterator(options, level));
      }
    }
  }

  // Callback from TableCache::Get()
  namespace
  {

    // SaverState枚举用来标识查询key的结果状态。
    enum SaverState
    {
      kNotFound, // 没找到
      kFound,    // 找到了
      kDeleted,  // 删除了
      kCorrupt,  // 不正确
    };

    // struct Saver结构体用来保存user_key对应的value信息。
    struct Saver
    {
      SaverState state;
      const Comparator *ucmp;
      Slice user_key;
      std::string *value;
    };
  }

  // SaveValue()函数一般用作TableCache::Get()方法的最后一个参数，用于保存
  // 从sstable中查找user_key的结果。
  static void SaveValue(void *arg, const Slice &ikey, const Slice &v)
  {
    Saver *s = reinterpret_cast<Saver *>(arg);
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(ikey, &parsed_key))
    {
      s->state = kCorrupt;
    }
    else
    {
      // 如果parsed_key.user_key等于s->user_key，说明找到了这个key记录
      // 然后根据parsed_key.type判断值是否有效，有效的话，就保存到s->value中。
      if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0)
      {
        s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
        if (s->state == kFound)
        {
          s->value->assign(v.data(), v.size());
        }
      }
    }
  }

  static bool NewestFirst(FileMetaData *a, FileMetaData *b)
  {
    return a->number > b->number;
  }

  // ForEachOverlapping()方法用于对user_key所落在的sstable文件对应的文件元信息对象
  // 执行一个func操作，并根据函数func的返回值判断是否需要对user_key所落在的其他
  // 文件元信息对象执行相同的操作，如果不需要，则直接返回。
  void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                   void *arg,
                                   bool (*func)(void *, int, FileMetaData *))
  {
    // TODO(sanjay): Change Version::Get() to use this function.
    const Comparator *ucmp = vset_->icmp_.user_comparator();

    // 首先是从0层(level 0)开始，由于level 0中的sstable文件之间可能存在key重叠，
    // 所以在处理level 0的时候需要遍历该层中的所有文件，将包含了user_key的所有
    // sstable文件对应的文件元信息对象收集起来。然后对于目标集合，依次调用func
    // 并根据func的返回值判断是否需要返回。
    std::vector<FileMetaData *> tmp;
    tmp.reserve(files_[0].size());
    for (uint32_t i = 0; i < files_[0].size(); i++)
    {
      FileMetaData *f = files_[0][i];
      if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
          ucmp->Compare(user_key, f->largest.user_key()) <= 0)
      {
        tmp.push_back(f);
      }
    }
    if (!tmp.empty())
    {
      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      for (uint32_t i = 0; i < tmp.size(); i++)
      {
        if (!(*func)(arg, 0, tmp[i]))
        {
          return;
        }
      }
    }

    // 如果在level 0中没有包含了user_key的sstable，或者对于找到的满足条件的
    // sstable文件，func返回值并没有说要提前返回，那么就会继续搜寻更高层的
    // sstable文件，由于更高层的sstable文件同层级之间不存在key重叠，所以
    // 一个层级如果存在包含了user_key的文件的话，那么这个文件也是唯一的。
    // 那么就对这个文件对应的文件元信息对象执行func操作，并根据返回值判断
    // 是否需要从更高层中继续搜寻。以此类推。
    for (int level = 1; level < config::kNumLevels; level++)
    {
      size_t num_files = files_[level].size();
      if (num_files == 0)
        continue;

      // Binary search to find earliest index whose largest key >= internal_key.
      uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
      if (index < num_files)
      {
        FileMetaData *f = files_[level][index];
        if (ucmp->Compare(user_key, f->smallest.user_key()) < 0)
        {
          // All of "f" is past any data for user_key
        }
        else
        {
          if (!(*func)(arg, level, f))
          {
            return;
          }
        }
      }
    }
  }

  // Get()方法用于从当前版本中获取key值k对应的value值，并设置访问统计。
  Status Version::Get(const ReadOptions &options,
                      const LookupKey &k,
                      std::string *value,
                      GetStats *stats)
  {

    // 从LookupKey对象中获取到internal key和user key。
    Slice ikey = k.internal_key();
    Slice user_key = k.user_key();
    const Comparator *ucmp = vset_->icmp_.user_comparator();
    Status s;

    // 初始化访问统计对象
    stats->seek_file = NULL;
    stats->seek_file_level = -1;
    FileMetaData *last_file_read = NULL;
    int last_file_read_level = -1;

    std::vector<FileMetaData *> tmp;
    FileMetaData *tmp2;
    // 依次从每个层级中查找key值对应的value。
    for (int level = 0; level < config::kNumLevels; level++)
    {
      size_t num_files = files_[level].size();
      // 如果这个层级中没有sstable文件，那么接着处理下一个层级。
      if (num_files == 0)
        continue;
      // 获取当前处理层级中所有sstable文件对应的文件元信息对象数组的首地址。
      FileMetaData *const *files = &files_[level][0];

      // 如果是level 0的话，需要做特殊处理，因为level 0中的sstable文件之间可能存在
      // key重叠，那么首先需要将所有包含了待查找key的sstabl文件对应的文件元信息对象
      // 收集起来，并按照FileNumber进行排序，FileNumber大的，对应的文件元信息对象就
      // 更新，排在前面。并将排序完之后的结果设置到files中等待后续处理。
      if (level == 0)
      {
        tmp.reserve(num_files);
        for (uint32_t i = 0; i < num_files; i++)
        {
          FileMetaData *f = files[i];
          if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
              ucmp->Compare(user_key, f->largest.user_key()) <= 0)
          {
            tmp.push_back(f);
          }
        }
        if (tmp.empty())
          continue;

        std::sort(tmp.begin(), tmp.end(), NewestFirst);
        files = &tmp[0];
        num_files = tmp.size();
      }
      else
      {
        // 这个分支用于处理大于level 0的所有其他level。利用FindFile()做
        // 二分查找，如果没有找到包含了待查找key值的sstable文件，那么就将
        // files设置为NULL，对应的num_files为0，这样后面看到num_files为0，
        // 也不会做处理。如果包含了的话，就将这个文件对应的文件元信息对象
        // 设置到files中，并将num_files设置为1，因为对于高于level 0的其他
        // level来说，如果某个层级包含了某个key，那么这个一定只会落在其中
        // 的一个sstable文件中。
        uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
        if (index >= num_files)
        {
          files = NULL;
          num_files = 0;
        }
        else
        {
          tmp2 = files[index];
          if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0)
          {
            // All of "tmp2" is past any data for user_key
            files = NULL;
            num_files = 0;
          }
          else
          {
            files = &tmp2;
            num_files = 1;
          }
        }
      }

      // 对于上面流程中找到的包含了待查找key值的sstable文件对应的文件元信息对象，
      // 从文件元信息对象中获取到对应的sstable文件的FileNumber和file size，接着
      // 调用TableCache实例set_->table_cache_的Get()方法从该sstable文件中找到
      // key对应的value值。TableCache类的Get()方法会利用SaveValue()回调函数来
      // 保存获取到的value值。
      for (uint32_t i = 0; i < num_files; ++i)
      {

        // 如果last_file_read不为NULL，说明之前已经搜寻过了一个或者多个sstable文件，
        // 但是都没有找到符合条件的key-value记录。如果stats->seek_file等于NULL,说明
        // 在之前的搜寻过程中并没有设置过访问统计。这两个条件结合在一起是为了约束
        // 这样的一个条件，即对于本次的读操作(Get)，已经执行了多次的查询，即搜寻了
        // 多个sstable文件，在这样的情况下，需要保存本次的读操作中被查询的第一个文件
        // 及其所在的level。
        if (last_file_read != NULL && stats->seek_file == NULL)
        {
          stats->seek_file = last_file_read;
          stats->seek_file_level = last_file_read_level;
        }

        // 保存本次即将被读取的文件到last_file_read中，及对应的level到
        // last_file_read_level。因为本次处理的文件对于下一次处理过程来说
        // 就是上一次被处理的文件了。
        FileMetaData *f = files[i];
        last_file_read = f;
        last_file_read_level = level;

        // 初始化Saver对象，并将最终用来存放value值的对象设置到saver.value中
        // 这样如果TableCache类的方法Get()如果找到了key对应的记录的话，就会将
        // 对应的value保存到saver.value中，这样就将value设置传递到了上层。
        Saver saver;
        saver.state = kNotFound;
        saver.ucmp = ucmp;
        saver.user_key = user_key;
        saver.value = value;

        // 调用TableCache类实例的Get()方法执行查询动作，如果找到了对应的key-value
        // 记录，那么就可以直接返回了。对于level 0来说，因为所有符合条件的sstable
        // 文件已经按照FileNumber进行排序，所以如果在FileNumber大(即新的)sstable
        // 文件中找到了记录，那么就不再从更旧的sstable文件中查找了，直接返回。
        s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                     ikey, &saver, SaveValue);
        if (!s.ok())
        {
          return s;
        }
        switch (saver.state)
        {
        case kNotFound:
          break; // Keep searching in other files
        case kFound:
          return s;
        case kDeleted:
          s = Status::NotFound(Slice()); // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
        }
      }
    }

    return Status::NotFound(Slice()); // Use an empty error message for speed
  }

  // UpdateStats()方法用于根据访问统计判断是否需要更新下一次compact的文件和level。
  bool Version::UpdateStats(const GetStats &stats)
  {
    FileMetaData *f = stats.seek_file;
    if (f != NULL)
    {
      f->allowed_seeks--; // f允许在进行compact前最多被访问的次数递减一次。
      // 如果f允许在进行compact前最多被访问的次数小于等于0，并且之前没有设置
      // 下一次compact的文件的话，那么就将f设置为下次进行compact的文件，并将
      // 该文件所在的level设置为下次进行compact的level。
      if (f->allowed_seeks <= 0 && file_to_compact_ == NULL)
      {
        file_to_compact_ = f;
        file_to_compact_level_ = stats.seek_file_level;
        return true;
      }
    }
    return false;
  }

  bool Version::RecordReadSample(Slice internal_key)
  {
    ParsedInternalKey ikey;
    // 调用ParseInternalKey()从internal key中解析出user key、sequence number和type，
    // 并存放到ParsedInternalKey对象ikey中。
    if (!ParseInternalKey(internal_key, &ikey))
    {
      return false;
    }

    // struct State是一个统计类，通过函数Match来对匹配的文件元信息对象做一些访问统计。
    // 这里的说的匹配可以是对于某个key来说，如果某个sstable包含了这个key，那么我们
    // 就说匹配了。
    struct State
    {

      // stats用于存放第一个匹配的文件元信息对象
      GetStats stats; // Holds first matching file
      int matches;    // matches是一个计数器，对匹配的文件元信息对象进行计数。

      // Match()方法用于对匹配的文件元信息对象做进一步处理，包括：
      // 1. 统计匹配的文件元信息对象个数。
      // 2. 保存第一个匹配的文件元信息对象及其对应的sstable文件所在的level。
      // 3. 返回匹配的文件元信息对象个数是否小于2的逻辑结果。调用Match()函数
      //    的地方会根据Match()函数的返回值来判断是否需要对其他匹配的文件元信息
      //    对象再调用Match()函数做处理，如果已经有两个文件元信息对象已经匹配了，
      //    那么就不再对其他匹配的文件元信息对象做处理了。
      static bool Match(void *arg, int level, FileMetaData *f)
      {
        State *state = reinterpret_cast<State *>(arg);
        state->matches++;

        // 保存第一个匹配的文件元信息对象。
        if (state->matches == 1)
        {
          // Remember first match.
          state->stats.seek_file = f;
          state->stats.seek_file_level = level;
        }
        return state->matches < 2;
      }
    };

    State state;
    state.matches = 0;

    // ForEachOverlapping()方法用于对user_key所落在的sstable文件对应的文件元信息对象
    // 执行一个State::Match操作，并根据该函数的返回值判断是否需要对user_key所落在的其他
    // 文件元信息对象执行相同的操作，如果不需要，则直接返回。
    ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

    // 如果state.matches >= 2，说明对于user_key来说，至少有两个sstable文件包含了这个key
    // 那么就调用UpdateStats()方法用于根据访问统计判断是否需要更新下一次compact的文件和level
    if (state.matches >= 2)
    {
      // 1MB cost is about 1 seek (see comment in Builder::Apply).
      return UpdateStats(state.stats);
    }
    return false;
  }

  void Version::Ref()
  {
    ++refs_;
  }

  void Version::Unref()
  {
    assert(this != &vset_->dummy_versions_);
    assert(refs_ >= 1);
    --refs_;
    if (refs_ == 0)
    {
      delete this;
    }
  }

  // SomeFileOverlapsRange()函数用于判断files文件中是否有文件的key值范围
  // 和[*smallest_user_key, *largest_user_key]有重叠，如果有的话，那么就返回
  // true。那么OverlapInLevel()方法的用途就是判断level层所在的sstable文件中
  // 是否有文件的key值范围和[*smallest_user_key, *largest_user_key]有重叠，
  // 如果有的话，就返回true；否则返回false。
  bool Version::OverlapInLevel(int level,
                               const Slice *smallest_user_key,
                               const Slice *largest_user_key)
  {
    return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                                 smallest_user_key, largest_user_key);
  }

  // PickLevelForMemTableOutput()方法用于给memtable选择一个合适的level
  // 作为memtable下沉为sstable的目标层数。
  int Version::PickLevelForMemTableOutput(
      const Slice &smallest_user_key,
      const Slice &largest_user_key)
  {
    int level = 0;

    // 首选判断在第0层是否有sstable文件和[smallest_user_key,largest_user_key]
    // 有重叠，如果有的话，那么就将第0层作为memtable下沉为sstable文件的目标层数。
    // 如果第0层没有的话，那么选择的依据有两个，假定目前迭代的层数为level(仍从0开始)：
    // 1. 如果该层的下一层(level + 1)中的sstable文件key值范围有和[smallest_user_key,
    //    largest_user_key]重叠，那么level层就作为目标层数。
    // 2. 如果第一个条件没有符合，但是该层的下两层(level + 2)中和[smallest_user_key,
    //    largest_user_key]key值范围有重叠的sstable文件的总大小大于下两层最大重叠字节数的
    //    话，那么也将level层作为目标层数。
    // 上面的处理流程，对于等0层来说，如果该层中有sstable文件的key值范围和目标范围重叠，
    // 或者该层本身没有sstable文件的key值范围和目标范围重叠，但是下一层或者下两层的sstable
    // 满足上面的条件，都会将第0层作为目标层数。
    if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key))
    {
      InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
      InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
      std::vector<FileMetaData *> overlaps;
      while (level < config::kMaxMemCompactLevel)
      {
        if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key))
        {
          break;
        }
        if (level + 2 < config::kNumLevels)
        {
          // Check that file does not overlap too many grandparent bytes.
          GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
          const int64_t sum = TotalFileSize(overlaps);
          if (sum > MaxGrandParentOverlapBytes(vset_->options_))
          {
            break;
          }
        }
        level++;
      }
    }
    return level;
  }

  // GetOverlappingInputs()用于将level层的所有和key值范围[begin,end]有
  // 重叠的sstable文件对应的文件元信息对象收集起来存放到inputs数组中。
  void Version::GetOverlappingInputs(
      int level,
      const InternalKey *begin,
      const InternalKey *end,
      std::vector<FileMetaData *> *inputs)
  {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    inputs->clear();
    Slice user_begin, user_end;
    // 从internal key中获取user key。
    if (begin != NULL)
    {
      user_begin = begin->user_key();
    }
    if (end != NULL)
    {
      user_end = end->user_key();
    }
    const Comparator *user_cmp = vset_->icmp_.user_comparator();

    // 遍历level层的所有sstable文件元信息对象，对于每一个文件元信息对象，
    // 获取到其中存放的最大和最小key值。如果最大的key值都比begin小，那么
    // 这个sstable肯定不会和[begin,end]有重叠；如果最小的key值都比end
    // 大，那么这个sstable肯定不会和[begin,end]有重叠；否则，这个sstable
    // 文件的key值返回和[begin,end]就会有重叠，保存这个sstable文件对应的
    // 文件元信息对象。
    // 对于第0层来说比较特殊，因为第0层的sstable文件的key值范围可能会互相
    // 重叠，这个时候如果某个sstable的最小key值比begin小的话，那么就用这个
    // 最小key值作为begin，然后清除已经收集的文件元信息对象，并从该层的第一
    // 个文件开始重新判断是否和这个新key值范围[begin(new),end]有重叠；如果
    // 某个sstable文件的最大key值比end大的话，那么就用这个最大key值作为end，
    // 然后清除已经收集的文件元信息对象，并从该层的第一个文件开始重新判断是否
    // 和这个新的key值范围有重叠。这样做的结果就是可能有一些和最原始的[begin,end]
    // 没有重叠的sstable文件对应的文件元信息对象也加入到了数组中。
    for (size_t i = 0; i < files_[level].size();)
    {
      FileMetaData *f = files_[level][i++];
      const Slice file_start = f->smallest.user_key();
      const Slice file_limit = f->largest.user_key();
      if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0)
      {
        // "f" is completely before specified range; skip it
      }
      else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0)
      {
        // "f" is completely after specified range; skip it
      }
      else
      {
        inputs->push_back(f);
        if (level == 0)
        {
          // Level-0 files may overlap each other.  So check if the newly
          // added file has expanded the range.  If so, restart search.
          if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0)
          {
            user_begin = file_start;
            inputs->clear();
            i = 0;
          }
          else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0)
          {
            user_end = file_limit;
            inputs->clear();
            i = 0;
          }
        }
      }
    }
  }

  // DebugString()方法用于打印出Version类实例的信息，用于调试。
  std::string Version::DebugString() const
  {
    std::string r;
    for (int level = 0; level < config::kNumLevels; level++)
    {
      r.append("--- level ");
      AppendNumberTo(&r, level);
      r.append(" ---\n");
      const std::vector<FileMetaData *> &files = files_[level];
      for (size_t i = 0; i < files.size(); i++)
      {
        r.push_back(' ');
        AppendNumberTo(&r, files[i]->number);
        r.push_back(':');
        AppendNumberTo(&r, files[i]->file_size);
        r.append("[");
        r.append(files[i]->smallest.DebugString());
        r.append(" .. ");
        r.append(files[i]->largest.DebugString());
        r.append("]\n");
      }
    }
    return r;
  }

  class VersionSet::Builder
  {
  private:
    // Helper to sort by v->files_[file_number].smallest
    struct BySmallestKey
    {
      const InternalKeyComparator *internal_comparator;

      // 实现比较两个FileMetaData类实例的大小。如果key值相同，那么就比较两个
      // 的FileNumber，FileNumber大的那个文件就大。
      bool operator()(FileMetaData *f1, FileMetaData *f2) const
      {
        int r = internal_comparator->Compare(f1->smallest, f2->smallest);
        if (r != 0)
        {
          return (r < 0);
        }
        else
        {
          // Break ties by file number
          // 在smallest key相同的情况下，FileNumber大的那个文件比
          // FileNumber小的文件要“大”
          return (f1->number < f2->number);
        }
      }
    };

    // 定义存放FileMetaData *对象的集合类型，其中比较器采用BySmallestKey。
    typedef std::set<FileMetaData *, BySmallestKey> FileSet;

    // 要添加和删除的sstable文件集合，其中要删除的sstable文件集合中存放的是文件
    // 对应的FileNumber，而要添加的文件集合中存放的则是文件对应的FileMetaData
    // 类实例信息。
    struct LevelState
    {
      std::set<uint64_t> deleted_files;
      FileSet *added_files;
    };

    // vset_用于存放VersionSet实例
    VersionSet *vset_;

    // base_用于存放Version实例，这里的上下文含义就是基准版本。
    Version *base_;

    // 各个level上面要更新（添加和删除）的文件集合。
    LevelState levels_[config::kNumLevels];

  public:
    // Initialize a builder with the files from *base and other info from *vset
    Builder(VersionSet *vset, Version *base)
        : vset_(vset),
          base_(base)
    {
      base_->Ref();
      BySmallestKey cmp;
      cmp.internal_comparator = &vset_->icmp_;
      for (int level = 0; level < config::kNumLevels; level++)
      {
        levels_[level].added_files = new FileSet(cmp);
      }
    }

    ~Builder()
    {
      for (int level = 0; level < config::kNumLevels; level++)
      {
        const FileSet *added = levels_[level].added_files;
        std::vector<FileMetaData *> to_unref;
        to_unref.reserve(added->size());
        for (FileSet::const_iterator it = added->begin();
             it != added->end(); ++it)
        {
          to_unref.push_back(*it);
        }
        delete added;
        for (uint32_t i = 0; i < to_unref.size(); i++)
        {
          FileMetaData *f = to_unref[i];
          f->refs--;
          if (f->refs <= 0)
          {
            delete f;
          }
        }
      }
      base_->Unref();
    }

    // Apply all of the edits in *edit to the current state.
    // 将VersionEdit对象edit中的信息应用到VersionSet::Builder类实例中，换句话说
    // 就是将版本变动信息添加到VersionSet::Builder类实例中，后面构建新的Version
    // 的时候要用到。
    void Apply(VersionEdit *edit)
    {
      // Update compaction pointers
      // 为了尽量均匀compact每个层级，所以会将这一次compact的end-key作为下一次
      // compact的start-key，edit->compact_pointers_就保存了每一个level下一次compact的
      // start-key，这里的操作就是将edit中保存的compact pointer信息添加到对应的
      // VersionSet实例中。
      for (size_t i = 0; i < edit->compact_pointers_.size(); i++)
      {
        const int level = edit->compact_pointers_[i].first;
        vset_->compact_pointer_[level] =
            edit->compact_pointers_[i].second.Encode().ToString();
      }

      // Delete files
      // 将edit中存放的要删除的文件信息添加到levels_的deleted_files中。
      const VersionEdit::DeletedFileSet &del = edit->deleted_files_;
      for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
           iter != del.end();
           ++iter)
      {
        const int level = iter->first;
        const uint64_t number = iter->second;
        levels_[level].deleted_files.insert(number);
      }

      // Add new files
      // 将edit中存放的要新增的文件信息添加到levels_的added_files中
      for (size_t i = 0; i < edit->new_files_.size(); i++)
      {
        const int level = edit->new_files_[i].first;
        FileMetaData *f = new FileMetaData(edit->new_files_[i].second);
        f->refs = 1;

        // We arrange to automatically compact this file after
        // a certain number of seeks.  Let's assume:
        //   (1) One seek costs 10ms
        //   (2) Writing or reading 1MB costs 10ms (100MB/s)
        //   (3) A compaction of 1MB does 25MB of IO:
        //         1MB read from this level
        //         10-12MB read from next level (boundaries may be misaligned)
        //         10-12MB written to next level
        // This implies that 25 seeks cost the same as the compaction
        // of 1MB of data.  I.e., one seek costs approximately the
        // same as the compaction of 40KB of data.  We are a little
        // conservative and allow approximately one seek for every 16KB
        // of data before triggering a compaction.
        // 初始化文件在进行compact之前的最大允许访问次数
        f->allowed_seeks = (f->file_size / 16384);
        if (f->allowed_seeks < 100)
          f->allowed_seeks = 100;

        levels_[level].deleted_files.erase(f->number);
        levels_[level].added_files->insert(f);
      }
    }

    // Save the current state in *v.
    // 用VersionSet::Builder实例中存放的基准版本信息以及从VersionEdit实例中获取到的
    // 版本变动信息一起构造出一个新的Version实例。
    void SaveTo(Version *v)
    {
      BySmallestKey cmp;
      // 设置InternalComparator，用于对sstable文件中的key进行比较。
      cmp.internal_comparator = &vset_->icmp_;
      for (int level = 0; level < config::kNumLevels; level++)
      {
        // Merge the set of added files with the set of pre-existing files.
        // Drop any deleted files.  Store the result in *v.
        // 获取基准版本base_中的level层的所有sstable文件
        const std::vector<FileMetaData *> &base_files = base_->files_[level];
        std::vector<FileMetaData *>::const_iterator base_iter = base_files.begin();
        std::vector<FileMetaData *>::const_iterator base_end = base_files.end();

        // 获取在level层要新增的文件集合，集合中的内容是从VersionEdit类实例中获取到的
        // 获取过程是在Apply()方法中实现的。
        const FileSet *added = levels_[level].added_files;
        v->files_[level].reserve(base_files.size() + added->size());

        // 下面的循环处理过程所做的事情就是将基准版本中level层的sstable文件和从VersionEdit
        // 类实例中获取到的新版本中要新增的sstable文件放在一起，共同组成新版本level层的
        // sstable文件集合。两者组合的规则如下：
        // 对于added集合中的每一个文件，先将base_files数组中小于该文件的所有文件添加到
        // 新版中中，然后将该文件添加到新版本中。这一过程持续到added集合中的所有文件
        // 都处理完毕，added集合中的文件处理完毕之后，可能base_files数组中可能还有部分文件
        // 还没有添加到新版本中，所以最后要将这部分文件也一并添加到新版本中。最后添加的
        // 这部分文件(不一定有)有一个特点就是这部分文件比added集合中所有文件都要"大"，当然
        // 这个"大"是根据FileMetaData * 对象的比较器来说的，比较器实现可以参考BySmallestKey。
        for (FileSet::const_iterator added_iter = added->begin();
             added_iter != added->end();
             ++added_iter)
        {
          // Add all smaller files listed in base_
          for (std::vector<FileMetaData *>::const_iterator bpos = std::upper_bound(base_iter, base_end, *added_iter, cmp);
               base_iter != bpos;
               ++base_iter)
          {
            MaybeAddFile(v, level, *base_iter);
          }

          MaybeAddFile(v, level, *added_iter);
        }

        // Add remaining base files
        for (; base_iter != base_end; ++base_iter)
        {
          MaybeAddFile(v, level, *base_iter);
        }

#ifndef NDEBUG
        // Make sure there is no overlap in levels > 0
        if (level > 0)
        {
          for (uint32_t i = 1; i < v->files_[level].size(); i++)
          {
            const InternalKey &prev_end = v->files_[level][i - 1]->largest;
            const InternalKey &this_begin = v->files_[level][i]->smallest;
            if (vset_->icmp_.Compare(prev_end, this_begin) >= 0)
            {
              fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                      prev_end.DebugString().c_str(),
                      this_begin.DebugString().c_str());
              abort();
            }
          }
        }
#endif
      }
    }

    // MaybeAddFile()用于判断是否需要往Version实例v的level层中添加文件元信息对象f
    // 如果文件元信息对象f已经在要删除的文件集合中，那就不往Version实例中添加这个
    // 文件元信息对象了。否则的话，就将其添加到Version实例v的level层sstable文件vector中
    void MaybeAddFile(Version *v, int level, FileMetaData *f)
    {
      if (levels_[level].deleted_files.count(f->number) > 0)
      {
        // File is deleted: do nothing
      }
      else
      {
        std::vector<FileMetaData *> *files = &v->files_[level];
        if (level > 0 && !files->empty())
        {
          // Must not overlap
          assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                      f->smallest) < 0);
        }
        f->refs++;
        files->push_back(f);
      }
    }
  };

  VersionSet::VersionSet(const std::string &dbname,
                         const Options *options,
                         TableCache *table_cache,
                         const InternalKeyComparator *cmp)
      : env_(options->env),
        dbname_(dbname),
        options_(options),
        table_cache_(table_cache),
        icmp_(*cmp),
        next_file_number_(2),
        manifest_file_number_(0), // Filled by Recover()
        last_sequence_(0),
        log_number_(0),
        prev_log_number_(0),
        descriptor_file_(NULL),
        descriptor_log_(NULL),
        dummy_versions_(this),
        current_(NULL)
  {
    AppendVersion(new Version(this));
  }

  VersionSet::~VersionSet()
  {
    current_->Unref();
    assert(dummy_versions_.next_ == &dummy_versions_); // List must be empty
    delete descriptor_log_;
    delete descriptor_file_;
  }

  void VersionSet::AppendVersion(Version *v)
  {
    // Make "v" current
    assert(v->refs_ == 0);
    assert(v != current_);
    if (current_ != NULL)
    {
      current_->Unref();
    }
    current_ = v;
    v->Ref();

    // Append to linked list
    v->prev_ = dummy_versions_.prev_;
    v->next_ = &dummy_versions_;
    v->prev_->next_ = v;
    v->next_->prev_ = v;
  }

  Status VersionSet::LogAndApply(VersionEdit *edit, port::Mutex *mu)
  {
    if (edit->has_log_number_)
    {
      assert(edit->log_number_ >= log_number_);
      assert(edit->log_number_ < next_file_number_);
    }
    else
    {
      edit->SetLogNumber(log_number_);
    }

    if (!edit->has_prev_log_number_)
    {
      edit->SetPrevLogNumber(prev_log_number_);
    }

    edit->SetNextFile(next_file_number_);
    edit->SetLastSequence(last_sequence_);

    Version *v = new Version(this);
    {
      Builder builder(this, current_);
      builder.Apply(edit);
      builder.SaveTo(v);
    }
    Finalize(v);

    std::string new_manifest_file;
    Status s;
    if (descriptor_log_ == NULL)
    {
      assert(descriptor_file_ == NULL);
      new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
      edit->SetNextFile(next_file_number_);
      s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
      if (s.ok())
      {
        descriptor_log_ = new log::Writer(descriptor_file_);
        s = WriteSnapshot(descriptor_log_);
      }
    }

    // Unlock during expensive MANIFEST log write
    {
      mu->Unlock();

      // Write new record to MANIFEST log
      if (s.ok())
      {
        std::string record;
        edit->EncodeTo(&record);
        s = descriptor_log_->AddRecord(record);
        if (s.ok())
        {
          s = descriptor_file_->Sync();
        }
        if (!s.ok())
        {
          Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
        }
      }

      // If we just created a new descriptor file, install it by writing a
      // new CURRENT file that points to it.
      if (s.ok() && !new_manifest_file.empty())
      {
        s = SetCurrentFile(env_, dbname_, manifest_file_number_);
      }

      mu->Lock();
    }

    // Install the new version
    if (s.ok())
    {
      AppendVersion(v);
      log_number_ = edit->log_number_;
      prev_log_number_ = edit->prev_log_number_;
    }
    else
    {
      delete v;
      if (!new_manifest_file.empty())
      {
        delete descriptor_log_;
        delete descriptor_file_;
        descriptor_log_ = NULL;
        descriptor_file_ = NULL;
        env_->DeleteFile(new_manifest_file);
      }
    }

    return s;
  }

  Status VersionSet::Recover(bool *save_manifest)
  {
    struct LogReporter : public log::Reader::Reporter
    {
      Status *status;
      virtual void Corruption(size_t bytes, const Status &s)
      {
        if (this->status->ok())
          *this->status = s;
      }
    };

    // Read "CURRENT" file, which contains a pointer to the current manifest file
    std::string current;
    Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
    if (!s.ok())
    {
      return s;
    }
    if (current.empty() || current[current.size() - 1] != '\n')
    {
      return Status::Corruption("CURRENT file does not end with newline");
    }
    current.resize(current.size() - 1);

    std::string dscname = dbname_ + "/" + current;
    SequentialFile *file;
    s = env_->NewSequentialFile(dscname, &file);
    if (!s.ok())
    {
      if (s.IsNotFound())
      {
        return Status::Corruption(
            "CURRENT points to a non-existent file", s.ToString());
      }
      return s;
    }

    bool have_log_number = false;
    bool have_prev_log_number = false;
    bool have_next_file = false;
    bool have_last_sequence = false;
    uint64_t next_file = 0;
    uint64_t last_sequence = 0;
    uint64_t log_number = 0;
    uint64_t prev_log_number = 0;
    Builder builder(this, current_);

    {
      LogReporter reporter;
      reporter.status = &s;
      log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
      Slice record;
      std::string scratch;
      while (reader.ReadRecord(&record, &scratch) && s.ok())
      {
        VersionEdit edit;
        s = edit.DecodeFrom(record);
        if (s.ok())
        {
          if (edit.has_comparator_ &&
              edit.comparator_ != icmp_.user_comparator()->Name())
          {
            s = Status::InvalidArgument(
                edit.comparator_ + " does not match existing comparator ",
                icmp_.user_comparator()->Name());
          }
        }

        if (s.ok())
        {
          builder.Apply(&edit);
        }

        if (edit.has_log_number_)
        {
          log_number = edit.log_number_;
          have_log_number = true;
        }

        if (edit.has_prev_log_number_)
        {
          prev_log_number = edit.prev_log_number_;
          have_prev_log_number = true;
        }

        if (edit.has_next_file_number_)
        {
          next_file = edit.next_file_number_;
          have_next_file = true;
        }

        if (edit.has_last_sequence_)
        {
          last_sequence = edit.last_sequence_;
          have_last_sequence = true;
        }
      }
    }
    delete file;
    file = NULL;

    if (s.ok())
    {
      if (!have_next_file)
      {
        s = Status::Corruption("no meta-nextfile entry in descriptor");
      }
      else if (!have_log_number)
      {
        s = Status::Corruption("no meta-lognumber entry in descriptor");
      }
      else if (!have_last_sequence)
      {
        s = Status::Corruption("no last-sequence-number entry in descriptor");
      }

      if (!have_prev_log_number)
      {
        prev_log_number = 0;
      }

      MarkFileNumberUsed(prev_log_number);
      MarkFileNumberUsed(log_number);
    }

    if (s.ok())
    {
      Version *v = new Version(this);
      builder.SaveTo(v);
      // Install recovered version
      Finalize(v);
      AppendVersion(v);
      manifest_file_number_ = next_file;
      next_file_number_ = next_file + 1;
      last_sequence_ = last_sequence;
      log_number_ = log_number;
      prev_log_number_ = prev_log_number;

      // See if we can reuse the existing MANIFEST file.
      if (ReuseManifest(dscname, current))
      {
        // No need to save new manifest
      }
      else
      {
        *save_manifest = true;
      }
    }

    return s;
  }

  bool VersionSet::ReuseManifest(const std::string &dscname,
                                 const std::string &dscbase)
  {
    if (!options_->reuse_logs)
    {
      return false;
    }
    FileType manifest_type;
    uint64_t manifest_number;
    uint64_t manifest_size;
    if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
        manifest_type != kDescriptorFile ||
        !env_->GetFileSize(dscname, &manifest_size).ok() ||
        // Make new compacted MANIFEST if old one is too big
        manifest_size >= TargetFileSize(options_))
    {
      return false;
    }

    assert(descriptor_file_ == NULL);
    assert(descriptor_log_ == NULL);
    Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
    if (!r.ok())
    {
      Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
      assert(descriptor_file_ == NULL);
      return false;
    }

    Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
    descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
    manifest_file_number_ = manifest_number;
    return true;
  }

  void VersionSet::MarkFileNumberUsed(uint64_t number)
  {
    if (next_file_number_ <= number)
    {
      next_file_number_ = number + 1;
    }
  }

  void VersionSet::Finalize(Version *v)
  {
    // Precomputed best level for next compaction
    int best_level = -1;
    double best_score = -1;

    for (int level = 0; level < config::kNumLevels - 1; level++)
    {
      double score;
      if (level == 0)
      {
        score = v->files_[level].size() /
                static_cast<double>(config::kL0_CompactionTrigger);
      }
      else
      {
        const uint64_t level_bytes = TotalFileSize(v->files_[level]);
        score =
            static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
      }

      if (score > best_score)
      {
        best_level = level;
        best_score = score;
      }
    }

    v->compaction_level_ = best_level;
    v->compaction_score_ = best_score;
  }

  Status VersionSet::WriteSnapshot(log::Writer *log)
  {
    // TODO: Break up into multiple records to reduce memory usage on recovery?

    // Save metadata
    VersionEdit edit;
    edit.SetComparatorName(icmp_.user_comparator()->Name());

    // Save compaction pointers
    for (int level = 0; level < config::kNumLevels; level++)
    {
      if (!compact_pointer_[level].empty())
      {
        InternalKey key;
        key.DecodeFrom(compact_pointer_[level]);
        edit.SetCompactPointer(level, key);
      }
    }

    // Save files
    for (int level = 0; level < config::kNumLevels; level++)
    {
      const std::vector<FileMetaData *> &files = current_->files_[level];
      for (size_t i = 0; i < files.size(); i++)
      {
        const FileMetaData *f = files[i];
        edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
      }
    }

    std::string record;
    edit.EncodeTo(&record);
    return log->AddRecord(record);
  }

  int VersionSet::NumLevelFiles(int level) const
  {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    return current_->files_[level].size();
  }

  const char *VersionSet::LevelSummary(LevelSummaryStorage *scratch) const
  {
    // Update code if kNumLevels changes
    assert(config::kNumLevels == 7);
    snprintf(scratch->buffer, sizeof(scratch->buffer),
             "files[ %d %d %d %d %d %d %d ]",
             int(current_->files_[0].size()),
             int(current_->files_[1].size()),
             int(current_->files_[2].size()),
             int(current_->files_[3].size()),
             int(current_->files_[4].size()),
             int(current_->files_[5].size()),
             int(current_->files_[6].size()));
    return scratch->buffer;
  }

  uint64_t VersionSet::ApproximateOffsetOf(Version *v, const InternalKey &ikey)
  {
    uint64_t result = 0;
    for (int level = 0; level < config::kNumLevels; level++)
    {
      const std::vector<FileMetaData *> &files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++)
      {
        if (icmp_.Compare(files[i]->largest, ikey) <= 0)
        {
          // Entire file is before "ikey", so just add the file size
          result += files[i]->file_size;
        }
        else if (icmp_.Compare(files[i]->smallest, ikey) > 0)
        {
          // Entire file is after "ikey", so ignore
          if (level > 0)
          {
            // Files other than level 0 are sorted by meta->smallest, so
            // no further files in this level will contain data for
            // "ikey".
            break;
          }
        }
        else
        {
          // "ikey" falls in the range for this table.  Add the
          // approximate offset of "ikey" within the table.
          Table *tableptr;
          Iterator *iter = table_cache_->NewIterator(
              ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
          if (tableptr != NULL)
          {
            result += tableptr->ApproximateOffsetOf(ikey.Encode());
          }
          delete iter;
        }
      }
    }
    return result;
  }

  void VersionSet::AddLiveFiles(std::set<uint64_t> *live)
  {
    for (Version *v = dummy_versions_.next_;
         v != &dummy_versions_;
         v = v->next_)
    {
      for (int level = 0; level < config::kNumLevels; level++)
      {
        const std::vector<FileMetaData *> &files = v->files_[level];
        for (size_t i = 0; i < files.size(); i++)
        {
          live->insert(files[i]->number);
        }
      }
    }
  }

  int64_t VersionSet::NumLevelBytes(int level) const
  {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    return TotalFileSize(current_->files_[level]);
  }

  int64_t VersionSet::MaxNextLevelOverlappingBytes()
  {
    int64_t result = 0;
    std::vector<FileMetaData *> overlaps;
    for (int level = 1; level < config::kNumLevels - 1; level++)
    {
      for (size_t i = 0; i < current_->files_[level].size(); i++)
      {
        const FileMetaData *f = current_->files_[level][i];
        current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                       &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > result)
        {
          result = sum;
        }
      }
    }
    return result;
  }

  // Stores the minimal range that covers all entries in inputs in
  // *smallest, *largest.
  // REQUIRES: inputs is not empty
  void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs,
                            InternalKey *smallest,
                            InternalKey *largest)
  {
    assert(!inputs.empty());
    smallest->Clear();
    largest->Clear();
    for (size_t i = 0; i < inputs.size(); i++)
    {
      FileMetaData *f = inputs[i];
      if (i == 0)
      {
        *smallest = f->smallest;
        *largest = f->largest;
      }
      else
      {
        if (icmp_.Compare(f->smallest, *smallest) < 0)
        {
          *smallest = f->smallest;
        }
        if (icmp_.Compare(f->largest, *largest) > 0)
        {
          *largest = f->largest;
        }
      }
    }
  }

  // Stores the minimal range that covers all entries in inputs1 and inputs2
  // in *smallest, *largest.
  // REQUIRES: inputs is not empty
  void VersionSet::GetRange2(const std::vector<FileMetaData *> &inputs1,
                             const std::vector<FileMetaData *> &inputs2,
                             InternalKey *smallest,
                             InternalKey *largest)
  {
    std::vector<FileMetaData *> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end());
    GetRange(all, smallest, largest);
  }

  Iterator *VersionSet::MakeInputIterator(Compaction *c)
  {
    ReadOptions options;
    options.verify_checksums = options_->paranoid_checks;
    options.fill_cache = false;

    // Level-0 files have to be merged together.  For other levels,
    // we will make a concatenating iterator per level.
    // TODO(opt): use concatenating iterator for level-0 if there is no overlap
    const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
    Iterator **list = new Iterator *[space];
    int num = 0;
    for (int which = 0; which < 2; which++)
    {
      if (!c->inputs_[which].empty())
      {
        if (c->level() + which == 0)
        {
          const std::vector<FileMetaData *> &files = c->inputs_[which];
          for (size_t i = 0; i < files.size(); i++)
          {
            list[num++] = table_cache_->NewIterator(
                options, files[i]->number, files[i]->file_size);
          }
        }
        else
        {
          // Create concatenating iterator for the files from this level
          list[num++] = NewTwoLevelIterator(
              new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
              &GetFileIterator, table_cache_, options);
        }
      }
    }
    assert(num <= space);
    Iterator *result = NewMergingIterator(&icmp_, list, num);
    delete[] list;
    return result;
  }

  Compaction *VersionSet::PickCompaction()
  {
    Compaction *c;
    int level;

    const bool size_compaction = (current_->compaction_score_ >= 1);
    const bool seek_compaction = (current_->file_to_compact_ != NULL);
    if (size_compaction)
    {
      level = current_->compaction_level_;
      assert(level >= 0);
      assert(level + 1 < config::kNumLevels);
      c = new Compaction(options_, level);

      for (size_t i = 0; i < current_->files_[level].size(); i++)
      {
        FileMetaData *f = current_->files_[level][i];
        if (compact_pointer_[level].empty() ||
            icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0)
        {
          c->inputs_[0].push_back(f);
          break;
        }
      }
      if (c->inputs_[0].empty())
      {
        // Wrap-around to the beginning of the key space
        c->inputs_[0].push_back(current_->files_[level][0]);
      }
    }
    else if (seek_compaction)
    {
      level = current_->file_to_compact_level_;
      c = new Compaction(options_, level);
      c->inputs_[0].push_back(current_->file_to_compact_);
    }
    else
    {
      return NULL;
    }

    c->input_version_ = current_;
    c->input_version_->Ref();

    if (level == 0)
    {
      InternalKey smallest, largest;
      GetRange(c->inputs_[0], &smallest, &largest);
      current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
      assert(!c->inputs_[0].empty());
    }

    SetupOtherInputs(c);

    return c;
  }

  void VersionSet::SetupOtherInputs(Compaction *c)
  {
    const int level = c->level();
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);

    current_->GetOverlappingInputs(level + 1, &smallest, &largest, &c->inputs_[1]);

    // Get entire range covered by compaction
    InternalKey all_start, all_limit;
    GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

    // See if we can grow the number of inputs in "level" without
    // changing the number of "level+1" files we pick up.
    if (!c->inputs_[1].empty())
    {
      std::vector<FileMetaData *> expanded0;
      current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
      const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
      const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
      const int64_t expanded0_size = TotalFileSize(expanded0);
      if (expanded0.size() > c->inputs_[0].size() &&
          inputs1_size + expanded0_size <
              ExpandedCompactionByteSizeLimit(options_))
      {
        InternalKey new_start, new_limit;
        GetRange(expanded0, &new_start, &new_limit);
        std::vector<FileMetaData *> expanded1;
        current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                       &expanded1);
        if (expanded1.size() == c->inputs_[1].size())
        {
          Log(options_->info_log,
              "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
              level,
              int(c->inputs_[0].size()),
              int(c->inputs_[1].size()),
              long(inputs0_size), long(inputs1_size),
              int(expanded0.size()),
              int(expanded1.size()),
              long(expanded0_size), long(inputs1_size));
          smallest = new_start;
          largest = new_limit;
          c->inputs_[0] = expanded0;
          c->inputs_[1] = expanded1;
          GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
        }
      }
    }

    // Compute the set of grandparent files that overlap this compaction
    // (parent == level+1; grandparent == level+2)
    if (level + 2 < config::kNumLevels)
    {
      current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                     &c->grandparents_);
    }

    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    compact_pointer_[level] = largest.Encode().ToString();
    c->edit_.SetCompactPointer(level, largest);
  }

  Compaction *VersionSet::CompactRange(
      int level,
      const InternalKey *begin,
      const InternalKey *end)
  {
    std::vector<FileMetaData *> inputs;
    current_->GetOverlappingInputs(level, begin, end, &inputs);
    if (inputs.empty())
    {
      return NULL;
    }

    if (level > 0)
    {
      const uint64_t limit = MaxFileSizeForLevel(options_, level);
      uint64_t total = 0;
      for (size_t i = 0; i < inputs.size(); i++)
      {
        uint64_t s = inputs[i]->file_size;
        total += s;
        if (total >= limit)
        {
          inputs.resize(i + 1);
          break;
        }
      }
    }

    Compaction *c = new Compaction(options_, level);
    c->input_version_ = current_;
    c->input_version_->Ref();
    c->inputs_[0] = inputs;
    SetupOtherInputs(c);
    return c;
  }

  Compaction::Compaction(const Options *options, int level)
      : level_(level),
        max_output_file_size_(MaxFileSizeForLevel(options, level)),
        input_version_(NULL),
        grandparent_index_(0),
        seen_key_(false),
        overlapped_bytes_(0)
  {
    for (int i = 0; i < config::kNumLevels; i++)
    {
      level_ptrs_[i] = 0;
    }
  }

  Compaction::~Compaction()
  {
    if (input_version_ != NULL)
    {
      input_version_->Unref();
    }
  }

  // IsTrivialMove()方法用于判断某一个compact操作是否存在着超过预期的损耗，
  // 其判断依据需要下面三个条件都满足：
  // 1. 要进行compact的level层中只有一个参与compact的sstable文件。
  // 2. 要进行compact的level+1层中没有参与compact的sstable文件。
  // 3. 要进行compact的level层的下两层中和此次进行compact的key值范围重叠
  //    的文件总大小不小于下两层的总的文件大小。
  bool Compaction::IsTrivialMove() const
  {
    const VersionSet *vset = input_version_->vset_;
    return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
            TotalFileSize(grandparents_) <=
                MaxGrandParentOverlapBytes(vset->options_));
  }

  // AddInputDeletions()方法用于将Compact类实例中存放的将要进行compact的
  // 两个level中的sstable文件对应的文件元信息对象加入到存放着版本变动相对于
  // 基线版本来说要删除的sstable文件元信息对象的集合中，因为要进行compact的
  // 两个level中的所有sstable文件在compact完成，生成新的level+1层sstable文件
  // 之后就可以删除了，所以这里将它们添加到存放着版本变动相对于基线版本来说要
  // 删除的sstable文件元信息对象的集合中
  void Compaction::AddInputDeletions(VersionEdit *edit)
  {
    for (int which = 0; which < 2; which++)
    {
      for (size_t i = 0; i < inputs_[which].size(); i++)
      {
        edit->DeleteFile(level_ + which, inputs_[which][i]->number);
      }
    }
  }

  bool Compaction::IsBaseLevelForKey(const Slice &user_key)
  {
    const Comparator *user_cmp = input_version_->vset_->icmp_.user_comparator();
    for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++)
    {
      const std::vector<FileMetaData *> &files = input_version_->files_[lvl];
      for (; level_ptrs_[lvl] < files.size();)
      {
        FileMetaData *f = files[level_ptrs_[lvl]];
        if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0)
        {
          if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0)
          {
            return false;
          }
          break;
        }
        level_ptrs_[lvl]++;
      }
    }
    return true;
  }

  bool Compaction::ShouldStopBefore(const Slice &internal_key)
  {
    const VersionSet *vset = input_version_->vset_;
    // Scan to find earliest grandparent file that contains key.
    const InternalKeyComparator *icmp = &vset->icmp_;
    while (grandparent_index_ < grandparents_.size() &&
           icmp->Compare(internal_key,
                         grandparents_[grandparent_index_]->largest.Encode()) > 0)
    {
      if (seen_key_)
      {
        overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
      }
      grandparent_index_++;
    }
    seen_key_ = true;

    if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_))
    {
      // Too much overlap for current output; start new output
      overlapped_bytes_ = 0;
      return true;
    }
    else
    {
      return false;
    }
  }

  // 释放输入
  void Compaction::ReleaseInputs()
  {
    if (input_version_ != NULL)
    {
      input_version_->Unref();
      input_version_ = NULL;
    }
  }

} // namespace leveldb
