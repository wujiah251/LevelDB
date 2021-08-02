#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace leveldb
{

  class InternalKeyComparator; //内部key比较器
  class Mutex;                 // 互斥锁
  class MemTableIterator;

  class MemTable
  {
  public:
    explicit MemTable(const InternalKeyComparator &comparator);

    // 引用计数+1
    void Ref() { ++refs_; }

    // 引用计数减1
    void Unref()
    {
      --refs_;
      assert(refs_ >= 0);
      if (refs_ <= 0)
      {
        delete this;
      }
    }

    // 返回memtable实例的内存使用量，其实就算调用内存分配器的函数
    size_t ApproximateMemoryUsage();

    // 创建一个迭代器，其实就是一个MemTableIterator类实例。
    // 内部封装了skiplist
    Iterator *NewIterator();
    // 添加一个key-value
    void Add(SequenceNumber seq, ValueType type,
             const Slice &key,
             const Slice &value);
    // 根据key获取value
    bool Get(const LookupKey &key, std::string *value, Status *s);

  private:
    ~MemTable();

    // MemTable类内部使用的键比较器类型。
    struct KeyComparator
    {
      const InternalKeyComparator comparator;
      explicit KeyComparator(const InternalKeyComparator &c) : comparator(c) {}
      int operator()(const char *a, const char *b) const;
    };

    friend class MemTableIterator;
    friend class MemTableBackwardIterator;

    // 定义一个MemTable用来存储元素的skiplist类型，该skiplist内部的key类型是字符串，
    // 所用的key比较器类型是KeyComparator
    typedef SkipList<const char *, KeyComparator> Table;

    // MemTable类实例用来比较内部元素大小的比较器。
    KeyComparator comparator_;

    // MemTable类实例的内部引用计数器。
    int refs_;

    // MemTable类实例用来给元素分配内存时使用的内存管理器。
    Arena arena_;

    // MemTable类实例用来存储元素的表，其内部实现其实是一个跳表，skiplist。
    Table table_;

    // No copying allowed
    MemTable(const MemTable &);
    void operator=(const MemTable &);
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_MEMTABLE_H_
