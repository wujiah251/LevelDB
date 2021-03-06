// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <stdio.h>
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb
{

  // Grouping of constants.  We may want to make some of these
  // parameters set via options.
  namespace config
  {
    static const int kNumLevels = 7;

    // Level-0 compaction is started when we hit this many files.
    static const int kL0_CompactionTrigger = 4;

    // Soft limit on number of level-0 files.  We slow down writes at this point.
    static const int kL0_SlowdownWritesTrigger = 8;

    // Maximum number of level-0 files.  We stop writes at this point.
    static const int kL0_StopWritesTrigger = 12;

    // Maximum level to which a new compacted memtable is pushed if it
    // does not create overlap.  We try to push to level 2 to avoid the
    // relatively expensive level 0=>1 compactions and to avoid some
    // expensive manifest file operations.  We do not push all the way to
    // the largest level since that can generate a lot of wasted disk
    // space if the same key space is being repeatedly overwritten.
    static const int kMaxMemCompactLevel = 2;

    // Approximate gap in bytes between samples of data read during iteration.
    static const int kReadBytesPeriod = 1048576;

  } // namespace config

  class InternalKey;

  // Value types encoded as the last component of internal keys.
  // DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
  // data structures.
  enum ValueType
  {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1
  };
  // kValueTypeForSeek defines the ValueType that should be passed when
  // constructing a ParsedInternalKey object for seeking to a particular
  // sequence number (since we sort sequence numbers in decreasing order
  // and the value type is embedded as the low 8 bits in the sequence
  // number in internal keys, we need to use the highest-numbered
  // ValueType, not the lowest).
  static const ValueType kValueTypeForSeek = kTypeValue;

  typedef uint64_t SequenceNumber;

  // We leave eight bits empty at the bottom so a type and sequence#
  // can be packed together into 64-bits.
  static const SequenceNumber kMaxSequenceNumber =
      ((0x1ull << 56) - 1);

  struct ParsedInternalKey
  {
    Slice user_key;          // user key
    SequenceNumber sequence; // sequence number
    ValueType type;          // type

    ParsedInternalKey() {} // Intentionally left uninitialized (for speed)
    ParsedInternalKey(const Slice &u, const SequenceNumber &seq, ValueType t)
        : user_key(u), sequence(seq), type(t) {}
    std::string DebugString() const;
  };

  // Return the length of the encoding of "key".
  inline size_t InternalKeyEncodingLength(const ParsedInternalKey &key)
  {
    return key.user_key.size() + 8;
  }

  // Append the serialization of "key" to *result.
  extern void AppendInternalKey(std::string *result,
                                const ParsedInternalKey &key);

  // Attempt to parse an internal key from "internal_key".  On success,
  // stores the parsed data in "*result", and returns true.
  //
  // On error, returns false, leaves "*result" in an undefined state.
  extern bool ParseInternalKey(const Slice &internal_key,
                               ParsedInternalKey *result);

  // Returns the user key portion of an internal key.

  // ExtractUserKey()???????????????internal key????????????user key?????????
  inline Slice ExtractUserKey(const Slice &internal_key)
  {
    assert(internal_key.size() >= 8);
    return Slice(internal_key.data(), internal_key.size() - 8);
  }

  // ExtractValueType()???????????????internal_key????????????type?????????
  inline ValueType ExtractValueType(const Slice &internal_key)
  {
    assert(internal_key.size() >= 8);
    const size_t n = internal_key.size();
    uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
    unsigned char c = num & 0xff;
    return static_cast<ValueType>(c);
  }

  // A comparator for internal keys that uses a specified comparator for
  // the user key portion and breaks ties by decreasing sequence number.
  // InternalKeyComparator????????????????????????internal key????????????????????????
  class InternalKeyComparator : public Comparator
  {
  private:
    // user_comparator_??????????????????????????????????????????????????????????????????????????????user key???
    // ???InternalKeyComparator????????????Compare()??????????????????internal key??????
    const Comparator *user_comparator_;

  public:
    explicit InternalKeyComparator(const Comparator *c) : user_comparator_(c) {}
    virtual const char *Name() const;

    // Compare????????????InternalKeyComparator????????????internal key???????????????
    virtual int Compare(const Slice &a, const Slice &b) const;
    virtual void FindShortestSeparator(
        std::string *start,
        const Slice &limit) const;
    virtual void FindShortSuccessor(std::string *key) const;

    const Comparator *user_comparator() const { return user_comparator_; }

    // Compare????????????InternalKeyComparator????????????internal key???????????????
    int Compare(const InternalKey &a, const InternalKey &b) const;
  };

  // Filter policy wrapper that converts from internal keys to user keys
  class InternalFilterPolicy : public FilterPolicy
  {
  private:
    const FilterPolicy *const user_policy_;

  public:
    explicit InternalFilterPolicy(const FilterPolicy *p) : user_policy_(p) {}
    virtual const char *Name() const;
    virtual void CreateFilter(const Slice *keys, int n, std::string *dst) const;
    virtual bool KeyMayMatch(const Slice &key, const Slice &filter) const;
  };

  // MemTable???????????????????????????internal key??????????????????
  // |userkey|sequence number|type|
  class InternalKey
  {
  private:
    std::string rep_;

  public:
    InternalKey() {}

    // InternalKey???????????????????????????user_key???sequence number ???type?????????internal key???
    // ????????????rep_??????
    InternalKey(const Slice &user_key, SequenceNumber s, ValueType t)
    {
      AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
    }

    // DecodeFrom()?????????Slice????????????????????????internal key????????????
    void DecodeFrom(const Slice &s) { rep_.assign(s.data(), s.size()); }
    Slice Encode() const
    {
      assert(!rep_.empty());
      return rep_;
    }

    // user_key()?????????internal key?????????user key???
    Slice user_key() const { return ExtractUserKey(rep_); }

    void SetFrom(const ParsedInternalKey &p)
    {
      rep_.clear();
      AppendInternalKey(&rep_, p);
    }

    void Clear() { rep_.clear(); }

    // DebugString()????????????????????????internal key????????????????????????
    std::string DebugString() const;
  };

  // InternalKeyComparator?????????????????????
  inline int InternalKeyComparator::Compare(
      const InternalKey &a, const InternalKey &b) const
  {
    return Compare(a.Encode(), b.Encode());
  }

  // ???internal key???????????????????????????internal key???????????????????????????????????????????????????
  // ?????????ParsedInternalKey????????????result????????????user key???sequence number???type???
  inline bool ParseInternalKey(const Slice &internal_key,
                               ParsedInternalKey *result)
  {
    const size_t n = internal_key.size();
    if (n < 8)
      return false;

    // ?????????sequence number | type???????????????sequence number???type???
    uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
    unsigned char c = num & 0xff;
    result->sequence = num >> 8;
    result->type = static_cast<ValueType>(c);
    result->user_key = Slice(internal_key.data(), n - 8);
    return (c <= static_cast<unsigned char>(kTypeValue));
  }

  // LookupKey????????????????????????MemTable????????????????????????????????????????????????????????????????????????
  //   klength	varint32			 <-- start_
  //	 userkey  char[klength] 		 <-- kstart_
  //	 tag	  uint64
  //									 <-- end_
  // ????????????????????????????????????????????????????????????????????????memtable_key???internal_key??????user_key???
  // A?????????user_key.size() + 8 ?????????????????????
  // B?????????userkey
  // C?????????64??????????????????<< 8 + ????????? 64????????????????????????
  // memtable_key = A + B + C
  // internal_key = B + C
  // user_key = B
  class LookupKey
  {
  public:
    LookupKey(const Slice &user_key, SequenceNumber sequence);

    ~LookupKey();

    // Return a key suitable for lookup in a MemTable.
    Slice memtable_key() const { return Slice(start_, end_ - start_); }

    // Return an internal key (suitable for passing to an internal iterator)
    Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

    // Return the user key
    Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

  private:
    // We construct a char array of the form:
    //    klength  varint32               <-- start_
    //    userkey  char[klength]          <-- kstart_
    //    tag      uint64
    //                                    <-- end_
    // The array is a suitable MemTable key.
    // The suffix starting with "userkey" can be used as an InternalKey.
    const char *start_;
    const char *kstart_;
    const char *end_;
    char space_[200]; // Avoid allocation for short keys

    // No copying allowed
    LookupKey(const LookupKey &);
    void operator=(const LookupKey &);
  };

  inline LookupKey::~LookupKey()
  {
    if (start_ != space_)
      delete[] start_;
  }

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_DBFORMAT_H_
