#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb
{

  class Slice;

  class LEVELDB_EXPORT WriteBatch
  {
  public:
    WriteBatch();
    ~WriteBatch();

    void Put(const Slice &key, const Slice &value);

    void Delete(const Slice &key);

    void Clear();

    size_t ApproximateSize();

    class Handler
    {
    public:
      virtual ~Handler();
      virtual void Put(const Slice &key, const Slice &value) = 0;
      virtual void Delete(const Slice &key) = 0;
    };
    Status Iterate(Handler *handler) const;

  private:
    friend class WriteBatchInternal;

    std::string rep_;
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
