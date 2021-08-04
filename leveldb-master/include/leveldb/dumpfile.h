#ifndef STORAGE_LEVELDB_INCLUDE_DUMPFILE_H_
#define STORAGE_LEVELDB_INCLUDE_DUMPFILE_H_

#include <string>
#include "leveldb/env.h"
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb
{

    LEVELDB_EXPORT Status DumpFile(Env *env, const std::string &fname,
                                   WritableFile *dst);

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_DUMPFILE_H_
