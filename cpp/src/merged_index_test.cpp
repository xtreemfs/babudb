// This file is part of babudb/cpp
//
// Copyright (c) 2010 Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/database.h"
#include <vector>
#include <utility>

#include "babudb/profiles/string_key.h"
#include "babudb/lookup_iterator.h"
#include "babudb/test.h"
#include "merged_index.h"

#include "yield/platform/memory_mapped_file.h"
using YIELD::MemoryMappedFile;
using namespace babudb;

TEST_TMPDIR(MergedIndex,babudb)
{
  StringOrder sorder;
  MergedIndex index("test", sorder);
  index.Add(Buffer::wrap("key1"), Buffer::wrap("val1"));

  // Snapshot, then overwrite the value
  index.Snapshot(2);
  index.Add(Buffer::wrap("key1"), Buffer::wrap("val2"));
  Buffer value = index.Lookup(Buffer::wrap("key1"));
  EXPECT_EQUAL(value, Buffer::wrap("val2"));

  // Check the snapshot
  LookupIterator snapshot = index.GetSnapshot(2);
  EXPECT_EQUAL(Buffer::wrap("val1"), (*snapshot).second);
}
