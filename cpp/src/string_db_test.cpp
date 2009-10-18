// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/profiles/string_db.h"
#include <vector>
#include <utility>

#include "babudb/profiles/string_key.h"
#include "babudb/test.h"
#include "index/merger.h"

#include "yield/platform/memory_mapped_file.h"
using YIELD::MemoryMappedFile;
using namespace babudb;

TEST_TMPDIR(StringDB,babudb)
{
  vector<string> indices; indices.push_back("index");

  StringDB* db = StringDB::Open(testPath("test"), indices);
  db->Add("index", "key1", "value1");
  db->Commit();
  ASSERT_TRUE(db->Lookup("index", "key1") == "value1");
  ASSERT_TRUE(db->Lookup("index", "key2") == "");
  delete db;

  db = StringDB::Open(testPath("test"), indices);
  ASSERT_TRUE(db->Lookup("index", "key1") == "value1");
  ASSERT_TRUE(db->Lookup("index", "key2") == "");
  db->Compact(testPath("alt_"));
  db->Add("index", "key2", "value2");
  db->Commit();
  ASSERT_TRUE(db->Lookup("index", "key1") == "value1");
  ASSERT_TRUE(db->Lookup("index", "key2") == "value2");
  delete db;

  db = StringDB::Open(testPath("test"), indices);
  ASSERT_TRUE(db->Lookup("index", "key1") == "value1");
  ASSERT_TRUE(db->Lookup("index", "key2") == "value2");
  db->Compact(testPath("alt_"));
  delete db;
}
