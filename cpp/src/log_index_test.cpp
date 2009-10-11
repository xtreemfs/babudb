// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/Database.h"
#include "babudb/profiles/string_key.h"

#include "log_index.h"
#include "merged_index.h"

#include "yield/platform/memory_mapped_file.h"
using YIELD::MemoryMappedFile;
using namespace babudb;

#include "babudb/test.h"

TEST_TMPDIR(LogIndex,babudb)
{
  StringOrder myorder;

  std::vector<babudb::IndexDescriptor> indices;
  indices.push_back(std::make_pair("testidx", &myorder));

  Database* db = Database::Open(testPath("test").getHostCharsetPath(), indices);

  StringSetOperation("testidx", 1, "Key1", "data1").applyTo(*db);

  Buffer result = db->Lookup("testidx", DataHolder("Key1"));
  EXPECT_FALSE(result.isEmpty());
  EXPECT_TRUE(strncmp((char*)result.data,"data1",5) == 0);

  result = db->Lookup("testidx",DataHolder("Key2"));
  EXPECT_TRUE(result.isEmpty());

  StringSetOperation("testidx", 2, "Key2", "data2").applyTo(*db);
  result = db->Lookup("testidx", DataHolder("Key2"));
  EXPECT_FALSE(result.isEmpty());

  // Overwrite
  StringSetOperation("testidx", 3, "Key1", "data3").applyTo(*db);

  result = db->Lookup("testidx", DataHolder("Key1"));
  EXPECT_FALSE(result.isEmpty());
  EXPECT_TRUE(strncmp((char*)result.data,"data3",5) == 0);

  // Prefix
  StringSetOperation("testidx", 4, "Ke4", "data4").applyTo(*db);

/*	vector<pair<Buffer,Buffer> > results = db->match("testidx",Buffer("Key2",4));
	EXPECT_TRUE(results.size() == 1);

	results = db->match("testidx",Buffer("Key"));
	EXPECT_TRUE(results.size() == 2);

	results = db->match("testidx",Buffer("Ke"));
	EXPECT_TRUE(results.size() == 3);
*/
  delete db;
}
