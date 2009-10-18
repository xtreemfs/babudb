// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/database.h"
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

  StringSetOperation("testidx", "Key1", "data1").ApplyTo(*db, 1);

  Buffer result = db->Lookup("testidx", DataHolder("Key1"));
  EXPECT_FALSE(result.isEmpty());
  EXPECT_TRUE(strncmp((char*)result.data,"data1",5) == 0);

  result = db->Lookup("testidx",DataHolder("Key2"));
  EXPECT_TRUE(result.isEmpty());

  StringSetOperation("testidx", "Key2", "data2").ApplyTo(*db, 2);
  result = db->Lookup("testidx", DataHolder("Key2"));
  EXPECT_FALSE(result.isEmpty());

  // Overwrite
  StringSetOperation("testidx", "Key1", "data3").ApplyTo(*db, 3);

  result = db->Lookup("testidx", DataHolder("Key1"));
  EXPECT_FALSE(result.isEmpty());
  EXPECT_TRUE(strncmp((char*)result.data,"data3",5) == 0);

  // Prefix
  StringSetOperation("testidx", "Ke4", "data4").ApplyTo(*db, 4);

/*	vector<pair<Buffer,Buffer> > results = db->match("testidx",Buffer("Key2",4));
	EXPECT_TRUE(results.size() == 1);

	results = db->match("testidx",Buffer("Key"));
	EXPECT_TRUE(results.size() == 2);

	results = db->match("testidx",Buffer("Ke"));
	EXPECT_TRUE(results.size() == 3);
*/
  delete db;
}
