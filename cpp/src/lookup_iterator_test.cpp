// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, 2010 Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include <utility>
using std::pair;

#include "babudb/log/log.h"
#include "log_index.h"
#include "babudb/profiles/string_key.h"
#include "babudb/lookup_iterator.h"
#include "index/index_writer.h"
#include "index/index.h"
#include "index/merger.h"
#include "yield/platform/memory_mapped_file.h"
using namespace YIELD;
using namespace babudb;

#include "babudb/test.h"

void EXPECT_KEY_ADVANCE(
    LookupIterator& it,
    const babudb::Buffer& key, const babudb::Buffer& value) {
  EXPECT_TRUE(it.hasMore());
  if (!((*it).first == key)) {
    string found((char*)(*it).first.data, (*it).first.size);
    string expected((char*)key.data ,key.size);
    EXPECT_TRUE(false);
  }
  if (!((*it).second == value)) {
    string found((char*)(*it).second.data, (*it).second.size);
    string expected((char*)value.data, value.size);
    EXPECT_TRUE(false);
  }
	++it;
}

TEST_TMPDIR(LookupIteratorEmptyDb,babudb)
{
  StringOrder sorder;
  vector<LogIndex*> logi;
  
	DataHolder lower("no0");
	DataHolder upper("no1");
	LookupIterator it_empty(logi, NULL, sorder, lower, upper);
  
	LookupIterator it_empty2(logi, NULL, sorder);
}

TEST_TMPDIR(LookupIterator,babudb)
{
  StringOrder sorder;
	DataHolder k1("key1"), v1("value1"), k2("key2"), v2("value2");
	DataHolder k3("key3"), v3("value3"), k4("key4"), v4("value4");

	{	// Create an ImmutableIndex
    IndexMerger* merger = new IndexMerger(testPath("testdb-testidx"), sorder);

	  merger->Add(1, k1, v1);
	  merger->Add(2, k2, v2);
	  merger->Add(3, k3, v3);
	  merger->Add(4, k4, v4);
 
    merger->Run();
    delete merger;
	}

  ImmutableIndex* loadedindex = ImmutableIndex::Load(testPath("testdb-testidx_4.idx"), 4, sorder);
	
	LogIndex li1(sorder, 1);
	LogIndex li2(sorder, 1);
	LogIndex li3(sorder, 1);

	vector<LogIndex*> logi; logi.push_back(&li1); logi.push_back(&li2); logi.push_back(&li3);

	DataHolder ovalue("overlayvalue");
	DataHolder k0("key0"), k5("key5");

	li1.Add(k1, ovalue);

	li2.Add(k0, Buffer::Deleted());
	li2.Add(k1, Buffer::Deleted());
	li2.Add(k4, Buffer::Deleted());
	li2.Add(k5, ovalue);

	li3.Add(k0, ovalue);
	li3.Add(k3, ovalue);

	// Now we have 1 immutable + 3 overlay indices
  //
  //      ImmutableIndex  li3        li2        li1
  //                      (2)        (1)        (0)
  // key0                 ovalue     XXX
  // key1 value1                     XXX        ovalue                   
  // key2 value2                  
  // key3 value3          ovalue
  // key4 value4                     XXX
  // key5                            ovalue

	{	// try empty iterator
		DataHolder lower("no0");
		DataHolder upper("no1");

		LookupIterator it_empty(logi, loadedindex, sorder, lower, upper);
		EXPECT_FALSE(it_empty.hasMore());
	}

	{	// only load ImmutableIndex
		LookupIterator it(vector<LogIndex*>(), loadedindex, sorder);
    
    EXPECT_KEY_ADVANCE(it, k1, v1);
    EXPECT_KEY_ADVANCE(it, k2, v2);
    EXPECT_KEY_ADVANCE(it, k3, v3);
    EXPECT_KEY_ADVANCE(it, k4, v4);
		EXPECT_FALSE(it.hasMore());
	}

	{	// now for a range query
		DataHolder lower("key1x");
		DataHolder upper("key4x");
		LookupIterator it(logi, loadedindex, sorder, lower, upper);

    EXPECT_KEY_ADVANCE(it, k2, v2);
    EXPECT_KEY_ADVANCE(it, k3, ovalue);
		EXPECT_FALSE(it.hasMore());
	}

	{	// and another one that equals the whole range
		DataHolder lower2("key");
		DataHolder upper2("key5x");
		LookupIterator it(logi, loadedindex, sorder, lower2, upper2);
    
    EXPECT_KEY_ADVANCE(it, k1, ovalue);
    EXPECT_KEY_ADVANCE(it, k2, v2);
    EXPECT_KEY_ADVANCE(it, k3, ovalue);
    EXPECT_KEY_ADVANCE(it, k5, ovalue);
		EXPECT_FALSE(it.hasMore());
	}

	{	// and as a full sweep
		LookupIterator it(logi, loadedindex, sorder);

    EXPECT_KEY_ADVANCE(it, k1, ovalue);
    EXPECT_KEY_ADVANCE(it, k2, v2);
    EXPECT_KEY_ADVANCE(it, k3, ovalue);
    EXPECT_KEY_ADVANCE(it, k5, ovalue);
		EXPECT_FALSE(it.hasMore());
	}

	{
		// now test starting with Deleted values
		li1.Add(k0,Buffer::Deleted());
		li1.Add(k1,Buffer::Deleted());
		li1.Add(k2,Buffer::Deleted());
		li1.Add(k3,Buffer::Deleted());
		li1.Add(k4,Buffer::Deleted());
		li1.Add(k5,Buffer::Deleted());

		DataHolder lower2("key");
		DataHolder upper2("key3");
		LookupIterator it2(logi, loadedindex, sorder, lower2, upper2);

		EXPECT_FALSE(it2.hasMore());
	}
}
