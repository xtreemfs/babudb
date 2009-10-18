// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

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

	// now we have 1 pers + 3 overlays

	DataHolder ovalue("overlayvalue");
	DataHolder k0("key0"), k5("key5");

	li1.Add(k1,ovalue);

	li2.Add(k0,ovalue);
	li2.Add(k4,Buffer::Deleted());
	li2.Add(k5,ovalue);

	li3.Add(k0,Buffer::Deleted());
	li3.Add(k3,ovalue);

	{	// try empty iterator
		DataHolder lower("no0");
		DataHolder upper("no1");

		LookupIterator it_empty(logi, loadedindex, sorder, lower, upper);
		EXPECT_FALSE(it_empty.hasMore());
	}

	{	// now for a range query
		DataHolder lower("key1x");
		DataHolder upper("key4x");

		LookupIterator it(logi, loadedindex, sorder, lower, upper);

		EXPECT_TRUE(it.hasMore());
		EXPECT_EQUAL((*it).first, k2);
		EXPECT_EQUAL((*it).second, v2);
		++it;
		EXPECT_TRUE(it.hasMore());
		EXPECT_EQUAL((*it).first, k3);
		EXPECT_EQUAL((*it).second, ovalue);
		++it;
		EXPECT_FALSE(it.hasMore());
	}

	{	// and another one
		DataHolder lower2("key");
		DataHolder upper2("key5x");
		LookupIterator it2(logi, loadedindex, sorder, lower2, upper2);

		EXPECT_TRUE(it2.hasMore());
		EXPECT_EQUAL((*it2).first, k0);
		EXPECT_EQUAL((*it2).second, ovalue);
		++it2;
		EXPECT_TRUE(it2.hasMore());
		EXPECT_EQUAL((*it2).first, k1);
		EXPECT_EQUAL((*it2).second, ovalue);
		++it2;
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
