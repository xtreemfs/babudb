// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include <utility>
using std::pair;

#include "Log.h"
#include "LogIndex.h"
#include "babudb/StringConfiguration.h"
#include "babudb/LookupIterator.h"
#include "ImmutableIndexWriter.h"
#include "ImmutableIndex.h"
#include "yield/platform/memory_mapped_file.h"
using namespace YIELD;
using namespace babudb;

#include "babudb/test.h"

TEST_TMPDIR(LookupIterator,babudb)
{
	DataHolder k1("key1"), v1("value1"), k2("key2"), v2("value2");
	DataHolder k3("key3"), v3("value3"), k4("key4"), v4("value4");

	unsigned long mmap_flags = O_CREAT|O_RDWR|O_SYNC;

	{	// Create an ImmutableIndex
		auto_ptr<MemoryMappedFile> file(new MemoryMappedFile(testPath("test.idx").getHostCharsetPath(), 1024 * 1024, mmap_flags));

		ImmutableIndexWriter writer(file,2);

		writer.add(k1,v1);
		writer.add(k2,v2);

		writer.add(k3,v3);
		writer.add(k4,v4);

		writer.finalize();
	}

	StringOrder myorder;
	auto_ptr<MemoryMappedFile> file2(new MemoryMappedFile(testPath("test.idx").getHostCharsetPath(), 1024 * 1024, mmap_flags));
	ImmutableIndex idx(file2,myorder,0);
	idx.load();

	LogIndex li1(myorder,1);
	LogIndex li2(myorder,1);
	LogIndex li3(myorder,1);

	vector<LogIndex*> logi; logi.push_back(&li1); logi.push_back(&li2); logi.push_back(&li3);

	// now we have 1 pers + 3 overlays

	DataHolder ovalue("overlayvalue");
	DataHolder k0("key0"), k5("key5");

	li1.add(k1,ovalue);

	li2.add(k0,ovalue);
	li2.add(k4,Data::Deleted());
	li2.add(k5,ovalue);

	li3.add(k0,Data::Deleted());
	li3.add(k3,ovalue);

	{	// try empty iterator
		DataHolder lower("no0");
		DataHolder upper("no1");

		LookupIterator it_empty(logi, &idx, myorder, lower, upper);
		EXPECT_FALSE(it_empty.hasMore());
	}

	{	// now for a range query
		DataHolder lower("key1x");
		DataHolder upper("key4x");

		LookupIterator it(logi, &idx, myorder, lower, upper);

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
		LookupIterator it2(logi, &idx, myorder, lower2, upper2);

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
		li1.add(k0,Data::Deleted());
		li1.add(k1,Data::Deleted());
		li1.add(k2,Data::Deleted());
		li1.add(k3,Data::Deleted());
		li1.add(k4,Data::Deleted());
		li1.add(k5,Data::Deleted());

		DataHolder lower2("key");
		DataHolder upper2("key3");
		LookupIterator it2(logi, &idx, myorder, lower2, upper2);

		EXPECT_FALSE(it2.hasMore());
	}
}
