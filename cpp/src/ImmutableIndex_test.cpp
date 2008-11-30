// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include <utility>
using std::pair;

#include "Log.h"
#include "LogIndex.h"
#include "babudb/StringConfiguration.h"
#include "ImmutableIndexWriter.h"
#include "ImmutableIndex.h"
#include "yield/platform/memory_mapped_file.h"
using namespace YIELD_NS;
using namespace babudb;

#include "babudb/test.h"

TEST_TMPDIR(ImmutableIndex,babudb)
{
	unsigned long mmap_flags = DOOF_CREATE|DOOF_READ|DOOF_WRITE|DOOF_SYNC;

	auto_ptr<MemoryMappedFile> file(new MemoryMappedFile(testPath("test.idx").getHostCharsetPath(), 1024 * 1024, mmap_flags));

	ImmutableIndexWriter writer(file,2);

	DataHolder k1("key1"), v1("value1"), k2("key2"), v2("value2");
	writer.add(k1,v1);
	writer.add(k2,v2);

	DataHolder k3("key3"),v3("value3"),k4("key4"),v4("value4");
	writer.add(k3,v3);
	writer.add(k4,v4);

	writer.finalize();

	auto_ptr<MemoryMappedFile> file2(new MemoryMappedFile(testPath("test.idx").getHostCharsetPath(), 1024 * 1024, mmap_flags));
	SequentialFile seqfile(file2);

	StringOrder myorder;
	MapCompare maporder(myorder);
	map<Data,offset_t,MapCompare> index(maporder);
	ImmutableIndex::loadIndex(seqfile, index);

	EXPECT_TRUE(index.find(Data(k1)) != index.end());
	EXPECT_TRUE(index.find(Data(k3)) != index.end());
	EXPECT_TRUE(index.find(Data(k2)) == index.end());
	EXPECT_TRUE(index.find(Data(k4)) == index.end());

	DataHolder prefix("ke");
	EXPECT_TRUE(index.find(Data(prefix)) == index.end());
	seqfile.close();

	auto_ptr<MemoryMappedFile> file3(new MemoryMappedFile(testPath("test.idx").getHostCharsetPath(), 1024 * 1024, mmap_flags));
	ImmutableIndex idx(file3,myorder,0);
	idx.load();

	// search keys directly
	EXPECT_TRUE(!idx.lookup(k1).isEmpty());
	EXPECT_TRUE(!idx.lookup(k2).isEmpty());
	EXPECT_TRUE(idx.lookup(k2) == v2);
	EXPECT_TRUE(!idx.lookup(k3).isEmpty());
	EXPECT_TRUE(!idx.lookup(k4).isEmpty());

	// not founds
	DataHolder before("a");
	EXPECT_TRUE(idx.lookup(before).isEmpty());
	DataHolder middle("key21");
	EXPECT_TRUE(idx.lookup(middle).isEmpty());
	DataHolder after("x");
	EXPECT_TRUE(idx.lookup(after).isEmpty());


	// and iteration
	ImmutableIndex::iterator i = idx.begin();
	EXPECT_TRUE(!(*i).first.isEmpty());
	EXPECT_TRUE(i != idx.end());

	++i;
	EXPECT_TRUE(!(*i).first.isEmpty());
	EXPECT_TRUE(i != idx.end());

	++i;
	EXPECT_TRUE(!(*i).first.isEmpty());
	EXPECT_TRUE(i != idx.end());

	++i;
	EXPECT_TRUE(!(*i).first.isEmpty());
	EXPECT_TRUE(i != idx.end());

	++i;
	EXPECT_TRUE(i == idx.end());


	// matches
	ImmutableIndex::iterator pi = idx.find(prefix);
	EXPECT_TRUE(pi != idx.end());
	EXPECT_TRUE((*pi).first == "key1");
	EXPECT_TRUE(pi != idx.end());
	EXPECT_TRUE(idx.find(DataHolder("kez")) == idx.end() );
}
