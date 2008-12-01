// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include <utility>
using std::pair;

#include "Log.h"
#include "LogIndex.h"
#include "ImmutableIndexWriter.h"
#include "ImmutableIndex.h"
#include "IndexMerger.h"
#include "babudb/StringConfiguration.h"
#include "yield/platform/memory_mapped_file.h"
using namespace YIELD;
using namespace babudb;

#include "babudb/test.h"

class DirectOperationTarget : public OperationTarget {
public:
	DirectOperationTarget(LogIndex* log_idx)
		: log_index(log_idx) {}

	void set(const string& index, const Data& key, const Data& value) {
		log_index->add(key,value);
	}

	void remove(const string& index, const Data& key) {
		log_index->add(key,Data::Deleted());
	}

private:
	LogIndex* log_index;
};


TEST_TMPDIR(ImmutableIndexWriter,babudb)
{
	unsigned long mmap_flags = DOOF_CREATE|DOOF_READ|DOOF_WRITE|DOOF_SYNC;

	// create ImmutableIndex from LogIndex

	StringOrder sorder;
	LogIndex log_index(sorder,1);
	DirectOperationTarget tgt(&log_index);

	StringSetOperation op1("", "Key1","data1");
	op1.addYourself(tgt);

	StringSetOperation op2("", "Key2","data2");
	op2.addYourself(tgt);

	StringSetOperation op3("", "Key3","data3");
	op3.addYourself(tgt);

	StringSetOperation op4("", "Key4","data4");
	op4.addYourself(tgt);

	auto_ptr<MemoryMappedFile> file(new MemoryMappedFile(testPath("test.idx").getHostCharsetPath(), 1024 * 1024, mmap_flags));
	auto_ptr<ImmutableIndexWriter> writer(new ImmutableIndexWriter(file,2));

	IndexCreator merger(writer, log_index, sorder);
	merger.run();

	// and load it again and check

	auto_ptr<MemoryMappedFile> file2(new MemoryMappedFile(testPath("test.idx").getHostCharsetPath(), 1024 * 1024, DOOF_READ));
	ImmutableIndex loadedindex(file2,sorder,0);
	loadedindex.load();

	EXPECT_TRUE(!loadedindex.lookup(DataHolder("Key1")).isEmpty());
	EXPECT_TRUE(!loadedindex.lookup(DataHolder("Key2")).isEmpty());
	EXPECT_TRUE(!loadedindex.lookup(DataHolder("Key3")).isEmpty());
	EXPECT_TRUE(!loadedindex.lookup(DataHolder("Key4")).isEmpty());


	// create another LogIndex

	LogIndex log_index2(sorder, 1);
	DirectOperationTarget tgt2(&log_index2);

	StringSetOperation op12("", "Key12","data21");
	op12.addYourself(tgt2);

	StringSetOperation op22("", "Key22","data22");
	op22.addYourself(tgt2);

	StringSetOperation op32("", "Key32","data32");
	op32.addYourself(tgt2);

	StringSetOperation op42("", "Key42","data42");
	op42.addYourself(tgt2);

	// and merge it

	auto_ptr<MemoryMappedFile> filetarget(new MemoryMappedFile(testPath("test_t.idx").getHostCharsetPath(), 1024 * 1024, mmap_flags));
	auto_ptr<ImmutableIndexWriter> writer2(new ImmutableIndexWriter(filetarget,2));

	IndexMerger merger2(writer2, log_index2, loadedindex, sorder);
	merger2.run();

	auto_ptr<MemoryMappedFile> file3(new MemoryMappedFile(testPath("test_t.idx").getHostCharsetPath(), 1024 * 1024, DOOF_READ));
	ImmutableIndex idx(file3,sorder,0);
	idx.load();

	EXPECT_TRUE(!idx.lookup(DataHolder("Key1")).isEmpty());
	EXPECT_TRUE(!idx.lookup(DataHolder("Key2")).isEmpty());
	EXPECT_TRUE(!idx.lookup(DataHolder("Key3")).isEmpty());

	EXPECT_TRUE(idx.lookup(DataHolder("Key123")).isEmpty());

	EXPECT_TRUE(!idx.lookup(DataHolder("Key12")).isEmpty());
	EXPECT_TRUE(!idx.lookup(DataHolder("Key22")).isEmpty());
	EXPECT_TRUE(!idx.lookup(DataHolder("Key32")).isEmpty());
}
