// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/test.h"

#include "SequentialFile.h"
#include "yield/platform/memory_mapped_file.h"
using namespace YIELD_NS;
using namespace babudb;

TEST_TMPDIR(SequentialFile_iteration,babudb)
{
	auto_ptr<MemoryMappedFile> file(new MemoryMappedFile(testPath("testfile").getHostCharsetPath(), 1024 * 1024, DOOF_CREATE|DOOF_READ|DOOF_WRITE|DOOF_SYNC));
	SequentialFile sf(file, NULL);

	EXPECT_TRUE(sf.begin() == sf.end());
	EXPECT_TRUE(sf.rbegin() == sf.rend());

	sf.append(1,1,false);
	sf.append(2,2,false);
	sf.append(3,3,false);
	sf.commit();

	sf.close();

	file.reset(new MemoryMappedFile(testPath("testfile").getHostCharsetPath(), 1024 * 1024, DOOF_READ));
	SequentialFile sf2(file, NULL);

	SequentialFile::iterator i = sf2.begin();
	EXPECT_TRUE(i.getType() == 1);
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 1);

	++i;
	EXPECT_TRUE(i.getType() == 2);
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 2);

	--i;
	EXPECT_TRUE(i.getType() == 1);
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 1);

	++i; ++i;
	EXPECT_TRUE(i.getType() == 3);
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 3);

	++i;
	EXPECT_TRUE(i == sf2.end());

	i = sf2.rbegin();
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 3);
	++i;
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 2);

	i = sf2.end(); --i;
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 3);
	--i;
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 2);

	i = sf2.rend(); --i;
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 1);
	--i;
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 2);
}

TEST_TMPDIR(SequentialFile_rollback,babudb)
{
	auto_ptr<MemoryMappedFile> file(new MemoryMappedFile(testPath("testfile").getHostCharsetPath(), 1024 * 1024, DOOF_CREATE|DOOF_READ|DOOF_WRITE|DOOF_SYNC));
	SequentialFile sf(file, NULL);

	sf.append(1,1,false);
	sf.append(2,2,false);
	sf.commit();
	sf.append(3,3,false);

	sf.close();

	file.reset(new MemoryMappedFile(testPath("testfile").getHostCharsetPath(), 1024 * 1024, DOOF_READ));
	SequentialFile sf2(file, NULL);

	SequentialFile::iterator i = sf2.begin();
	EXPECT_TRUE(i.getType() == 1);
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 1);

	++i;
	EXPECT_TRUE(i.getType() == 2);
	EXPECT_TRUE(i.getRecord()->getPayloadSize() == 2);

	++i;
	EXPECT_TRUE(i == sf2.end());
}
