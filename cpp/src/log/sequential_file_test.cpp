// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/log/sequential_file.h"

#include "babudb/test.h"
#include "yield/platform/disk_operations.h"

#define POSIX  // for O_ definitions from fcntl.h
#include <fcntl.h>

using namespace YIELD;
using namespace babudb;

TEST_TMPDIR(SequentialFile_iteration,babudb)
{
  LogStorage* file = PersistentLogStorage::Open(testPath("testfile").getHostCharsetPath());
	SequentialFile sf(file, NULL);

	EXPECT_TRUE(sf.begin() == sf.end());
	EXPECT_TRUE(sf.rbegin() == sf.rend());

	sf.append(1,1);
	sf.append(2,2);
	sf.append(3,3);
	sf.commit();

	sf.close();

  file = PersistentLogStorage::OpenReadOnly(testPath("testfile").getHostCharsetPath());
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
  LogStorage* file = PersistentLogStorage::Open(testPath("testfile").getHostCharsetPath());
	SequentialFile sf(file, NULL);

	sf.append(1,1);
	sf.append(2,2);
	sf.commit();
	sf.append(3,3);

	sf.close();

  file = PersistentLogStorage::OpenReadOnly(testPath("testfile").getHostCharsetPath());
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
