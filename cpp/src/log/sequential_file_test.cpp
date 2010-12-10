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

	EXPECT_TRUE(sf.First().GetNext() == NULL);
	EXPECT_TRUE(sf.Last().GetPrevious() == NULL);

	sf.append(1,1);
	sf.append(2,2);
	sf.append(3,3);
	sf.commit();

	sf.close();

  file = PersistentLogStorage::OpenReadOnly(testPath("testfile").getHostCharsetPath());
	SequentialFile sf2(file, NULL);

	SequentialFile::iterator i = sf2.First();
  EXPECT_FALSE(i.IsValid());
  EXPECT_TRUE(i.GetNext() != NULL);
	EXPECT_TRUE(i.GetType() == 1);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 1);
  
  EXPECT_TRUE(i.GetNext() != NULL);
	EXPECT_TRUE(i.GetType() == 2);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 2);
  
  EXPECT_TRUE(i.GetPrevious() != NULL);
	EXPECT_TRUE(i.GetType() == 1);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 1);
  
  EXPECT_TRUE(i.GetNext() != NULL);
  EXPECT_TRUE(i.GetNext() != NULL);
	EXPECT_TRUE(i.GetType() == 3);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 3);
  
  EXPECT_FALSE(i.GetNext() != NULL);

	i = sf2.Last();
  EXPECT_FALSE(i.IsValid());
  EXPECT_TRUE(i.GetPrevious() != NULL);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 3);
  EXPECT_TRUE(i.GetPrevious() != NULL);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 2);
  EXPECT_TRUE(i.GetPrevious() != NULL);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 1);

  EXPECT_FALSE(i.GetPrevious() != NULL);
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
  
	SequentialFile::iterator i = sf2.First();
  EXPECT_TRUE(i.GetNext() != NULL);
  EXPECT_TRUE(i.IsValid());
	EXPECT_TRUE(i.GetType() == 1);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 1);
  
  EXPECT_TRUE(i.GetNext() != NULL);
	EXPECT_TRUE(i.GetType() == 2);
	EXPECT_TRUE(i.GetRecord()->getPayloadSize() == 2);
  
  EXPECT_FALSE(i.GetNext() != NULL);
}
