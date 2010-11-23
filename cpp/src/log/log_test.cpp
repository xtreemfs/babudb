// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/log/log.h"

#include "babudb/profiles/string_key.h"
#include "yield/platform/memory_mapped_file.h"
using YIELD::MemoryMappedFile;
using namespace babudb;

#include "babudb/test.h"
#include "babudb/test_helper.h"

void WriteToLog(Log* log) {
	LogSection* tail = log->getTail();

	ASSERT_TRUE(tail->Append(DummyOperation(1)) == 1); 
  tail->Commit();

 	log->advanceTail();
	tail = log->getTail();
  ASSERT_TRUE(tail->StartTransaction() == 2);
	ASSERT_TRUE(tail->Append(DummyOperation(2)) == 2); 
  ASSERT_TRUE(tail->StartTransaction() == 2);
  tail->Commit();
	ASSERT_TRUE(tail->Append(DummyOperation(3)) == 3); 
  tail->Commit();

	log->advanceTail();
	tail = log->getTail();
	tail->Append(DummyOperation(4)); tail->Commit();
	tail->Append(DummyOperation(5)); tail->Commit();
	tail->Append(DummyOperation(6)); tail->Commit();
}

TEST_TMPDIR(Log,babudb)
{
  {
	  Log log(testPath("testlog"));
    WriteToLog(&log);
	  log.close();
  }
  {
	  Log log(testPath("testlog"));
	  log.loadRequiredLogSections(0);
	  EXPECT_EQUAL(log.NumberOfSections(), 3);
	  log.close();
  }
  {
	  Log log(testPath("testlog"));
	  log.loadRequiredLogSections(1);
	  EXPECT_EQUAL(log.NumberOfSections(), 2);
	  log.close();
  }
  {
	  Log log(testPath("testlog"));
  	log.loadRequiredLogSections(2);
  	EXPECT_EQUAL(log.NumberOfSections(), 2);
  	log.close();
  }
}

TEST_TMPDIR(LogVolatile,babudb) {
  Buffer empty_buffer = Buffer::Empty();
	Log log(empty_buffer);
  WriteToLog(&log);
}