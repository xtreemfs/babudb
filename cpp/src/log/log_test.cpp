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

TEST_TMPDIR(Log,babudb)
{
	auto_ptr<Log> log(new Log(testPath("testlog")));

	LogSection* tail = log->getTail();

	tail->Append(DummyOperation(1)); tail->Commit();
	tail->Append(DummyOperation(2)); tail->Commit();
	tail->Append(DummyOperation(3)); tail->Commit();

	log->advanceTail();
	tail = log->getTail();

	tail->Append(DummyOperation(4)); tail->Commit();
	tail->Append(DummyOperation(5)); tail->Commit();
	tail->Append(DummyOperation(6)); tail->Commit();
	log->close();

	log.reset(new Log(testPath("testlog")));

	log->loadRequiredLogSections(0);

	EXPECT_EQUAL(log->getSections().size(), 2);
	log->close();
}
