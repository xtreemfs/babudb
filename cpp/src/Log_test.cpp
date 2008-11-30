// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "Log.h"

#include "babudb/StringConfiguration.h"
#include "yield/platform/memory_mapped_file.h"
using YIELD_NS::MemoryMappedFile;
using namespace babudb;

#include "babudb/test.h"
#include "babudb/test_helper.h"

TEST_TMPDIR(Log,babudb)
{
	auto_ptr<Log> log(new Log(testPath("testlog")));

	LogSection* tail = log->getTail();

	tail->appendOperation(DummyOperation(1)); tail->commitOperations();
	tail->appendOperation(DummyOperation(2)); tail->commitOperations();
	tail->appendOperation(DummyOperation(3)); tail->commitOperations();

	log->advanceTail();
	tail = log->getTail();

	tail->appendOperation(DummyOperation(4)); tail->commitOperations();
	tail->appendOperation(DummyOperation(5)); tail->commitOperations();
	tail->appendOperation(DummyOperation(6)); tail->commitOperations();
	log->close();

	log.reset(new Log(testPath("testlog")));

	log->loadRequiredLogSections(0);

	EXPECT_EQUAL(log->getSections().size(), 2);
	log->close();
}
