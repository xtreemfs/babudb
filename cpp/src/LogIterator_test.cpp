// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/StringConfiguration.h"
#include "babudb/test.h"
#include "babudb/test_helper.h"
#include "babudb/LogIterator.h"

#include "Log.h"

#include "yield/platform/memory_mapped_file.h"
using YIELD::MemoryMappedFile;
using namespace babudb;

TEST_TMPDIR(LogIterator,babudb)
{
	auto_ptr<Log> log(new Log(testPath("testlog")));

	Log::iterator i = log->begin();	// try an empty log first
	EXPECT_TRUE(i == log->end());
	Log::reverse_iterator r = log->rbegin();	
	EXPECT_TRUE(r == log->rend());

	LogSection* tail = log->getTail();

	tail->appendOperation(DummyOperation('A')); tail->commitOperations();
	tail->appendOperation(DummyOperation('B')); tail->commitOperations();

	log->advanceTail();
	tail = log->getTail();

	tail->appendOperation(DummyOperation('C')); tail->commitOperations();
	tail->appendOperation(DummyOperation('D')); tail->commitOperations();

	log->close();
	log.reset(new Log(testPath("testlog")));

	log->loadRequiredLogSections(0);

	EXPECT_TRUE(log->getSections().size() == 2);
	DummyOperation op(0);

	i = log->begin();
	EXPECT_TRUE(i != log->end());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'A');

	++i;
	EXPECT_TRUE(i != log->end());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'B');

	++i;
	EXPECT_TRUE(i != log->end());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'C');

	++i;
	EXPECT_TRUE(i != log->end());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'D');

	++i;
	EXPECT_TRUE(i == log->end());

	// now reverse

	i = log->end(); --i;
	EXPECT_TRUE(i != log->end());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'D');

	--i;
	EXPECT_TRUE(i != log->begin());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'C');

	--i;
	EXPECT_TRUE(i != log->begin());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'B');

	--i;
	EXPECT_TRUE(i == log->begin());
	EXPECT_TRUE(i.getType() != 0);
	EXPECT_TRUE(op.deserialize(*i).value == 'A');



	// now reverse iterator

	r = log->rbegin();
	EXPECT_TRUE(r != log->rend());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'D');

	++r;
	EXPECT_TRUE(r != log->rend());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'C');

	++r;
	EXPECT_TRUE(r != log->rend());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'B');

	++r;
	EXPECT_TRUE(r != log->rend());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'A');

	++r;
	EXPECT_TRUE(r == log->rend());

	// and reverse reverse
	
	r = log->rend(); --r;
	EXPECT_TRUE(r != log->rend());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'A');

	--r;
	EXPECT_TRUE(r != log->rend());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'B');

	--r;
	EXPECT_TRUE(r != log->rend());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'C');

	--r;
	EXPECT_TRUE(r == log->rbegin());
	EXPECT_TRUE(r.getType() != 0);
	EXPECT_TRUE(op.deserialize(*r).value == 'D');

	log->close();
}
