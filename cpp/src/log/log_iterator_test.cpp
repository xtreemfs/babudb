// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/profiles/string_key.h"
#include "babudb/test.h"
#include "babudb/test_helper.h"

#include "babudb/log/log_iterator.h"
#include "babudb/log/log.h"

#include "yield/platform/memory_mapped_file.h"
using YIELD::MemoryMappedFile;
using namespace babudb;

TEST_TMPDIR(LogIterator,babudb)
{
  {
    Log log(testPath("testlog"));

    Log::iterator i = log.begin();	// try an empty log first
    EXPECT_TRUE(i == log.end());
    Log::reverse_iterator r = log.rbegin();	
    EXPECT_TRUE(r == log.rend());

    LogSection* tail = log.getTail();

    tail->Append(DummyOperation('A')); tail->Commit();
    tail->Append(DummyOperation('B')); tail->Commit();

    log.advanceTail();
    tail = log.getTail();

    tail->Append(DummyOperation('C')); tail->Commit();
    tail->Append(DummyOperation('D')); tail->Commit();

    log.close();
  }
  {
    Log log(testPath("testlog"));

    log.loadRequiredLogSections(0);

    EXPECT_TRUE(log.getSections().size() == 2);
    DummyOperation op(0);

    Log::iterator i = log.begin();
    EXPECT_TRUE(i != log.end());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*i).value == 'A');
    EXPECT_TRUE(i.GetLSN() == 1);

    ++i;
    EXPECT_TRUE(i != log.end());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*i).value == 'B');
    EXPECT_TRUE(i.GetLSN() == 2);

    ++i;
    EXPECT_TRUE(i != log.end());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*i).value == 'C');
    EXPECT_TRUE(i.GetLSN() == 3);

    ++i;
    EXPECT_TRUE(i != log.end());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*i).value == 'D');
    EXPECT_TRUE(i.GetLSN() == 4);

    ++i;
    EXPECT_TRUE(i == log.end());

    // now reverse
    i = log.end(); --i;
    EXPECT_TRUE(i != log.end());
    EXPECT_TRUE(i.getType() != 0);
    EXPECT_TRUE(op.Deserialize(*i).value == 'D');

    --i;
    EXPECT_TRUE(i != log.begin());
    EXPECT_TRUE(i.getType() != 0);
    EXPECT_TRUE(op.Deserialize(*i).value == 'C');

    --i;
    EXPECT_TRUE(i != log.begin());
    EXPECT_TRUE(i.getType() != 0);
    EXPECT_TRUE(op.Deserialize(*i).value == 'B');

    --i;
    EXPECT_TRUE(i == log.begin());
    EXPECT_TRUE(i.getType() != 0);
    EXPECT_TRUE(op.Deserialize(*i).value == 'A');


    // now reverse iterator
    Log::reverse_iterator r = log.rbegin();
    EXPECT_TRUE(r != log.rend());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'D');

    ++r;
    EXPECT_TRUE(r != log.rend());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'C');

    ++r;
    EXPECT_TRUE(r != log.rend());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'B');

    ++r;
    EXPECT_TRUE(r != log.rend());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'A');

    ++r;
    EXPECT_TRUE(r == log.rend());

    // and reverse reverse
  
    r = log.rend(); --r;
    EXPECT_TRUE(r != log.rend());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'A');

    --r;
    EXPECT_TRUE(r != log.rend());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'B');

    --r;
    EXPECT_TRUE(r != log.rend());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'C');

    --r;
    EXPECT_TRUE(r == log.rbegin());
    EXPECT_EQUAL(i.getType(), DUMMY_OPERATION_TYPE);
    EXPECT_TRUE(op.Deserialize(*r).value == 'D');

    log.close();
  }
}
