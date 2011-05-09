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

TEST_TMPDIR(LogIteratorEmptyLog,babudb)
{
  {
    Log log(testPath("testlog"));

    std::auto_ptr<Log::iterator> i(log.First());	// try an empty log first
    EXPECT_FALSE(i->IsValid());
    EXPECT_FALSE((bool)i->GetNext());
    EXPECT_FALSE((bool)i->GetNext());
    EXPECT_FALSE(i->IsValid());
    EXPECT_FALSE((bool)i->GetPrevious());
    EXPECT_FALSE((bool)i->GetPrevious());
    EXPECT_FALSE(i->IsValid());
    i.reset(log.Last());
    EXPECT_FALSE(i->IsValid());
    EXPECT_FALSE((bool)i->GetNext());
    EXPECT_FALSE((bool)i->GetNext());
    EXPECT_FALSE(i->IsValid());
    EXPECT_FALSE((bool)i->GetPrevious());
    EXPECT_FALSE((bool)i->GetPrevious());
    EXPECT_FALSE(i->IsValid());
  }
}

TEST_TMPDIR(LogIterator,babudb)
{
  {
    Log log(testPath("testlog"));
    LogSection* tail = log.GetTail(1);

    tail->Append(DummyOperation('A')); tail->Commit();
    tail->Append(DummyOperation('B')); tail->Commit();

    log.AdvanceTail();
    tail = log.GetTail(3);

    tail->Append(DummyOperation('C')); tail->Commit();
    tail->Append(DummyOperation('D')); tail->Commit();

    log.Close();
  }
  {
    Log log(testPath("testlog"));

    log.Open(0);

    EXPECT_TRUE(log.NumberOfSections() == 2);
    DummyOperation op(0);
    
    std::auto_ptr<Log::iterator> i(log.First());
    EXPECT_FALSE(i->IsValid());
    EXPECT_TRUE(i->GetNext());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'A');
    
    EXPECT_TRUE(i->GetNext());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'B');
    
    EXPECT_TRUE(i->GetNext());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'C');
    
    EXPECT_TRUE(i->GetNext());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'D');
    
    EXPECT_FALSE((bool)i->GetNext());
    EXPECT_FALSE(i->IsValid());
    EXPECT_FALSE((bool)i->GetNext());  // idempotent
    EXPECT_FALSE(i->IsValid());
    
    EXPECT_TRUE(i->GetPrevious());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'D');

    // now reverse
    i.reset(log.Last());
    EXPECT_FALSE(i->IsValid());
    EXPECT_TRUE(i->GetPrevious());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'D');
    
    EXPECT_TRUE(i->GetPrevious());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'C');
    
    EXPECT_TRUE(i->GetPrevious());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'B');
    
    EXPECT_TRUE(i->GetPrevious());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'A');
    
    EXPECT_FALSE((bool)i->GetPrevious());
    EXPECT_FALSE(i->IsValid());
    EXPECT_FALSE((bool)i->GetPrevious());  // idempotent
    EXPECT_FALSE(i->IsValid());
    
    EXPECT_TRUE(i->GetNext());
    EXPECT_TRUE(i->IsValid());
    EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'A');

    log.Close();
  }
}

TEST_TMPDIR(LogIteratorOneSectionAndErase,babudb)
{
  Log log(testPath("testlog"));
  LogSection* tail = log.GetTail(1);
  
  tail->Append(DummyOperation('A')); tail->Commit();
  tail->Append(DummyOperation('B')); tail->Commit();
  tail->Append(DummyOperation('C')); tail->Commit();
  
  DummyOperation op(0);    
  std::auto_ptr<Log::iterator> i(log.First());
  EXPECT_FALSE(i->IsValid());
  EXPECT_TRUE(i->GetNext());
  EXPECT_TRUE(i->IsValid());
  EXPECT_TRUE(i->GetNext());
  EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'B');

  tail->Erase(i->GetRecordIterator());
  
  EXPECT_TRUE(i->GetNext());
  EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'C');
  
  EXPECT_TRUE(i->GetPrevious());
  EXPECT_TRUE(op.Deserialize(i->AsData()).value == 'A');
  
  log.Close();
}
