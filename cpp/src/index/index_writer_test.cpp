// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, 2010 Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include <utility>
using std::pair;

#include "babudb/profiles/string_key.h"
#include "yield/platform/memory_mapped_file.h"
#include "index/index_writer.h"
#include "index/index.h"
#include "index/merger.h"
#include "log_index.h"
using namespace yield;
using namespace babudb;

#include "babudb/test.h"

TEST_TMPDIR(ImmutableIndexWriter,babudb)
{
  // create ImmutableIndex from LogIndex
  StringOrder sorder;
  IndexMerger* merger = new IndexMerger(testPath("testdb-testidx"), sorder);

  merger->Add(1, Buffer("key1"), Buffer("data1"));
  merger->Add(2, Buffer("key2"), Buffer("data2"));
  merger->Add(3, Buffer("key3"), Buffer("data3"));
  merger->Add(4, Buffer("key4"), Buffer("data4"));
  merger->Add(4, Buffer("key5"), Buffer::Empty());
  
  merger->Run();
  delete merger;

  // load it and check
  ImmutableIndex::DiskIndices indices = ImmutableIndex::FindIndices(
      testPath("testdb-testidx"));
  ImmutableIndex* loadedindex = ImmutableIndex::LoadLatestIntactIndex(indices, sorder);
  EXPECT_EQUAL(loadedindex->GetLastLSN(), 4);
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key1")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key2")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key3")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key4")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key5")).isNotExists());
  EXPECT_TRUE(loadedindex->Lookup(Buffer("key5")).isEmpty());

  // create another LogIndex
  merger = new IndexMerger(testPath("testdb-testidx"), sorder, loadedindex);

  merger->Add(5, Buffer("key12"), Buffer("data12"));
  merger->Add(6, Buffer("key22"), Buffer("data22"));
  merger->Add(7, Buffer("key32"), Buffer("data32"));
  merger->Add(8, Buffer("key42"), Buffer("data42"));
  
  merger->Run();
  delete merger;
  delete loadedindex;

  // load it and check
  indices = ImmutableIndex::FindIndices(testPath("testdb-testidx"));
  loadedindex = ImmutableIndex::LoadLatestIntactIndex(indices, sorder);
  EXPECT_EQUAL(loadedindex->GetLastLSN(), 8);
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key1")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key2")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key3")).isEmpty());
  EXPECT_TRUE(loadedindex->Lookup(Buffer("key5")).isEmpty());

  EXPECT_TRUE(loadedindex->Lookup(Buffer("key123")).isNotExists());

  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key12")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key22")).isEmpty());
  EXPECT_TRUE(!loadedindex->Lookup(Buffer("key32")).isEmpty());
}

TEST_TMPDIR(ImmutableIndexWriterEmpty,babudb)
{
  // Create an empty index
  ImmutableIndexWriter* writer = ImmutableIndex::Create(
      testPath("testdb-empty"), 9, 64*1024);
  writer->Finalize();
  delete writer;
  
  // And load it again
  StringOrder sorder;
  ImmutableIndex::DiskIndices indices = ImmutableIndex::FindIndices(
      testPath("testdb-empty"));
  ImmutableIndex* loadedindex = ImmutableIndex::LoadLatestIntactIndex(indices, sorder);
  EXPECT_EQUAL(loadedindex->GetLastLSN(), 9);
  EXPECT_TRUE(loadedindex->Lookup(Buffer("key123")).isNotExists());
}
