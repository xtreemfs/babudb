// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/database.h"
#include "babudb/lookup_iterator.h"
#include "babudb/log/log_iterator.h"

#include "babudb/log/log.h"
#include "log_index.h" 
#include "merged_index.h"
#include "index/merger.h"
#include "babudb/test.h"
using namespace babudb;

#include <sstream>
using std::pair;

#include "yield/platform/memory_mapped_file.h"
using namespace YIELD;

Database::Database(const string& name)
  : name(name) {}

Database::~Database() {
  for(map<string,MergedIndex*>::iterator i = indices.begin(); i != indices.end(); ++i)
    delete i->second;
}

Database* Database::Open(const string& name, const vector<IndexDescriptor>& index_list) {
  Database* database = new Database(name);

  for(vector<IndexDescriptor>::const_iterator i = index_list.begin(); i != index_list.end(); ++i) {
    database->indices.insert(std::make_pair(i->first, new MergedIndex(name + "-" + i->first, *i->second)));
  }

  // for each index, load latest intact ImmutableIndex and record MSNs
  database->latest_lsn = 0;
  database->minimal_persistent_lsn = MAX_LSN;
  for(map<string,MergedIndex*>::iterator i = database->indices.begin(); i != database->indices.end(); ++i) {
    lsn_t last_persistent_lsn = i->second->GetLastPersistentLSN();
    database->minimal_persistent_lsn = std::min(database->minimal_persistent_lsn, last_persistent_lsn);
  }
  database->latest_lsn = database->minimal_persistent_lsn;

  return database;
}

void Database::Snapshot(const string& index_name) {
  MergedIndex* index = indices[index_name];
  index->Snapshot(latest_lsn);
}

void Database::CompactIndex(const string& index_name, lsn_t snapshot_lsn) {
  if (snapshot_lsn == indices[index_name]->GetLastPersistentLSN()) {
    return;  // nothing to do
  }

  LookupIterator snapshot = indices[index_name]->GetSnapshot(snapshot_lsn);
  ImmutableIndexWriter* writer = ImmutableIndex::Create(
      name + "-" + index_name, snapshot_lsn, 64*1024);

  while (snapshot.hasMore()) {
    writer->Add((*snapshot).first, (*snapshot).second);
    ++snapshot;
  }

  writer->Finalize();
}

void Database::Cleanup(const string& obsolete_prefix) {
  for(map<string,MergedIndex*>::iterator i = indices.begin(); i != indices.end(); ++i) {
    i->second->Cleanup(obsolete_prefix);
  }
}

lsn_t Database::GetCurrentLSN() const {
  return latest_lsn;
}

lsn_t Database::GetMinimalPersistentLSN() const {
  return minimal_persistent_lsn;
}

void Database::Add(const string& index_name, lsn_t change_lsn,
                   const Buffer& key, const Buffer& value) {
  // Operations can affect multiple indices, plus we can have LSNs that do not affect indices at all.
  EXPECT_TRUE(change_lsn >= latest_lsn);
  MergedIndex* index = indices[index_name];
  index->Add(key, value);
  latest_lsn = change_lsn;
}

void Database::Remove(const string& index_name, lsn_t change_lsn, const Buffer& key) {
  EXPECT_TRUE(change_lsn >= latest_lsn);
  MergedIndex* index = indices[index_name];
  index->Remove(key);
  latest_lsn = change_lsn;
}

Buffer Database::Lookup(const string& index, const Buffer& key) {
  return indices[index]->Lookup(key);
}

LookupIterator Database::Lookup(const string& index, const Buffer& lower, const Buffer& upper) {
  return indices[index]->Lookup(lower, upper);
}

vector<pair<string, lsn_t> > Database::GetIndexVersions() {
  vector<pair<string, lsn_t> > result;
  for(map<string,MergedIndex*>::iterator i = indices.begin(); i != indices.end(); ++i) {
    result.push_back(std::make_pair(i->first, i->second->GetLastPersistentLSN()));
  }
  return result;
}

std::vector<std::pair<string, string> > Database::GetIndexPaths() {
  vector<pair<string, string> > result;
  vector<pair<string, lsn_t> > versions = Database::GetIndexVersions();
  for (vector<pair<string, lsn_t> >::iterator i = versions.begin();
       i != versions.end(); ++i) {
    result.push_back(std::make_pair(i->first,
        ImmutableIndex::GetIndexName(name + "-" + i->first, i->second)));
  }
  return result;
}

int Database::ReadIndex(const string& index_name, lsn_t version,
                        int offset, char* buffer, int bytes) {
  ImmutableIndex* index = indices[index_name]->GetBase();
  if (index->GetLastLSN() != version)
    return -1;
  return index->Read(offset, buffer, bytes);
}