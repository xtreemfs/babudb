// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// The core Database class. A set of persistent and volatile key-value indices that
// represent the application-managed log state up to a certain LSN.
//
// The application manages a log of operations that can be identified by their 
// LSNs (log sequence numbers). These operations change the database state via
// Add() and Remove().

// The database maintains a set if indices that are persistent up to a certain LSN and
// extended with volatile overlay indices. After Open() the application has to replay its
// log from GetMinimalLSN() + 1 against the database to re-establish the current state.

// Volatile index overlays can be merged into the persistent index with the Migrate()
// call. The application marks LSNs which form a complete unit to be merged with the
// MarkMergeUnit() call.

// TODO: 
// - implement index with non-unique keys (for full-text indices)
//   can be implemented as non-unique key or multi-value index.
//   needs a Remove(key, value)


#ifndef BABUDB_DATABASE_H
#define BABUDB_DATABASE_H

#include <utility>
#include <map>
#include <vector>
#include <string>
using std::string;

#include "babudb/buffer.h"

namespace babudb {

class KeyOrder;
class MergedIndex;
class IndexCreator;
class LookupIterator;
class IndexMerger;

typedef std::pair<string, KeyOrder*> IndexDescriptor;

class Database {
 public:
  // Register the indices and open the database
  static Database* Open(const string& name, const std::vector<IndexDescriptor>& indices);
  ~Database();

  void Add(const string& index_name, lsn_t lsn, const Buffer& key, const Buffer& value);
  void Remove(const string& index_name, lsn_t lsn, const Buffer& key);

  // Single key lookup
  Buffer Lookup(const string& index, const Buffer& key);
  // Prefix lookup
  LookupIterator Lookup(const string& index, const Buffer& lower, const Buffer& upper);

  // The next Add or Remove call needs to have change_lsn = GetCurrentlLSN() + 1
  lsn_t GetCurrentLSN();
  // Called after Open to find out from where on to replay the log.
  // Also any log merges need to start from here.
  lsn_t GetMinimalPersistentLSN();

  // TODO: get merger from here, maybe even multi-index merger
  IndexMerger* GetMerger(const string& name);
  void Cleanup(const string& obsolete_prefix);

private:
  Database(const string& name);

  std::map<string,MergedIndex*> indices;
  string name;

  lsn_t latest_lsn;  // the latest known LSN in the Database state
  lsn_t minimal_persistent_lsn;  // the minimum persistent LSN in all indices
};

};  // namespace babudb

#endif
