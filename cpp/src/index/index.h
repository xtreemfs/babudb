// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// ImmutableIndex implements a persistent immutable search tree. It represents
// the state for an index up to a LSN.
// Indices are named with the following scheme:
//     databasename-indexname_lastlsn.idx

#ifndef BABUB_IMMUTABLEINDEX_H
#define BABUB_IMMUTABLEINDEX_H

#include <map>
#include <utility>
#include <vector>
#include <memory>
using namespace std;

#include "babudb/key.h"
#include "babudb/log/sequential_file.h"

namespace YIELD {
class Path;
}

namespace babudb {

class LogIndex;
class ImmutableIndexWriter;

class ImmutableIndex {
public:
	static ImmutableIndex* Load(
      const string& file_name, lsn_t lsn, const KeyOrder& order);
  static ImmutableIndexWriter* Create(
      const string& name, lsn_t lsn, size_t chunk_size);

	typedef class ImmutableIndexIterator iterator;
	Buffer Lookup(Buffer search_key);
	iterator Find(Buffer key);

	iterator begin() const;
	iterator end() const;

	lsn_t GetLastLSN() { return latest_lsn; }

  typedef vector<pair<YIELD::Path,lsn_t> > DiskIndices;
  static DiskIndices FindIndices(const string& name_prefix);
  static ImmutableIndex* LoadLatestIntactIndex(DiskIndices& on_disk, 
                                               const KeyOrder& order);
  void CleanupObsolete(const string& file_name, 
                       const string& obsolete_prefix); 

private:
	ImmutableIndex(auto_ptr<LogStorage> mm, const KeyOrder& order, lsn_t);
  bool LoadRoot();
	typedef std::map<Buffer,offset_t,MapCompare> Tree;

	Tree::iterator findChunk(const Buffer& key);
	offset_t* getOffsetTable(offset_t offset_rec_offset);

	SequentialFile storage;
	Tree index;
	const KeyOrder& order;
	lsn_t latest_lsn;
};

};

#endif
