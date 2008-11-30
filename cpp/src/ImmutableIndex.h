// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef MMUTABLEINDEX_H
#define MMUTABLEINDEX_H

#include <map>
#include <utility>
#include <vector>
#include <memory>
using namespace std;

#include "babudb/KeyOrder.h"
#include "SequentialFile.h"

namespace YIELD_NS { class MemoryMappedFile; }

namespace babudb {

class LogIndex;
class ImmutableIndexWriter;

class ImmutableIndex {
public:
	ImmutableIndex(auto_ptr<YIELD_NS::MemoryMappedFile> mm, KeyOrder& order, lsn_t);

	bool checkHealth();
	void load();

	static void loadIndex(SequentialFile& storage, std::map<Data,offset_t,MapCompare>& index);

	typedef class ImmutableIndexIterator iterator;
	Data lookup(Data search_key);
	iterator find(Data key);

	iterator begin();
	iterator end();

	lsn_t getLastLSN() { return latest_lsn; }

private:
	typedef std::map<Data,offset_t,MapCompare> Tree;

	Tree::iterator findChunk(const Data& key);
	offset_t* getOffsetTable(offset_t offset_rec_offset);

	SequentialFile storage;
	Tree index;
	KeyOrder& order;
	lsn_t latest_lsn;
};

};

#endif
