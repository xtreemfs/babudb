// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef OGINDEX_H
#define OGINDEX_H

#include <yield/platform/yunit.h>

#include "babudb/KeyOrder.h"
#include "babudb/Operation.h"
#include "SequentialFile.h"

#include <map>
using std::map;
#include <vector>
using std::vector;
#include <utility>

namespace babudb {

class LogSection;
class LogIndex;

class LogIndex {
public:
	LogIndex(KeyOrder& order, lsn_t first) : order(order), latest_value(MapCompare(order)), first_lsn(first), last_lsn(0) {}

	Data lookup(const Data& key);
//	vector<std::pair<Data,Data> > search(Data value); // needs value comp. operator

	bool add(const Data&, const Data&);

	lsn_t getFirstLSN()			{ return first_lsn; }
	void setLastLSN(lsn_t l)	{ last_lsn = l; }
	lsn_t getLastLSN()			{ return last_lsn; }	// only valid for closed LogIndices

	typedef map<Data,Data,MapCompare> Tree;
	typedef Tree::iterator iterator;

	iterator begin() { return latest_value.begin(); }
	iterator end()	 { return latest_value.end(); }

	iterator find(const Data& key)
	{
		return latest_value.lower_bound(key);
	}

private:
	lsn_t first_lsn, last_lsn;

	Tree latest_value;
	KeyOrder& order;
};

};

#endif
