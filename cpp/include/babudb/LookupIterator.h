// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_LOOKUPITERATOR_H
#define BABUDB_LOOKUPITERATOR_H

#include <vector>
using std::vector;
#include <map>
using std::map;

#include "babudb/KeyOrder.h"
#include "babudb/Operation.h"

namespace babudb {

class LogIndex;
class ImmutableIndex;
class Data;
class ImmutableIndexIterator;

class LookupIterator {
public:
	LookupIterator(const vector<LogIndex*>& idx, ImmutableIndex* iidx, const KeyOrder& order, const Data& start_key, const Data& end_key);
	~LookupIterator();

	void operator ++ ();
	std::pair<Data,Data> operator * ();

	bool hasMore();

private:
	void findMinimalIterator();
	void advanceIterator(int);
	void assureNonDeletedCursor();

	int current_depth;
	vector<map<Data,Data,MapCompare>::iterator> logi_it;		// MSI to LSI
	const KeyOrder& order;
	const Data& end_key;
	vector<LogIndex*> logi;
	ImmutableIndex* iidx;
	ImmutableIndexIterator* iidx_it;
};

};

#endif
