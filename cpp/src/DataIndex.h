// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef ATAINDEX_H
#define ATAINDEX_H

#include <memory>
#include <utility>
#include <map>
#include <string>
#include <vector>
using namespace std;

#include "babudb/Operation.h"
#include "babudb/KeyOrder.h"

namespace babudb {
class LogSection;
class LogIndex;
class ImmutableIndex;
class KeyOrder;
class LookupIterator;
class Log;

class DataIndex {
public:
	explicit DataIndex(const std::string& name, KeyOrder& order);
	DataIndex(const DataIndex& other);
	~DataIndex();

	lsn_t loadLatestIntactImmutableIndex();
	void cleanup(lsn_t from_lsn, const string& to);

	typedef map<Data,Data,SimpleMapCompare> ResultMap;
	Data lookup(const Data& key);
	LookupIterator lookup(const Data&, const Data&);

	void advanceTail(lsn_t current_lsn);
	LogIndex* getTail();

	void advanceImmutableIndex(ImmutableIndex*);

	LogIndex* getMigratableIndex();
	ImmutableIndex *getMigrationBase();

	KeyOrder& getOrder() { return order; }

private:
	LogIndex*				tail;
	vector<LogIndex*>		log_indices;
	ImmutableIndex*			immutable_index;
	KeyOrder& order;
	string name_prefix;
};

class DataIndexOperationTarget : public OperationTarget {
public:
	DataIndexOperationTarget(Log* log, std::map<string,DataIndex*>& idcs, lsn_t l)
		: log(log), idcs(idcs), lsn(l) {}

	bool set(const string& index, const Data& key, const Data& value);
	bool remove(const string& index, const Data& key);
	void setLSN(lsn_t l) { lsn = l; }

private:
	lsn_t lsn;
	Log* log;
	std::map<string,DataIndex*>& idcs;
};


};

#endif
