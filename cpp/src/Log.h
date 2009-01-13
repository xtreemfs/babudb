// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef OG_H
#define OG_H

#include <string>
using std::string;
#include <vector>
#include <map>

#include "LogSection.h"
#include "babudb/LogIterator.h"

namespace babudb {

class DataIndex;
class OperationFactory;
class DataIndexOperationTarget;
template <class T> class LogIterator;

class Log {
public:
	Log(const string& name_prefix);
	~Log();

	void loadRequiredLogSections(lsn_t);
	void replayToLogIndices(DataIndexOperationTarget& indices, const OperationFactory&, lsn_t min_lsn);
	void close();

	void cleanup(lsn_t from_lsn, const string& to);

	LogSection* getTail();
	void advanceTail();
	lsn_t getLastLSN();

	typedef LogIteratorForward iterator;
	typedef LogIteratorBackward reverse_iterator;

	iterator begin();
	iterator end();

	reverse_iterator rbegin();
	reverse_iterator rend();

	std::vector<LogSection*>& getSections() { return sections; }

private:
	std::vector<LogSection*> sections;

	LogSection* tail;
	string name_prefix;
};

};

#endif
