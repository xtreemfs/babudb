// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef OG_H
#define OG_H

#include <string>
using std::string;
#include <vector>
using std::vector;
#include <map>
using std::map;

#include "LogSection.h"

namespace babudb {

class DataIndex;
class OperationFactory;
class LogIterator;
class DataIndexOperationTarget;

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

	LogIterator begin();
	LogIterator end();

	vector<LogSection*>& getSections() { return sections; }

private:
	vector<LogSection*> sections;

	LogSection* tail;
	string name_prefix;
};

};

#endif
