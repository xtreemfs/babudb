// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_DATABASE_H
#define BABUDB_DATABASE_H

#include <utility>
#include <map>
#include <vector>
#include <string>
using std::string;

#include "Operation.h"

namespace babudb {

class KeyOrder;
class DataIndex;
class Log;
class LogIterator;
class IndexCreator;
class LookupIterator;

class Database {
public:
	Database(const string& name, const OperationFactory& op_f);
	~Database();

	void startup();
	void shutdown();

	void registerIndex(const string& name, KeyOrder& order);

	void execute(Operation& op);
	void commit();

	Data lookup(const string& index, const Data& key);
	LookupIterator lookup(const string& index, const Data& lower, const Data& upper);

	typedef LogIterator iterator;
	iterator begin();
	iterator end();
	iterator rbegin();
	iterator rend();

	/** Allow the deletion all log entries < from_lsn from the log
	*/

	void cleanup(lsn_t from_lsn, const string& to);

	/** Migrate the oldest volatile index to disk. Proceed n steps in this
		process. If finished, it returns true.
	*/

	bool migrate(const string& index, int steps);

private:
	friend class OperationTarget;

	Log* log;
	std::map<string,DataIndex*> indices;
	string name;
	const OperationFactory& factory;
	IndexCreator* merger;
};

};

#endif
