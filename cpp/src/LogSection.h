// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef OGSECTION_H
#define OGSECTION_H

#include <string>
using std::string;

#include "SequentialFile.h"
#include "babudb/Operation.h"

namespace YIELD_NS { class MemoryMappedFile; }

namespace babudb {

class Operation;

class LogSection : public SequentialFile {
public:
	LogSection(auto_ptr<YIELD_NS::MemoryMappedFile>, lsn_t first);

	lsn_t getFirstLSN();
	lsn_t getLastLSN();

	lsn_t appendOperation(const Operation& operation);
	void commitOperations();

private:
	bool in_transaction;
	lsn_t first_lsn; // the first lsn in this file
	lsn_t next_lsn;  // the next lsn to write
};

};

#endif
