// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "LogSection.h"
#include "LogStats.h"
#include "babudb/Operation.h"

#include "yield/platform/yunit.h"
#include "yield/platform/memory_mapped_file.h"

using namespace YIELD;
using namespace babudb;

#define LSN_RECORD_TYPE 0

LogSection::LogSection(auto_ptr<MemoryMappedFile> mmfile, lsn_t first)
: SequentialFile(mmfile, new LogStats()), in_transaction(false), first_lsn(first), next_lsn(0) {
	// set or retrieve last LSN
	if(empty()) {
		next_lsn = first;
	} else {
		for(SequentialFile::iterator i = rbegin(); i != rend(); ++i) {
			if(i.getType() == LSN_RECORD_TYPE) {
				ASSERT_TRUE(i.getRecord()->getPayloadSize() == sizeof(lsn_t));
				next_lsn = *(lsn_t*)i.getRecord()->getPayload() + 1;
				break;
			}
		}
	}
}

lsn_t LogSection::getFirstLSN() {
	return first_lsn;
}

lsn_t LogSection::getLastLSN() {
	ASSERT_TRUE(next_lsn != 0);
	return next_lsn - 1;
}

lsn_t LogSection::appendOperation(const Operation& operation) {
	if(!in_transaction) {
		in_transaction = true;
		lsn_t* write_location = (lsn_t*)getFreeSpace(sizeof(lsn_t));
		*write_location = next_lsn;
		frameData(write_location, sizeof(lsn_t), LSN_RECORD_TYPE);
		next_lsn++;
	}

	void* write_location = getFreeSpace(OPERATION_MAX_SIZE);
	Data record_data = operation.serialize(Data(write_location, OPERATION_MAX_SIZE));
	frameData(record_data.data, (unsigned int)record_data.size, operation.getType());

	return next_lsn - 1;
}

void LogSection::commitOperations() {
	in_transaction = false;
	commit();
}
