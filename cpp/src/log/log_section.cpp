// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/log/log_section.h"
#include "babudb/log/log_stats.h"
#include "babudb/buffer.h"

#include "yield/platform/yunit.h"
#include "yield/platform/memory_mapped_file.h"

using namespace YIELD;
using namespace babudb;

#define LSN_RECORD_TYPE 0
#define USER_RECORD_TYPE 1

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

lsn_t LogSection::Append(const Serializable& entry) {
	if(!in_transaction) {
    // Write LSN frame
		in_transaction = true;
		lsn_t* write_location = (lsn_t*)getFreeSpace(sizeof(lsn_t));
		*write_location = next_lsn;
		frameData(write_location, sizeof(lsn_t), LSN_RECORD_TYPE);
		next_lsn++;
	}

	void* write_location = getFreeSpace(RECORD_MAX_SIZE);
  entry.Serialize(Buffer(write_location, RECORD_MAX_SIZE));
  frameData(write_location, (unsigned int)entry.GetSize(), USER_RECORD_TYPE);

	return next_lsn - 1;
}

void LogSection::Commit() {
	in_transaction = false;
	commit();
}
