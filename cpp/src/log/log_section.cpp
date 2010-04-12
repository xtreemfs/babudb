// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/log/log_section.h"
#include "babudb/log/log_stats.h"
#include "babudb/log/log_storage.h"
#include "babudb/buffer.h"

#include "yield/platform/assert.h"

using namespace babudb;

LogSection::LogSection(auto_ptr<LogStorage> mmfile, lsn_t first)
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

lsn_t LogSection::StartTransaction() {
	if(!in_transaction) {
    // Write LSN frame
		in_transaction = true;
		lsn_t* write_location = (lsn_t*)getFreeSpace(sizeof(lsn_t));
		*write_location = next_lsn;
		frameData(write_location, sizeof(lsn_t), LSN_RECORD_TYPE);
		next_lsn++;
	}
  return next_lsn - 1;
}

lsn_t LogSection::Append(const Serializable& entry) {
  lsn_t lsn = StartTransaction();
	void* write_location = getFreeSpace(RECORD_MAX_SIZE);
  entry.Serialize(Buffer(write_location, RECORD_MAX_SIZE));
  frameData(write_location, (unsigned int)entry.GetSize(), USER_RECORD_TYPE + entry.GetType());
	return lsn;
}

void LogSection::Commit() {
	in_transaction = false;
	commit();
}
