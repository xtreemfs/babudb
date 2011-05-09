// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/log/log_section.h"
#include "babudb/log/log_storage.h"
#include "babudb/buffer.h"

#include "yield/platform/assert.h"

using namespace babudb;

LogSection::LogSection(LogStorage* mmfile, lsn_t first)
    : SequentialFile(mmfile), first_lsn(first) { }

lsn_t LogSection::getFirstLSN() const {
	return first_lsn;
}

void LogSection::Append(const Serializable& entry) {
	void* write_location = getFreeSpace(RECORD_MAX_SIZE);
  entry.Serialize(Buffer(write_location, RECORD_MAX_SIZE));
  unsigned int payload_size = entry.GetSize();
  ASSERT_TRUE(payload_size <= RECORD_MAX_SIZE);  // be paranoid
  frameData(write_location, payload_size);
}

void LogSection::Commit() {
	commit();
}

void LogSection::Erase(const iterator& it) {
  erase(record2offset(it.GetRecord()));
}
