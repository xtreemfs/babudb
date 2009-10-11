// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// The LogSection augments the records of a SequentialFile with LSNs.

#ifndef LOG__LOG_SECTION_H
#define LOG__LOG_SECTION_H

#include <string>
using std::string;

#include "babudb/log/sequential_file.h"
#include "babudb/buffer.h"

namespace YIELD { class MemoryMappedFile; }

namespace babudb {

class Serializable {
public:
  virtual size_t GetSize() const = 0;
  virtual void Serialize(const Buffer& buffer) const = 0;
};

class LogSection : public SequentialFile {
public:
	LogSection(auto_ptr<YIELD::MemoryMappedFile>, lsn_t first);

	lsn_t getFirstLSN();
	lsn_t getLastLSN();

	lsn_t Append(const Serializable& entry);
	void Commit();

private:
	bool in_transaction;
	lsn_t first_lsn; // the first lsn in this file
	lsn_t next_lsn;  // the next lsn to write
};

};

#endif
