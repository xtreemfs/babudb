// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// The LogSection augments the records of a SequentialFile with LSNs.

#ifndef BABUDB_LOG_LOGSECTION_H
#define BABUDB_LOG_LOGSECTION_H

#include "babudb/log/sequential_file.h"
#include "babudb/buffer.h"

namespace babudb {

class LogStorage;

// The interface to an application log record
class Serializable {
public:
  virtual size_t GetSize() const = 0;
  virtual void Serialize(const Buffer& buffer) const = 0;
  virtual int GetType() const { return 0; }
  virtual ~Serializable() {}
};

class LogSection : public SequentialFile {
public:
	LogSection(LogStorage*, lsn_t first);

	lsn_t getFirstLSN() const;

  // Append entry, start a new transaction if necessary
	void Append(const Serializable& entry);
  // Make the current transaction durable
	void Commit();
  void Erase(const iterator& it);

private:
	lsn_t first_lsn; // the first lsn in this file
};

}  // namespace babudb

#endif
