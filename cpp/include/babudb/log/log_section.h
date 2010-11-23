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
#include <vector>

#include "babudb/log/sequential_file.h"
#include "babudb/buffer.h"

#define LSN_RECORD_TYPE 0
#define USER_RECORD_TYPE 1

namespace babudb {

class LogStorage;

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

	lsn_t getFirstLSN();
	lsn_t getLastLSN();

  // Start a new transaction, idempotent
  lsn_t StartTransaction();
  // Append entry, start a new transaction if necessary
	lsn_t Append(const Serializable& entry);
  // Make the current transaction durable
	void Commit();

  // Commit an empty transaction with a new LSN
  void SeekForwardTo(babudb::lsn_t);

private:
	bool in_transaction;
	lsn_t first_lsn; // the first lsn in this file
	lsn_t next_lsn;  // the next lsn to write
};

class LogSectionIterator {
public:
  LogSectionIterator(const LogSectionIterator& it); 

  static LogSectionIterator begin(std::vector<LogSection*>& sections);
  static LogSectionIterator last(std::vector<LogSection*>& sections);
  static LogSectionIterator end(std::vector<LogSection*>& sections);
  static LogSectionIterator rbegin(std::vector<LogSection*>& sections);
  static LogSectionIterator rlast(std::vector<LogSection*>& sections);
  static LogSectionIterator rend(std::vector<LogSection*>& sections);
  
	void operator ++ ();
	void operator -- ();
	bool operator != (const LogSectionIterator& other) const;
	bool operator == (const LogSectionIterator& other) const;

  void operator = (const LogSectionIterator& other);

	LogSection* operator * ()	const;

  bool IsReverse() const {
    return direction < 0;
  }

private:
  LogSectionIterator(std::vector<LogSection*>& sections);
  std::vector<LogSection*>& sections;
  int index;
  int direction;
  int begin_index;
  int end_index;
};

}

#endif
