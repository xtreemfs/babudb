// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// The Log bundles a series of LogSections with increasing LSNs. The Log classes
// do not keep a record of the current LSN.
// Example usage:
//   Log log("/path/mylog");
//   log.LoadSections(0, true);
//   ... iterate/recover and find last lsn ...
//   LogSection* tail = log.GetTail(last_lsn + 1);
//
// The log is stored as a set of files whose file names contain the
// minimal LSN: prefix_<min_lsn>.log
//
// LSNs don't need to be contiguous, but must increase between log commits.
//

#ifndef BABUDB_LOG_LOG_H
#define BABUDB_LOG_LOG_H

#include <string>
#include <vector>

#include "babudb/log/log_section.h"
#include "babudb/log/log_iterator.h"

namespace babudb {

class Log {
public:
  // Construct a volatile in-memory log
  explicit Log(Buffer data);
  // A log file with the specific prefix. Individual sections
  // will be named /na/me/prefix_<lastlsn>.log
  explicit Log(const std::string& name_prefix);
  // Also Close()s the log
  ~Log();

  // Load all log sections that contain at least records including
  // starting_from_lsn.
  void Open(lsn_t starting_from_lsn);
  // Close all log sections
  void Close();

  // Cleanup log by moving all 
  void Cleanup(lsn_t to_lsn, const std::string& to);

  // Get a writable tail. Creates a new LogSection if there is none.
  LogSection* GetTail(babudb::lsn_t next_lsn);
  // Force creation of a new writable tail.
  void AdvanceTail();

  typedef LogIterator iterator;
  // An iterator starting at the first record in the log.
  iterator* First() const;
  // An iterator starting at the last record in the log.
  iterator* Last() const;

  int NumberOfSections() const;

private:
  std::vector<LogSection*> sections;
  LogSection* tail;
  string name_prefix;
};

}  // namespace babudb

#endif
