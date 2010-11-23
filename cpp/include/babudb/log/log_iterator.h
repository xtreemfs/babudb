// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_LOGITERATOR_H
#define BABUDB_LOGITERATOR_H

#include "babudb/buffer.h"
#include "babudb/log/record_iterator.h"
#include "babudb/log/log_section.h"

#include <vector>

namespace babudb {
class LogSection;

class LogIterator {
public:
  LogIterator(const LogIterator&);
  LogIterator(
      const LogSectionIterator& sections_begin,
      const LogSectionIterator& sections_end,
      const LogSectionIterator& current_section,
      const RecordIterator& current_record);

  void operator ++ ();
  void operator -- ();

  bool operator != (const LogIterator&) const;
  bool operator == (const LogIterator&) const;

  Buffer& operator * () const;
  Buffer* operator -> () const;
  Buffer asData() const { return this->operator *(); }
  Buffer getOperationWithFrame() const;

  record_type_t getType() const;
  lsn_t GetLSN() const { return lsn; };

  static RecordIterator section_begin(const LogSectionIterator& section);
  static RecordIterator section_end(const LogSectionIterator& section);

private:
  LogSectionIterator sections_begin;
  LogSectionIterator sections_end;
  LogSectionIterator current_section;
  RecordIterator current_record;
  mutable Buffer current_record_data;
  lsn_t lsn;
};

}

#endif
