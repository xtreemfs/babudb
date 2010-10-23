// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_LOGITERATOR_H
#define BABUDB_LOGITERATOR_H

#include "babudb/buffer.h"
#include "babudb/log/record_iterator.h"

#include <vector>

namespace babudb {
class LogSection;

template <class T>
class LogIterator {
public:
  LogIterator(const LogIterator&);
  LogIterator(const T& sections_begin,
        const T& sections_end,
        const T& current_section,
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

  static RecordIterator section_begin(const T& section);
  static RecordIterator section_end(const T& section);

private:
  T sections_begin;
  T sections_end;
  T current_section;
  RecordIterator current_record;
  mutable Buffer current_record_data;
  lsn_t lsn;
};

typedef class LogIterator<std::vector<LogSection*>::iterator> LogIteratorForward;
typedef class LogIterator<std::vector<LogSection*>::reverse_iterator> LogIteratorBackward;
}

#endif
