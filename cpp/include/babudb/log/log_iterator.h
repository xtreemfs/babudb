// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_LOG_LOGITERATOR_H
#define BABUDB_LOG_LOGITERATOR_H

#include <memory>

#include "babudb/buffer.h"
#include "babudb/log/record_iterator.h"

namespace babudb {
class LogSectionIterator;

class LogIterator {
public:
  ~LogIterator();

  Buffer GetNext();
  Buffer GetPrevious();

  bool operator != (const LogIterator&) const;
  bool operator == (const LogIterator&) const;

  Buffer operator * () const;
  Buffer AsData() const {
    return this->operator *();
  }
  Buffer GetOperationWithFrame() const;
  RecordIterator GetRecordIterator() const {
    return record_iterator;
  }
  bool IsValid() const;

private:
  LogIterator(LogSectionIterator* current_section);

  friend class Log;
  static LogIterator* First(LogSectionIterator* first_section);
  static LogIterator* Last(LogSectionIterator* last_section);
  
  std::auto_ptr<LogSectionIterator> current_section;
  RecordIterator record_iterator;
};

}

#endif
