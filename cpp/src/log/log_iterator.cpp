// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/log/log_iterator.h"
#include "babudb/log/log_section.h"
#include "babudb/log/sequential_file.h"

#include "yield/platform/yunit.h"

#include <vector>

namespace babudb {

LogIterator::LogIterator(const LogIterator& o)
  : sections_begin(o.sections_begin), sections_end(o.sections_end),
    current_section(o.current_section), current_record(o.current_record),
    current_record_data(Buffer::Empty()) {}

LogIterator::LogIterator(const LogSectionIterator& b,
  const LogSectionIterator& e,
  const LogSectionIterator& c_s,
  const RecordIterator& c)
  : sections_begin(b), sections_end(e), current_section(c_s), current_record(c),
    current_record_data(Buffer::Empty()) {

  if(current_section != sections_end && current_record.isType(LSN_RECORD_TYPE)) {
    lsn = *(lsn_t*)*current_record;
    this->operator ++();
  }
}

void LogIterator::operator ++ () {
  ASSERT_FALSE(current_section == sections_end);
  ++current_record;

  if(current_record == section_end(current_section)) {
    // at the end of the current section, proceed to the beginning of the next, if any
    LogSectionIterator next_section(current_section); ++next_section;

    if(next_section == sections_end)
      current_record = section_end(current_section); // back()->end()
    else
      current_record = section_begin(next_section);

    current_section = next_section;
  }

  // skip LSN records
  if(current_section != sections_end && current_record != section_end(current_section) && current_record.isType(0)) {
    lsn = *(lsn_t*)*current_record;
    this->operator ++();
  }
}

void LogIterator::operator -- () {
  if(current_section == sections_end)
    --current_section;

  if(current_record == section_begin(current_section)) {
    // at the end of the current section, proceed to the beginning of the next, if any
    ASSERT_TRUE(current_section != sections_begin);
    --current_section;
    current_record = section_end(current_section);
  }

  --current_record;

  if (current_record.isType(LSN_RECORD_TYPE)) {
    lsn = *(lsn_t*)*current_record;
  }

  // skip LSN records
  if(current_record.isType(LSN_RECORD_TYPE)) {
    if(current_section != sections_begin || current_record != section_begin(current_section))
      this->operator --();
  }
}

bool LogIterator::operator != (const LogIterator& other) const {
  return current_section != other.current_section || current_record != other.current_record;
}

bool LogIterator::operator == (const LogIterator& other) const {
  return current_section == other.current_section && current_record == other.current_record;
}

Buffer& LogIterator::operator * () const {
  ASSERT_TRUE(current_record.getType() != LSN_RECORD_TYPE);
  current_record_data.data = *current_record;
  current_record_data.size = current_record.getSize();
  return current_record_data;
}

Buffer* LogIterator::operator -> () const {
  ASSERT_TRUE(current_record.getType() != LSN_RECORD_TYPE);
  current_record_data.data = *current_record;
  current_record_data.size = current_record.getSize();
  return &current_record_data;
}

Buffer LogIterator::getOperationWithFrame() const {
  ASSERT_TRUE(current_record.getType() != LSN_RECORD_TYPE);
  return Buffer(current_record.getRecord(), current_record.getRecord()->getRecordSize());
}

record_type_t LogIterator::getType() const {
  return current_record.getType() - USER_RECORD_TYPE;
}

RecordIterator LogIterator::section_begin(const LogSectionIterator& section) {
  if (section.IsReverse()) {
    return (*section)->rbegin(); 
  } else {
    return (*section)->begin();
  }
}

RecordIterator LogIterator::section_end(const LogSectionIterator& section) {
  if (section.IsReverse()) {
    return (*section)->rend(); 
  } else {
    return (*section)->end();
  }
}

}