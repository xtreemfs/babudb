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

LogSection::LogSection(LogStorage* mmfile, lsn_t first)
    : SequentialFile(mmfile, new LogStats()), first_lsn(first) { }

lsn_t LogSection::getFirstLSN() const {
	return first_lsn;
}

void LogSection::Append(const Serializable& entry) {
	void* write_location = getFreeSpace(RECORD_MAX_SIZE);
  entry.Serialize(Buffer(write_location, RECORD_MAX_SIZE));
  unsigned int payload_size = entry.GetSize();
  ASSERT_TRUE(payload_size <= RECORD_MAX_SIZE);  // be paranoid
  frameData(write_location, payload_size, entry.GetType());
}

void LogSection::Commit() {
	commit();
}

void LogSection::Erase(const iterator& it) {
  erase(record2offset(it.GetRecord()));
}


LogSectionIterator::LogSectionIterator(
    const std::vector<LogSection*>& sections,
    std::vector<LogSection*>::const_iterator current)
  : sections(&sections), current_section(current) {}

LogSectionIterator::LogSectionIterator(const LogSectionIterator& it)
  : sections(it.sections), current_section(it.current_section) {}

void LogSectionIterator::operator = (const LogSectionIterator& other) {
  sections = other.sections;
  current_section = other.current_section;
}

LogSectionIterator LogSectionIterator::First(
    const std::vector<LogSection*>& sections) {
  return LogSectionIterator(sections, sections.begin());
}

LogSectionIterator LogSectionIterator::Last(
    const std::vector<LogSection*>& sections) {
  if (sections.empty()) {
    return LogSectionIterator(sections, sections.end());
  } else {
    return LogSectionIterator(sections, --(sections.end()));
  }
}
  
LogSection* LogSectionIterator::GetNext() {
  if (current_section == sections->end()) {
    return NULL;
  }
  ++current_section;
  if (current_section == sections->end()) {
    return NULL;
  } else {
    return *current_section;
  }
}

LogSection* LogSectionIterator::GetPrevious() {
  if (current_section == sections->begin()) {
    return NULL;
  } else {
    --current_section;
    return *current_section;
  }
}

bool LogSectionIterator::IsValid() const {
  return sections->size() > 0 && current_section != sections->end();
}

bool LogSectionIterator::operator != (const LogSectionIterator& other) const {
  return !this->operator==(other);
}

bool LogSectionIterator::operator == (const LogSectionIterator& other) const {
  ASSERT_TRUE(sections->begin() == other.sections->begin());
  return current_section == other.current_section;
}

LogSection* LogSectionIterator::operator * ()	const {
  if (current_section == sections->end()) {
    return NULL;
  } else {
    return *current_section;
  }
}
