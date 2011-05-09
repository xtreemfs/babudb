// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "log/log_section_iterator.h"
#include "yield/platform/assert.h"

namespace babudb {

LogSectionIterator::LogSectionIterator(
    std::vector<LogSection*> const* sections,
    std::vector<LogSection*>::const_iterator current)
  : sections(sections), current_section(current) {}

LogSectionIterator* LogSectionIterator::First(
    std::vector<LogSection*> const* sections) {
  return new LogSectionIterator(sections, sections->begin());
}

LogSectionIterator* LogSectionIterator::Last(
    std::vector<LogSection*> const* sections) {
  if (sections->empty()) {
    return new LogSectionIterator(sections, sections->end());
  } else {
    return new LogSectionIterator(sections, --(sections->end()));
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

}  // namespace babudb
