// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// The LogSection augments the records of a SequentialFile with LSNs.

#ifndef LOG_LOG_SECTION_ITERATOR_H
#define LOG_LOG_SECTION_ITERATOR_H

#include <vector>

namespace babudb {

class LogSection;

class LogSectionIterator {
public:
  static LogSectionIterator* First(std::vector<LogSection*> const* sections);
  static LogSectionIterator* Last(std::vector<LogSection*> const* sections);
  
	LogSection* GetNext();
	LogSection* GetPrevious();
  bool IsValid() const;
	bool operator != (const LogSectionIterator& other) const;
	bool operator == (const LogSectionIterator& other) const;

	LogSection* operator * ()	const;

private:
  LogSectionIterator(std::vector<LogSection*> const* sections,
                     std::vector<LogSection*>::const_iterator current);
  std::vector<LogSection*> const* sections;
  std::vector<LogSection*>::const_iterator current_section;
};

}  // namespace babudb

#endif
