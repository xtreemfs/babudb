// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "babudb/log/log_iterator.h"

#include "babudb/log/log.h"
#include "babudb/log/log_section.h"
#include "babudb/log/log_storage.h"
#include "../util.h"
#include "babudb/test.h"
using namespace babudb;

#include <algorithm>
#include <sstream>
using namespace std;

#include "yield/platform/directory_walker.h"
using namespace YIELD;

Log::Log(Buffer data) : tail(NULL), name_prefix("") {  // volatile in-memory
  LogStorage* storage = new VolatileLogStorage(data);
  tail = new LogSection(storage, 1);
  sections.push_back(tail);
}

Log::Log(const string& name_prefix) : tail(NULL), name_prefix(name_prefix) {}

Log::~Log() {
  if (tail != NULL) {
    close();
  }
  for(vector<LogSection*>::iterator i = sections.begin(); i != sections.end(); ++i)
    delete *i;
}

void Log::close() {
  advanceTail();
}

class LSNBefore {
public:
  typedef pair<YIELD::Path, lsn_t> vector_entry;
  bool operator () (const vector_entry& l, const vector_entry& r) { return l.second < r.second; }
};

typedef vector< pair<YIELD::Path, lsn_t> > DiskSections;

static DiskSections scanAvailableLogSections(const string& name_prefix) {
  DiskSections result;

  pair<YIELD::Path,YIELD::Path> prefix_parts = YIELD::Path(name_prefix).split();
  YIELD::DirectoryWalker walker(prefix_parts.first);

  while(walker.hasNext()) {
    lsn_t lsn;
    auto_ptr<YIELD::DirectoryEntry> entry = walker.getNext();

    if(matchFilename(entry->getPath(), prefix_parts.second.getHostCharsetPath(), "log", lsn))
      result.push_back(make_pair(entry->getPath(), lsn));
  }

  std::sort(result.begin(),result.end(),LSNBefore());
  return result;
}

// Rename log sections with LSNs smaller than to_lsn
void Log::cleanup(lsn_t to_lsn, const string& obsolete_prefix) {
  DiskSections disk_sections = scanAvailableLogSections(name_prefix);  // sorted by LSN

  for (DiskSections::iterator i = disk_sections.begin(); i != disk_sections.end(); ++i) {
    DiskSections::iterator next = i; next++;

    if (next != disk_sections.end() && next->second <= to_lsn) {
      pair<YIELD::Path,YIELD::Path> parts = i->first.split();
      YIELD::DiskOperations::rename(i->first, obsolete_prefix + parts.second.getHostCharsetPath());
    }
  }
}

// Loads all log sections with LSNs larger than min_lsn. Load them in order and check
// for continuity.
void Log::loadRequiredLogSections(lsn_t min_lsn) {
  ASSERT_TRUE(sections.size() == 0);  // otherwise somebody called startup() twice
  DiskSections disk_sections = scanAvailableLogSections(name_prefix);  // sorted by LSN

  for(DiskSections::iterator i = disk_sections.begin(); i != disk_sections.end(); ++i) {
    DiskSections::iterator next = i; next++;

    if(next == disk_sections.end() || (min_lsn + 1) < next->second) {
      LogStorage* file = PersistentLogStorage::OpenReadOnly(i->first);
      LogSection* section = new LogSection(file, i->second); // repairs if not graceful

      if(section->getFirstLSN() <= section->getLastLSN()) { // check if there is a LSN in this section
        sections.push_back(section);
      } else {
        ASSERT_TRUE(section->empty());
        delete section;
      }
    }
  }

  // Check that the sequence of LSNs is without gaps
  if (sections.size() > 0) {
    lsn_t prev_last_lsn = (*sections.begin())->getFirstLSN() - 1;
    for(vector<LogSection*>::iterator sec = sections.begin(); sec != sections.end(); ++sec ) {
      ASSERT_TRUE((*sec)->getFirstLSN() == prev_last_lsn + 1);
      prev_last_lsn = (*sec)->getLastLSN();
    }
  }
}

lsn_t Log::getLastLSN() {
  if(sections.empty())
    return 0;
  else
    return sections.back()->getLastLSN();
}

LogSection* Log::getTail() {
  if(tail == NULL ) {
    lsn_t next_lsn = 1;
    if(!sections.empty())
      next_lsn = sections.back()->getLastLSN() + 1;

    LogStorage* storage = NULL;
    // TODO: hack, refactor!
    if (!name_prefix.empty()) {
      std::ostringstream section_name;
      section_name << name_prefix << "_" << next_lsn << ".log";
      storage = PersistentLogStorage::Open(section_name.str());
    } else {
      storage = new VolatileLogStorage(1024);
    }
    tail = new LogSection(storage, next_lsn);
    sections.push_back(tail);
  }

  return tail;
}

void Log::advanceTail() {
  if(tail) {
    if(tail->isWritable()) {
      tail->truncate();
    }
    tail->close();
  }
  tail = NULL;
}

Log::iterator Log::begin() {
  if(sections.empty()) {
    return end();
  } else {
    return Log::iterator(
        sections.begin(), sections.end(),
        sections.begin(),
        Log::iterator::section_begin(sections.begin()));
  }
}

Log::iterator Log::end(){
  if(sections.empty()) {
    return LogIterator<std::vector<LogSection*>::iterator>(
      sections.begin(), sections.end(),
      sections.end(), RecordIterator());
  } else {
    return Log::iterator(
        sections.begin(), sections.end(),
        sections.end(),
        Log::iterator::section_end(sections.end() - 1));
  }
}

Log::reverse_iterator Log::rbegin() {
  if(sections.empty()) {
    return rend();
  } else {
    return Log::reverse_iterator(
        sections.rbegin(), sections.rend(),
        sections.rbegin(),
        Log::reverse_iterator::section_begin(sections.rbegin()));
  }
}

Log::reverse_iterator Log::rend(){
  if(sections.empty()) {
    return Log::reverse_iterator(
        sections.rbegin(), sections.rend(),
        sections.rend(),
        RecordIterator());
  } else {
    return Log::reverse_iterator(
        sections.rbegin(), sections.rend(),
        sections.rend(),
        Log::reverse_iterator::section_end(sections.rend() - 1));
  }
}
