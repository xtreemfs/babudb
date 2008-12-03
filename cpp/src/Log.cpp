// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/LogIterator.h"

#include "Log.h"
#include "LogSection.h"
#include "LogIndex.h"
#include "util.h"
#include "DataIndex.h"
using namespace babudb;

#include <algorithm>
#include <sstream>
using namespace std;

#include "yield/platform/directory_walker.h"
#include "yield/platform/memory_mapped_file.h"
using namespace YIELD;

Log::Log(const string& name_prefix) : tail(NULL), name_prefix(name_prefix) {}

Log::~Log() {
//	ASSERT_TRUE(tail == NULL); // close before delete!
	for(vector<LogSection*>::iterator i = sections.begin(); i != sections.end(); ++i)
		delete *i;
}

void Log::close() {
	advanceTail();
}


class LSNBefore {
public:
	typedef pair<YIELD::DiskPath, lsn_t> vector_entry;
	bool operator () (const vector_entry& l, const vector_entry& r) { return l.second < r.second; }
};

typedef vector< pair<YIELD::DiskPath, lsn_t> > DiskSections;

static DiskSections scanAvailableLogSections(const string& name_prefix) {
	DiskSections result;

	pair<YIELD::DiskPath,YIELD::DiskPath> prefix_parts = YIELD::DiskPath(name_prefix).split();
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

void Log::cleanup(lsn_t from_lsn, const string& to) {
	DiskSections disk_sections = scanAvailableLogSections(name_prefix);

	for(DiskSections::iterator i = disk_sections.begin(); i != disk_sections.end(); ++i) {
		DiskSections::iterator next = i; next++;

		if(next != disk_sections.end() && next->second <= from_lsn) {
			pair<YIELD::DiskPath,YIELD::DiskPath> parts = i->first.split();
			YIELD::DiskOperations::rename(i->first, YIELD::DiskPath(to) + parts.second);
		}
	}
}


/** Loads all log sections with LSNs larger than min_lsn. Load them in order and check
	for continuity.
*/

void Log::loadRequiredLogSections(lsn_t min_lsn) {
	DiskSections disk_sections = scanAvailableLogSections(name_prefix);

	for(DiskSections::iterator i = disk_sections.begin(); i != disk_sections.end(); ++i) {
		DiskSections::iterator next = i; next++;

		if(next == disk_sections.end() || next->second > min_lsn) {
			auto_ptr<YIELD::MemoryMappedFile> file(new YIELD::MemoryMappedFile(i->first, 4, DOOF_READ));

			LogSection* section = new LogSection(file, i->second); // repairs if not graceful
			sections.push_back(section);
		}
	}

	// Check that the sequence of LSNs is without gaps
	lsn_t prev_last_lsn = min_lsn;
	for(vector<LogSection*>::iterator sec = sections.begin(); sec != sections.end(); ++sec ) {
		ASSERT_TRUE((*sec)->getFirstLSN() == prev_last_lsn + 1);
		prev_last_lsn = (*sec)->getLastLSN();
	}
}

/** Replay the log to the LogIndices.
*/

void Log::replayToLogIndices(DataIndexOperationTarget& indices, const OperationFactory& factory, lsn_t min_lsn) {
	// now replay to indices
	lsn_t last_lsn = 0;
	for(vector<LogSection*>::iterator sec = sections.begin(); sec != sections.end(); ++sec ) {
		for(SequentialFile::iterator rec = (*sec)->begin();	rec != (*sec)->end(); ++rec) {
			if(rec.isType(0)) { // is lsn marker
				last_lsn = *(lsn_t*)*rec;
			}
			else {
				ASSERT_TRUE(last_lsn != 0);
				Data data(rec.getRecord()->getPayload(),rec.getRecord()->getPayloadSize());
				Operation* o = factory.createOperation(data,rec.getRecord()->getType());
				o->applyTo(indices);
				delete o;
			}
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

		std::ostringstream section_name;
		section_name << name_prefix << "_" << next_lsn << ".log";

		auto_ptr<YIELD::MemoryMappedFile> file(new YIELD::MemoryMappedFile(section_name.str(), 1024 * 1024, DOOF_CREATE|DOOF_READ|DOOF_WRITE|DOOF_SYNC));

		tail = new LogSection(file,next_lsn);
		sections.push_back(tail);
	}

	return tail;
}

void Log::advanceTail() {
	if(tail)
		tail->close();
	tail = NULL;
}

LogIterator Log::begin() {
	return LogIterator(sections.begin(), sections.end(), sections.begin(), (*sections.begin())->begin());
}

LogIterator Log::end(){
	return LogIterator(sections.begin(), sections.end(), sections.end(), sections.back()->end());
}
