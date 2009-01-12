// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/LogIterator.h"
#include "LogSection.h"
#include "SequentialFile.h"

#include "yield/platform/yunit.h"

using namespace babudb;

LogIterator::LogIterator(const LogIterator& o)
	: sections_begin(o.sections_begin), sections_end(o.sections_end),
	  current_section(o.current_section), current_record(o.current_record)  {}

LogIterator::LogIterator(const std::vector<LogSection*>::iterator& b,
						 const std::vector<LogSection*>::iterator& e,
						 const std::vector<LogSection*>::iterator& c_s,
						 const RecordIterator& c)
	: sections_begin(b), sections_end(e), current_section(c_s), current_record(c) {

	if(current_section != sections_end && current_record.isType(0))
		this->operator ++();
}

void LogIterator::operator ++ () {
	ASSERT_FALSE(current_section == sections_end);
	++current_record;

	if(current_record == (*current_section)->end()) {
		// at the end of the current section, proceed to the beginning of the next, if any
		std::vector<LogSection*>::iterator next_section = current_section; ++next_section;

		if(next_section == sections_end)
			current_record = (*current_section)->end(); // back()->end()
		else
			current_record = (*next_section)->begin();

		current_section = next_section;
	}

	// skip LSN records
	if(current_section != sections_end && current_record != (*current_section)->end() && current_record.isType(0)) {
		this->operator ++();
	}
}

void LogIterator::operator -- () {
	if(current_section == sections_end)
		--current_section;

	if(current_record == (*current_section)->begin()) {
		// at the end of the current section, proceed to the beginning of the next, if any
		ASSERT_TRUE(current_section != sections_begin);
		--current_section;
		current_record = (*current_section)->end();
	}

	--current_record;

	// skip LSN records
	if(current_record.isType(0)) {
		if(current_section != sections_begin || current_record != (*current_section)->begin())
			this->operator --();
	}
}

bool LogIterator::operator != (const LogIterator& other) const {
	return current_section != other.current_section || current_record != other.current_record;
}

bool LogIterator::operator == (const LogIterator& other) const {
	return current_section == other.current_section && current_record == other.current_record;
}

Data LogIterator::operator * () const {
	ASSERT_TRUE(getType() != 0);
	return Data(*current_record, current_record.getSize());
}

Data LogIterator::getOperationWithFrame() const {
	ASSERT_TRUE(getType() != 0);
	return Data(current_record.getRecord(), current_record.getRecord()->getRecordSize());
}

operation_type_t LogIterator::getType() const {
	return current_record.getType();
}

bool LogIterator::isMarked() const {
	return current_record.getRecord()->isMarked();
}
