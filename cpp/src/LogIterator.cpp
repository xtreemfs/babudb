// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/LogIterator.h"
#include "LogSection.h"
#include "SequentialFile.h"

#include "yield/platform/yunit.h"

#include <vector>

using namespace babudb;

template class LogIterator<std::vector<LogSection*>::iterator >;
template class LogIterator<std::vector<LogSection*>::reverse_iterator >;

template <>
RecordIterator LogIterator<std::vector<LogSection*>::iterator>::section_begin(std::vector<LogSection*>::iterator& section) {
	return (*section)->begin();
}

template <>
RecordIterator LogIterator<std::vector<LogSection*>::reverse_iterator>::section_begin(std::vector<LogSection*>::reverse_iterator& section) {
	return (*section)->rbegin();
}

template <>
RecordIterator LogIterator<std::vector<LogSection*>::iterator>::section_end(std::vector<LogSection*>::iterator& section) {
	return (*section)->end();
}

template <>
RecordIterator LogIterator<std::vector<LogSection*>::reverse_iterator>::section_end(std::vector<LogSection*>::reverse_iterator& section) {
	return (*section)->rend();
}



template <class T>
LogIterator<T>::LogIterator(const LogIterator& o)
	: sections_begin(o.sections_begin), sections_end(o.sections_end),
	  current_section(o.current_section), current_record(o.current_record)  {}

template <class T>
LogIterator<T>::LogIterator(const T& b,
						 const T& e,
						 const T& c_s,
						 const RecordIterator& c)
	: sections_begin(b), sections_end(e), current_section(c_s), current_record(c) {

	if(current_section != sections_end && current_record.isType(0))
		this->operator ++();
}

template <class T>
void LogIterator<T>::operator ++ () {
	ASSERT_FALSE(current_section == sections_end);
	++current_record;

	if(current_record == section_end(current_section)) {
		// at the end of the current section, proceed to the beginning of the next, if any
		T next_section = current_section; ++next_section;

		if(next_section == sections_end)
			current_record = section_end(current_section); // back()->end()
		else
			current_record = section_begin(next_section);

		current_section = next_section;
	}

	// skip LSN records
	if(current_section != sections_end && current_record != section_end(current_section) && current_record.isType(0)) {
		this->operator ++();
	}
}

template <class T>
void LogIterator<T>::operator -- () {
	if(current_section == sections_end)
		--current_section;

	if(current_record == section_begin(current_section)) {
		// at the end of the current section, proceed to the beginning of the next, if any
		ASSERT_TRUE(current_section != sections_begin);
		--current_section;
		current_record = section_end(current_section);
	}

	--current_record;

	// skip LSN records
	if(current_record.isType(0)) {
		if(current_section != sections_begin || current_record != section_begin(current_section))
			this->operator --();
	}
}

template <class T>
bool LogIterator<T>::operator != (const LogIterator& other) const {
	return current_section != other.current_section || current_record != other.current_record;
}

template <class T>
bool LogIterator<T>::operator == (const LogIterator& other) const {
	return current_section == other.current_section && current_record == other.current_record;
}

template <class T>
Data LogIterator<T>::operator * () const {
	ASSERT_TRUE(getType() != 0);
	return Data(*current_record, current_record.getSize());
}

template <class T>
Data LogIterator<T>::getOperationWithFrame() const {
	ASSERT_TRUE(getType() != 0);
	return Data(current_record.getRecord(), current_record.getRecord()->getRecordSize());
}

template <class T>
operation_type_t LogIterator<T>::getType() const {
	return current_record.getType();
}

template <class T>
bool LogIterator<T>::isMarked() const {
	return current_record.getRecord()->isMarked();
}
