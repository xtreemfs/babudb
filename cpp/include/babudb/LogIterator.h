// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_LOGITERATOR_H
#define BABUDB_LOGITERATOR_H

#include "babudb/Operation.h"
#include "babudb/RecordIterator.h"

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

	Data operator * () const;
	Data asData() const { return this->operator *(); }
	Data getOperationWithFrame() const;

	operation_type_t getType() const;

	static RecordIterator section_begin(T& section);
	static RecordIterator section_end(T& section);

private:
	T  sections_begin;
	T  sections_end;
	T  current_section;
	RecordIterator current_record;
};

typedef class LogIterator<std::vector<LogSection*>::iterator> LogIteratorForward;
typedef class LogIterator<std::vector<LogSection*>::reverse_iterator> LogIteratorBackward;

};

#endif
