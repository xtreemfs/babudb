// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_LOGITERATOR_H
#define BABUDB_LOGITERATOR_H

#include <vector>

#include "babudb/Operation.h"
#include "babudb/RecordIterator.h"

namespace babudb {
class LogSection;

class LogIterator {
public:
	LogIterator(const LogIterator&);
	LogIterator(const std::vector<LogSection*>::iterator&,
				const std::vector<LogSection*>::iterator&,
				const std::vector<LogSection*>::iterator&,
				const RecordIterator&);

	void operator ++ ();
	void operator -- ();

	bool operator != (const LogIterator&) const;
	bool operator == (const LogIterator&) const;

	Data operator * () const;
	Data asData() const { return this->operator *(); }
	Data getOperationWithFrame() const;

	operation_type_t getType() const;
	bool isMarked() const;

private:
	std::vector<LogSection*>::iterator  sections_begin;
	std::vector<LogSection*>::iterator  sections_end;
	std::vector<LogSection*>::iterator  current_section;
	RecordIterator						current_record;
};

};

#endif
