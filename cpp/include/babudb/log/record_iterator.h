// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// A RecordIterator is a container for the state necessary to iterate over the Records in a SequentialFile

#ifndef LOG__RECORD_ITERATOR_H
#define LOG__RECORD_ITERATOR_H

#include <cstddef>
#include "babudb/log/record_frame.h"

namespace babudb {

class RecordFrame;
class Buffer;

class RecordIterator
{
public:
	RecordIterator() : current(NULL), region_start(NULL), region_size(0), is_forward_iterator(true) {}

	RecordIterator(const RecordIterator& other)
		: current(other.current), region_start(other.region_start), region_size(other.region_size),
		  is_forward_iterator(other.is_forward_iterator) {}

	RecordIterator(void* start, size_t size, RecordFrame* pos, bool is_forward )
		: current(pos), region_start(start), region_size(size),
		  is_forward_iterator(is_forward) {}

	static RecordIterator begin(void* start, size_t size);
	static RecordIterator end(void* start, size_t size);
	static RecordIterator rbegin(void* start, size_t size);
	static RecordIterator rend(void* start, size_t size);

	void operator ++ () {
		if(is_forward_iterator) plusplus();
		else					minusminus();
	}

	void operator -- () {
		if(is_forward_iterator) minusminus();
		else					plusplus();
	}

	void reverse();

	bool operator != ( const RecordIterator& other ) const;
	bool operator == ( const RecordIterator& other ) const;

	void* operator * ()	const;
	size_t getSize() const;

	record_type_t getType() const;
	bool isType( record_type_t t ) const;

	RecordFrame* getRecord() const;
	Buffer asData() const;

protected:
	void plusplus();
	void minusminus();
	void windIteratorToStart();
	RecordFrame* windForwardToNextRecord(RecordFrame*);
	RecordFrame* windBackwardToNextRecord(RecordFrame*);

	RecordFrame* current;
	void* region_start;
	size_t region_size;
	bool is_forward_iterator;
};

}

#endif
