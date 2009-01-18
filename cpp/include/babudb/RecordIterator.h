// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_RECORDITERATOR_H
#define BABUDB_RECORDITERATOR_H

#include <cstddef>

namespace babudb {

class RecordFrame;
class Data;

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

	unsigned char getType() const;
	bool isType( unsigned char t ) const;

	RecordFrame* getRecord() const;
	Data asData() const;

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

};

#endif
