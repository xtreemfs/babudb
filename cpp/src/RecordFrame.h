// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef ECORDFRAME_H
#define ECORDFRAME_H

#include <cstddef>

namespace babudb {

typedef unsigned char			record_type_t;
#define RECORD_TYPE_BITS		3
#define RECORD_MAX_TYPE			((1 << RECORD_TYPE_BITS) - 1)

typedef unsigned short			record_frame_t; // the length of the frame header/footer
#define RECORD_FRAME_SIZE_BITS	(sizeof(record_frame_t)*8)
#define RECORD_FRAME_SIZE_BYTES	(sizeof(record_frame_t))

#define RECORD_FRAME_ALIGNMENT	2	// align on shorts
inline unsigned long long ALIGN(unsigned long long n,int a)		{ return (((n + (a-1)) / a) * a); }
inline bool ISALIGNED(void* n, int a)							{ return (unsigned long long)n == (((unsigned long long)n)/a)*a; }

class RecordFrame
{
public:
	void* getPayload();
	static RecordFrame* getRecord( void* payload );

	unsigned int getPayloadSize();
	unsigned int getRecordSize();

	bool isValid();

	record_type_t getType();
	void setType(record_type_t t);

	bool isMarked();
	void mark( bool m );

	bool isEndOfTransaction()					{ return structured_header.eot == 1; }
	void setEndOfTransaction( bool e )			{ structured_header.eot = (e?1:0); getFooter()->header = header; }

	void* getEndOfRecord()						{ return (unsigned char*)this + getRecordSize(); }
	RecordFrame *getStartHeader()				{ return (RecordFrame*)((unsigned char*)this - ALIGN(_getLengthField(), RECORD_FRAME_ALIGNMENT) - RECORD_FRAME_SIZE_BYTES); }

	bool mightBeHeaderOf( RecordFrame* other )	{ return header == other->header; }
	bool mightBeHeader()						{ return header != 0 && getRecordSize() != 0; }

	RecordFrame( record_type_t type, size_t size_in_bytes );

private:
	void setLength( size_t size_in_bytes )		{ structured_header.size = (unsigned int)size_in_bytes; getFooter()->header = header; }
	unsigned int _getLengthField()				{ return structured_header.size; }

	RecordFrame *getFooter()					{ return (RecordFrame*)((unsigned char*)this + ALIGN(_getLengthField(), RECORD_FRAME_ALIGNMENT) + RECORD_FRAME_SIZE_BYTES); }

	union
	{
		struct
		{
			unsigned int type		: RECORD_TYPE_BITS;
			unsigned int eot		: 1;											// transaction commit bit
			unsigned int marked		: 1;											// a marker, to be used for whatever
			unsigned int size		: RECORD_FRAME_SIZE_BITS - RECORD_TYPE_BITS - 1 - 1;	// true size of the record in bytes
		} structured_header;

		record_frame_t header;
	};
};

};

#endif
