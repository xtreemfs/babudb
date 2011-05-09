// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// A RecordFrame is the persistent data structure that frames a record in a SequentialFile

#ifndef BABUDB_LOG_RECORDFRAME_H
#define BABUDB_LOG_RECORDFRAME_H

#include <cstddef>

namespace babudb {

typedef unsigned short record_frame_t; // the length of the frame header/footer
static const int RECORD_FRAME_SIZE_BITS	= sizeof(record_frame_t) * 8;
static const int RECORD_FRAME_SIZE_BYTES = sizeof(record_frame_t);

static const int RECORD_MAX_SIZE_BITS = RECORD_FRAME_SIZE_BITS - 1;
static const int RECORD_MAX_SIZE = 1 << RECORD_MAX_SIZE_BITS;

static const int RECORD_FRAME_ALIGNMENT	= 2;	// align on shorts
inline unsigned long long ALIGN(unsigned long long n, int a) {
  return (((n + (a-1)) / a) * a);
}
inline bool ISALIGNED(void* n, int a)	{ 
  return (unsigned long long)n == (((unsigned long long)n)/a)*a; 
}

#if defined(__i386__) || defined(_M_IX86) || defined(TARGET_RT_LITTLE_ENDIAN) || defined(__x86_64__)  // little endian
inline record_frame_t fixEndianess(record_frame_t header ) {
	return header;
}
#elif defined(__ppc__) || defined(TARGET_RT_BIG_ENDIAN) // big endian
#pragma message( "You're entering untested territory with this big endian architecture " __FILE__) 
inline record_frame_t fixEndianess(record_frame_t header ) {
	return ((header >> 8) & 0xFF) | (header << 8);
}
#else
#pragma message( "Endianness could not be detected " __FILE__) 
#endif


class RecordFrame
{
public:
	void* getPayload() const;
	static RecordFrame* GetRecord( void* payload );

	unsigned int getPayloadSize() const;
	unsigned int GetRecordSize() const;

	bool isValid();

	bool isEndOfTransaction()	const	{
    return getHeader().structured_header.eot == 1;
  }
	void setEndOfTransaction( bool e ) {
    record_header h = getHeader();
    h.structured_header.eot = (e?1:0);
    setHeaderAndFooter(h);
  }

	void* getEndOfRecord()						{ return (unsigned char*)this + GetRecordSize(); }
	RecordFrame *getStartHeader()				{ return (RecordFrame*)((unsigned char*)this - ALIGN(_getLengthField(), RECORD_FRAME_ALIGNMENT) - RECORD_FRAME_SIZE_BYTES); }

	bool mightBeHeaderOf(RecordFrame* other) const {
    return header_data.plain_header == other->header_data.plain_header;
  }

	bool mightBeHeader() const { 
    return header_data.plain_header != 0 && GetRecordSize() != 0; 
  }

	RecordFrame(size_t size_in_bytes);

private:
	void setLength( size_t size_in_bytes ) {
    record_header h = getHeader();
    h.structured_header.size = (unsigned int)size_in_bytes;
    setHeaderAndFooter(h); 
  }

	unsigned int _getLengthField() const { 
    return getHeader().structured_header.size;
  }

	RecordFrame *getFooter() const { 
    return (RecordFrame*)((unsigned char*)this + ALIGN(_getLengthField(), RECORD_FRAME_ALIGNMENT) + RECORD_FRAME_SIZE_BYTES);
  }

	union record_header {
		struct sheader
		{
			unsigned int eot		: 1;												// transaction commit bit
			unsigned int size		: RECORD_MAX_SIZE_BITS;	// true size of the record in bytes
		} structured_header;

		record_frame_t plain_header;
	} header_data;

	record_header getHeader() const {
		return header_data;
	}

	void setHeaderAndFooter(record_header h) {
		record_frame_t fixed_header = fixEndianess(h.plain_header);
		header_data.plain_header = fixed_header;
		getFooter()->header_data.plain_header = fixed_header;
	}
};

}

#endif
