// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_TIME_H
#define YIELD_PLATFORM_TIME_H

#include "yield/platform/platform_types.h"

#ifndef _WIN32
#include <ctime>
#endif

#define NS_IN_MS 1000000
#define NS_IN_S  1000000000
#define MS_IN_S  1000


namespace YIELD
{
	class Time
	{
	public:
		static uint64_t getCurrentEpochTimeNS();
		static double getCurrentEpochTimeMS() { return ( double )getCurrentEpochTimeNS() / NS_IN_MS; }

#if !defined(_WIN32) && !defined(__MACH__)
		static inline struct timespec getNSFromCurrentTimespec( timeout_ns_t ns )
		{
			struct timespec tv;
			clock_gettime( CLOCK_REALTIME, &tv );
			tv.tv_sec += ns / NS_IN_S;
			tv.tv_nsec += ns % NS_IN_S;
			if ( tv.tv_nsec > NS_IN_S )
			{
				tv.tv_sec += 1;
				tv.tv_nsec -= NS_IN_S;
			}
			return tv;
		}
#endif

#if !defined(_WIN32)
		static inline struct timespec getNSFromNullTimespec( timeout_ns_t ns ) // This is a uint32_t because Solaris+gcc (4.0, 4.2) can't deal with uint64_t's on function calls for some reason
		{
			struct timespec tv;
			tv.tv_sec = ns / NS_IN_S;
			tv.tv_nsec = ns % NS_IN_S;
			return tv;
		}
#endif

		// HTTP datetime, always UTC
		static void getCurrentHTTPDateTime( char* out_str, unsigned char out_str_len ) { getHTTPDateTime( getCurrentEpochTimeNS(), out_str, out_str_len ); }
		static void getHTTPDateTime( uint64_t epoch_time_ns, char* out_str, unsigned char out_str_len ); // out_str_len should be >= 30
		static uint64_t parseHTTPDateTimeToEpochTimeNS( const char* );

		// Local Apache common log date-times
		static void getCurrentCommonLogDateTime( char* out_str, unsigned char out_str_len ) { getCommonLogDateTime( getCurrentEpochTimeNS(), out_str, out_str_len ); }
		static void getCommonLogDateTime( uint64_t epoch_time_ns, char* out_str, unsigned char out_str_len );

		// Local ISO date/times
		static void getCurrentISODateTime( char* out_str, unsigned char out_str_len ) { getISODateTime( getCurrentEpochTimeNS(), out_str, out_str_len ); }
		static void getISODateTime( uint64_t epoch_time_ns, char* out_str, unsigned char out_str_len );
		static void getCurrentISODate( char* out_str, unsigned char out_str_len  ) { getISODate( getCurrentEpochTimeNS(), out_str, out_str_len ); }
		static void getISODate( uint64_t, char* out_str, unsigned char out_str_len  );

		static inline uint64_t WIN2UNIX( uint64_t time ) { return ( ( time - ( uint64_t )116444736000000000LL ) / ( uint64_t )10000000LL ); }
		static inline uint64_t UNIX2WIN( uint64_t time ) { return ( time * 10000000LL + ( uint64_t )116444736000000000LL ); }

	private:
		static uint64_t freq_hz;
	};
};


#endif
