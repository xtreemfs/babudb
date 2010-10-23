// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/time.h"
#include "yield/platform/debug.h"
using namespace YIELD;

#include <cstdio>
#include <cstring>
using namespace std;

#ifdef __sun
#include <stdio.h> // For snprintf
#endif


const char* DaysOfWeek[] = {  "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
const char* Months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };


#ifdef _WIN32
#include "yield/platform/windows.h"
#endif


uint64_t Time::getCurrentEpochTimeNS()
{
#ifdef _WIN32
	FILETIME file_time;
	GetSystemTimeAsFileTime( &file_time );
	return create_uint64( file_time.dwHighDateTime, file_time.dwLowDateTime ) * 100; // SystemTime is in 100-ns intervals
#else
	struct timespec ts;
	clock_gettime( CLOCK_REALTIME, &ts );
	uint64_t epoch_time_ns = ts.tv_sec;
	epoch_time_ns *= NS_IN_S;
	epoch_time_ns += ts.tv_nsec;
	return epoch_time_ns;
#endif
}

#ifdef _WIN32
SYSTEMTIME convertEpochTimeNSToUTCSYSTEMTIME( uint64_t epoch_time_ns )
{
	epoch_time_ns /= 100;
	FILETIME file_time;
	file_time.dwHighDateTime = ( unsigned int )( ( uint64_t )epoch_time_ns >> 32 );
	file_time.dwLowDateTime = ( unsigned int )( ( uint64_t )epoch_time_ns & 0xFFFFFFFFLL );
	SYSTEMTIME win_tm;
	FileTimeToSystemTime( &file_time, &win_tm );
	return win_tm;
}

SYSTEMTIME convertEpochTimeNSToLocalSYSTEMTIME( uint64_t epoch_time_ns )
{
	SYSTEMTIME win_tm = convertEpochTimeNSToUTCSYSTEMTIME( epoch_time_ns );
	TIME_ZONE_INFORMATION win_tz;
	GetTimeZoneInformation( &win_tz );
	SYSTEMTIME win_local_tm;
	SystemTimeToTzSpecificLocalTime( &win_tz, &win_tm, &win_local_tm );
	win_tm = win_local_tm;
	return win_tm;
}
#endif

void Time::getHTTPDateTime( uint64_t epoch_time_ns, char* out_str, unsigned char out_str_len )
{
#ifdef _WIN32
	SYSTEMTIME win_tm = convertEpochTimeNSToUTCSYSTEMTIME( epoch_time_ns );

	_snprintf_s( out_str, out_str_len, _TRUNCATE,
		      "%s, %02d %s %04d %02d:%02d:%02d GMT",
	          DaysOfWeek [ win_tm.wDayOfWeek ],
	          win_tm.wDay,
	          Months [ win_tm.wMonth-1 ],
	          win_tm.wYear,
	          win_tm.wHour,
	          win_tm.wMinute,
	          win_tm.wSecond );
#else
	time_t epoch_time_s = ( time_t )( epoch_time_ns / NS_IN_S );
	struct tm unix_tm;
	gmtime_r( &epoch_time_s, &unix_tm );

	snprintf( out_str, out_str_len,
		      "%s, %02d %s %04d %02d:%02d:%02d GMT",
	          DaysOfWeek [ unix_tm.tm_wday ],
	          unix_tm.tm_mday,
	          Months [ unix_tm.tm_mon ],
	          unix_tm.tm_year + 1900,
	          unix_tm.tm_hour,
	          unix_tm.tm_min,
	          unix_tm.tm_sec );
#endif
}

uint64_t Time::parseHTTPDateTimeToEpochTimeNS( const char* date_str )
{
	char day[4], month[4];

#ifdef _WIN32
	SYSTEMTIME win_tm;

	int sf_ret = sscanf( date_str, "%03s, %02d %03s %04d %02d:%02d:%02d GMT",
	                     &day,
	                     &win_tm.wDay,
	                     &month,
	                     &win_tm.wYear,
	                     &win_tm.wHour,
	                     &win_tm.wMinute,
	                     &win_tm.wSecond );

	if ( sf_ret != 7 )
		return 0;

	for ( win_tm.wDayOfWeek = 0; win_tm.wDayOfWeek < 7; win_tm.wDayOfWeek++ )
		if ( strcmp( day, DaysOfWeek[win_tm.wDayOfWeek] ) == 0 ) break;

	for ( win_tm.wMonth = 0; win_tm.wMonth < 12; win_tm.wMonth++ )
		if ( strcmp( month, Months[win_tm.wMonth] ) == 0 ) break;
	win_tm.wMonth++; // Windows starts the months from 1

	FILETIME file_time;
	SystemTimeToFileTime( &win_tm, &file_time );
	return create_uint64( file_time.dwHighDateTime, file_time.dwLowDateTime ) * 100;
#else
	struct tm unix_tm;

	int sf_ret = sscanf( date_str, "%03s, %02d %03s %04d %02d:%02d:%02d GMT",
	                     (char*)&day,
	                     &unix_tm.tm_mday,
	                     (char*)&month,
	                     &unix_tm.tm_year,
	                     &unix_tm.tm_hour,
	                     &unix_tm.tm_min,
	                     &unix_tm.tm_sec );

	if ( sf_ret != 7 )
		return 0;

	unix_tm.tm_year -= 1900;

	for ( unix_tm.tm_wday = 0; unix_tm.tm_wday < 7; unix_tm.tm_wday++ )
		if ( strcmp( day, DaysOfWeek[unix_tm.tm_wday] ) == 0 ) break;

	for ( unix_tm.tm_mon = 0; unix_tm.tm_mon < 12; unix_tm.tm_mon++ )
		if ( strcmp( month, Months[unix_tm.tm_mon] ) == 0 ) break;

	time_t epoch_time_s = mktime( &unix_tm ); // mktime is thread-safe

	return epoch_time_s * NS_IN_S;
#endif
}

void Time::getCommonLogDateTime( uint64_t epoch_time_ns, char* out_str, unsigned char out_str_len )
{
#ifdef _WIN32
	SYSTEMTIME win_tm = convertEpochTimeNSToLocalSYSTEMTIME( epoch_time_ns );
	TIME_ZONE_INFORMATION win_tz;
	GetTimeZoneInformation( &win_tz );

	// 10/Oct/2000:13:55:36 -0700
	_snprintf_s( out_str, out_str_len, _TRUNCATE,
		      "%02d/%s/%04d:%02d:%02d:%02d %+0.4d",
	          win_tm.wDay,
	          Months[ win_tm.wMonth-1 ],
	          win_tm.wYear,
	          win_tm.wHour,
	          win_tm.wMinute,
	          win_tm.wSecond,
	          ( win_tz.Bias / 60 ) * -100 );
#else
	time_t epoch_time_s = ( time_t )( epoch_time_ns / NS_IN_S );
	struct tm unix_tm;
	localtime_r( &epoch_time_s, &unix_tm );

	snprintf( out_str, out_str_len,
 		      "%02d/%s/%04d:%02d:%02d:%02d %d",
	          unix_tm.tm_mday,
	          Months [ unix_tm.tm_mon ],
	          unix_tm.tm_year + 1900,
	          unix_tm.tm_hour,
	          unix_tm.tm_min,
	          unix_tm.tm_sec,
	          0 ); // Could use the extern timezone, which is supposed to be secs west of GMT..
#endif
}

void Time::getISODateTime( uint64_t epoch_time_ns, char* out_str, unsigned char out_str_len )
{
#ifdef _WIN32
	SYSTEMTIME win_tm = convertEpochTimeNSToLocalSYSTEMTIME( epoch_time_ns );

	_snprintf_s( out_str, out_str_len, _TRUNCATE,
		      "%04d-%02d-%02dT%02d:%02d:%02d.000Z",
			  win_tm.wYear,
			  win_tm.wMonth,
			  win_tm.wDay,
			  win_tm.wHour,
			  win_tm.wMinute,
			  win_tm.wSecond );
#else
	time_t epoch_time_s = ( time_t )( epoch_time_ns / NS_IN_S );
	struct tm unix_tm;
	localtime_r( &epoch_time_s, &unix_tm );

	snprintf( out_str, out_str_len,
		      "%04d-%02d-%02dT%02d:%02d:%02d.000Z",
			  unix_tm.tm_year + 1900,
			  unix_tm.tm_mon + 1,
			  unix_tm.tm_mday,
			  unix_tm.tm_hour,
			  unix_tm.tm_min,
			  unix_tm.tm_sec );
#endif
}

void Time::getISODate( uint64_t epoch_time_ns, char* out_str, unsigned char out_str_len )
{
#ifdef _WIN32
	SYSTEMTIME win_tm = convertEpochTimeNSToLocalSYSTEMTIME( epoch_time_ns );
	_snprintf_s( out_str, out_str_len, _TRUNCATE, "%04d-%02d-%02d", win_tm.wYear, win_tm.wMonth, win_tm.wDay );
#else
	time_t epoch_time_s = ( time_t )( epoch_time_ns / NS_IN_S );
	struct tm unix_tm;
	localtime_r( &epoch_time_s, &unix_tm );
	snprintf( out_str, out_str_len, "%04d-%02d-%02d", unix_tm.tm_year + 1900, unix_tm.tm_mon + 1, unix_tm.tm_mday );
#endif
}
