// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/time.h"
#include "yield/platform/thread.h"
using namespace YIELD;

#include <cstring>
using std::strlen;
using std::strncmp;


TEST( Time_getCurrentEpochTimeNS, babudb )
{
	uint64_t start_time_ns = Time::getCurrentEpochTimeNS();
	Thread::sleep( 100 * NS_IN_MS );
	uint64_t end_time_ns = Time::getCurrentEpochTimeNS();
	ASSERT_TRUE( end_time_ns > start_time_ns );
}

#ifndef _WIN32
TEST( Time_getNSFromNullTimespec, babudb )
{
	struct timespec tv = Time::getNSFromNullTimespec( 1 * NS_IN_S );
	ASSERT_TRUE( tv.tv_sec && tv.tv_nsec == 0 );
	tv = Time::getNSFromNullTimespec( 1500 * NS_IN_MS );
	ASSERT_TRUE( tv.tv_sec == 1 && tv.tv_nsec == 500 * NS_IN_MS );
}
#endif

TEST( Time_HTTPDateTime, babudb )
{
	char http_date_time[30];
	uint64_t epoch_time_ns = Time::getCurrentEpochTimeNS();
	Time::getHTTPDateTime( epoch_time_ns, http_date_time, 30 );
	ASSERT_TRUE( strstr( http_date_time, "GMT" ) != 0 );
	uint64_t parsed_epoch_time_ns = Time::parseHTTPDateTimeToEpochTimeNS( http_date_time );
	ASSERT_TRUE( epoch_time_ns % parsed_epoch_time_ns < NS_IN_S );
}

TEST( Time_CommonLogDateTime, babudb )
{
  /*
	char common_log_date_time[30];
	Time::getCurrentCommonLogDateTime( common_log_date_time, 30 );
	ASSERT_TRUE( strstr( common_log_date_time, "/200" ) != 0 );
  */
}

TEST( Time_ISODateTime, babudb )
{
	char iso_date_time[30];
	Time::getCurrentISODateTime( iso_date_time, 30 );
	ASSERT_EQUAL( strncmp( iso_date_time, "20", 2 ), 0 );
	char iso_date[30];
	Time::getCurrentISODate( iso_date, 30 );
	ASSERT_EQUAL( strncmp( iso_date, "20", 2 ), 0 );
	ASSERT_TRUE( strlen( iso_date_time ) > strlen( iso_date ) );
}
