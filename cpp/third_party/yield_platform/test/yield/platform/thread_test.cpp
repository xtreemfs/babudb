// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/time.h"
#include "yield/platform/thread.h"
#include "yield/platform/locks.h"
using namespace YIELD;


TEST_SUITE( Thread )


TEST( Thread_getCurrentThreadId, Thread )
{
	ASSERT_TRUE( Thread::getCurrentThreadId() > 0 );
}

TEST( Thread_setCurrentThreadName, Thread )
{
	Thread::setCurrentThreadName( "test thread" );
}

TEST( Thread_yield, Thread )
{
	Thread::yield();
}

TEST( Thread_sleep, Thread )
{
	double start_time_ms = Time::getCurrentEpochTimeMS();
	Thread::sleep( 10 * NS_IN_MS );
	double slept_ms = Time::getCurrentEpochTimeMS() - start_time_ms;
	ASSERT_TRUE( slept_ms >= 1 );
}

TEST( Thread_TLS, Thread )
{
	unsigned long tls_key = Thread::createTLSKey();
	unsigned long my_value = 42;
	Thread::setTLS( tls_key, ( void* )my_value );
	void* ret_value = Thread::getTLS( tls_key );
	ASSERT_EQUAL( ( unsigned long )ret_value, 42 );
}

class ThreadTestCase : public TestCase, public Thread
{
public:
	ThreadTestCase( const char* short_description ) : TestCase( short_description, ThreadTestSuite() )
	{ }

	void runTest()
	{
		lock.acquire();
		startThread();
		lock.acquire();
	}

	// Thread
	void run()
	{
		lock.release();
	}

	Mutex lock;
};

#define THREAD_TEST( TestCaseName ) \
class TestCaseName##Test : public ThreadTestCase \
{ \
public:\
	TestCaseName##Test() : ThreadTestCase( #TestCaseName "Test" ) { }\
  void run( Thread&  );\
};\
TestCaseName##Test TestCaseName##Test_inst;\
void TestCaseName##Test::run( Thread& thread )

#define THREAD_TEST_END lock.release();


THREAD_TEST( Thread_GetId )
{
	ASSERT_TRUE( thread.getId() > 0 );
	THREAD_TEST_END;
}

THREAD_TEST( Thread_SetName )
{
	thread.setName( "test thread" );
	THREAD_TEST_END;
}

TEST_MAIN( Thread )
