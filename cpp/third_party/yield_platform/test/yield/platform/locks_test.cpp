// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/thread.h"
#include "yield/platform/locks.h"
using namespace YIELD;


TEST_SUITE( Locks )

class MutexTest : public TestCase, public Thread
{
public:
	MutexTest() : TestCase( "MutexTest", LocksTestSuite() )
	{ }

	void runTest()
	{
		is_running = false;
		startThread();
		while ( !is_running )
			Thread::yield();
		mutex.acquire();
		Thread::sleep( 10 );
		mutex.release();
	}

	// Thread
	void run()
	{
		is_running = true;
		mutex.acquire();
		Thread::sleep( 10 );
		mutex.release();
	}

	Mutex mutex;
	bool is_running;
};

MutexTest MutexTest_inst;

class CountingSemaphoreTest : public TestCase, public Thread
{
public:
	CountingSemaphoreTest() : TestCase( "CountingSemaphoreTest", LocksTestSuite() )
	{ }

	void runTest()
	{
		sem.release();
		exited_threads_count = 0;
		startThread();
		while ( exited_threads_count < 1 )
			Thread::yield();
	}

	// Thread
	void run()
	{
		sem.acquire();
		exited_threads_count++;
	}

	CountingSemaphore sem;
	unsigned char exited_threads_count;
};

CountingSemaphoreTest CountingSemaphoreTest_inst;

TEST_MAIN( Locks )
