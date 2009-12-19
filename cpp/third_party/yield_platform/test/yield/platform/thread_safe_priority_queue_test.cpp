// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/thread_safe_priority_queue.h"
using namespace YIELD;


TEST_SUITE( ThreadSafePriorityQueue )

struct TestElement
{
public:
	TestElement( size_t my_size ) : my_size( my_size ) { }
	size_t my_size;
};

struct TestElementComparator
{
	bool operator()( const TestElement& left, const TestElement& right ) const
	{
		return left.my_size > right.my_size;
	}
};

TEST( ThreadSafePriorityQueue, ThreadSafePriorityQueue )
{
	ThreadSafePriorityQueue<TestElement, TestElementComparator> queue;

	TestElement small( 1 ), medium( 2 ), large( 3 );
	queue.enqueue( medium );
	queue.enqueue( large );
	queue.enqueue( small );

	TestElement dequeued = queue.try_dequeue();
	ASSERT_EQUAL( dequeued.my_size, 1 );
	dequeued = queue.try_dequeue();
	ASSERT_EQUAL( dequeued.my_size, 2 );
	dequeued = queue.try_dequeue();
	ASSERT_EQUAL( dequeued.my_size, 3 );
}

TEST_MAIN( ThreadSafePriorityQueue )
