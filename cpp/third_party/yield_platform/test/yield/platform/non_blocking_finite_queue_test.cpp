// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/non_blocking_finite_queue.h"
using namespace YIELD;

#define NBFQ_LENGTH 8

TEST_SUITE( NonBlockingFiniteQueue )

TEST( NonBlockingFiniteQueue_normal, NonBlockingFiniteQueue )
{
	unsigned long in_value = 1;
	NonBlockingFiniteQueue<void*, NBFQ_LENGTH> nbfq;
	ASSERT_TRUE( nbfq.enqueue( &in_value ) );
	void* out_value = nbfq.try_dequeue();
	ASSERT_EQUAL( out_value, &in_value );
	ASSERT_EQUAL( *( ( unsigned long* )out_value ), 1 );
	ASSERT_TRUE( nbfq.try_dequeue() == NULL );
}

TEST( NonBlockingFiniteQueue_full, NonBlockingFiniteQueue )
{
	unsigned long in_values[] = { 0, 1, 2, 3, 4, 5, 6, 7 };
	NonBlockingFiniteQueue<void*, NBFQ_LENGTH> nbfq;
	for ( unsigned char i = 0; i < NBFQ_LENGTH; i++ )
	{
		ASSERT_TRUE( nbfq.enqueue( &in_values[i] ) );
	}
	ASSERT_FALSE( nbfq.enqueue( &in_values[0] ) );
	for ( unsigned char i = 0; i < NBFQ_LENGTH; i++ )
	{
		void* out_value_ptr = nbfq.try_dequeue();
		ASSERT_TRUE( out_value_ptr != NULL );
		unsigned long out_value = *( ( unsigned long* )out_value_ptr );
		ASSERT_TRUE( out_value == in_values[i] );
	}
}

TEST_MAIN( NonBlockingFiniteQueue )
