// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/atomic_ops.h"
using namespace YIELD;


TEST_SUITE( AtomicOps )

TEST( atomic_cas, AtomicOps )
{
	volatile uint32_t current_value = 0;
	uint32_t old_value = atomic_cas( &current_value, 1, 0 );
	ASSERT_EQUAL( old_value, 0 );

#if defined(_WIN64) || defined(__LP64) || defined(__LLP64)
	uint64 current_value = 0, new_value = 1, _old_value = 0;
	uint64 new_current_value = atomic_cas( &current_value, 1, 0 );
	ASSERT_EQUALS( new_current_value, 1 );
#endif
}

TEST( atomic_inc, AtomicOps )
{
	volatile uint32_t current_value = 0;
	uint32_t new_current_value = atomic_inc( &current_value );
	std::cout << "Got new current value " << new_current_value;
	ASSERT_EQUAL( new_current_value, 1 );
}

TEST( atomic_dec, AtomicOps )
{
	volatile uint32_t current_value = 1;
	uint32_t new_current_value = atomic_dec( &current_value );
	ASSERT_EQUAL( new_current_value, 0 );
}

TEST_MAIN( AtomicOps )
