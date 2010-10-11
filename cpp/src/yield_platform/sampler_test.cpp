// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/sampler.h"
using namespace YIELD;

DECLARE_TEST_SUITE(babudb)

template <class LockType>
class SamplerTest : public TestCase
{
public:
	SamplerTest( const char* short_description ) : TestCase( short_description,  babudbTestSuite() )
	{ }

	void runTest()
	{
		// Single median
		{
			Sampler<unsigned int, 9, LockType> sampler;
			for ( unsigned char i = 0; i < 9; i++ )
				sampler.setNextSample( i );
			ASSERT_EQUAL( sampler.getSamplesCount(), 9 );
			SamplerStatistics stats = sampler.getStatistics();
			ASSERT_EQUAL( stats.min, 0 );
			ASSERT_EQUAL( stats.max, 8 );
			ASSERT_EQUAL( stats.mean, 4 );
			ASSERT_EQUAL( stats.median, 4 );
			ASSERT_EQUAL( stats.ninetieth_percentile, 8 );
		}

		// Averaged median
		{
			Sampler<unsigned int, 10, LockType> sampler;
			for ( unsigned char i = 0; i < 10; i++ )
				sampler.setNextSample( i );
			ASSERT_EQUAL( sampler.getSamplesCount(), 10 );

			SamplerStatistics stats = sampler.getStatistics();
			ASSERT_EQUAL( stats.min, 0 );
			ASSERT_EQUAL( stats.max, 9 );
			ASSERT_EQUAL( stats.mean, 4.5 );
			ASSERT_EQUAL( stats.median, 4.5 );
			ASSERT_EQUAL( stats.ninetieth_percentile, 9 );
		}

		// All 0's
		{
			Sampler<unsigned int, 10, LockType> sampler;
			for ( unsigned char i = 0; i < 10; i++ )
				sampler.setNextSample( 0 );
			ASSERT_EQUAL( sampler.getSamplesCount(), 10 );
			SamplerStatistics stats = sampler.getStatistics();
			ASSERT_TRUE( stats.min == 0 && stats.max == 0 && stats.mean == 0 && stats.median == 0 && stats.ninetieth_percentile == 0 );
		}

		// More samples than array length
		{
			Sampler<unsigned int, 10, LockType> sampler;
			for ( unsigned char i = 0; i < 15; i++ )
				sampler.setNextSample( i );
			ASSERT_EQUAL( sampler.getSamplesCount(), 10 );
		}
	}
};

SamplerTest<NOPLock> SamplerTestNOPLock_inst( "SamplerTestNOPLock" );
SamplerTest<Mutex> SamplerTestMutex_inst( "SamplerTestMutex" );
