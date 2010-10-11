// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_SAMPLER_H
#define YIELD_PLATFORM_SAMPLER_H

#include "yield/platform/platform_types.h"
#include "yield/platform/locks.h"

#include <climits>
#include <cstring>
#include <algorithm>


namespace YIELD
{
	struct SamplerStatistics
	{
		double min, max;
		double median, mean;
		double ninetieth_percentile;
	};


	template <typename SampleType, size_t ArraySize, class LockType = NOPLock>
	class Sampler
	{
	public:
		Sampler()
		{
			std::memset( samples, 0, sizeof( samples ) );
			samples_pos = samples_count = 0;
			min = ( SampleType )ULLONG_MAX; max = 0; total = 0;
		}

		void setNextSample( SampleType sample )
		{
			if ( samples_lock.try_acquire() )
			{
				samples[samples_pos] = sample;
				samples_pos = ( samples_pos + 1 ) % ArraySize;
				if ( samples_count < ArraySize ) samples_count++;

				if ( sample < min )
					min = sample;
				if ( sample > max )
					max = sample;
				total += sample;

				samples_lock.release();
			}
		}

		size_t getSamplesCount()
		{
			return samples_count;
		}

		SamplerStatistics getStatistics()
		{
			samples_lock.acquire();

			SamplerStatistics stats;

			if ( samples_count > 1 )
			{
				stats.min = ( double )min;
				stats.max = ( double )max;
				stats.mean = ( double )total / ( double )samples_count;

				// Sort for the median and ninetieth percentile
				std::sort( samples, samples + samples_count );

				stats.ninetieth_percentile = ( double )samples[( size_t )( 0.9 * ( double )( samples_count ) )];

				size_t sc_div_2 = samples_count / 2;
				if ( samples_count % 2 == 1 )
					stats.median = ( double )samples[sc_div_2];
				else
				{
					SampleType median_temp = samples[sc_div_2] + samples[sc_div_2-1];
					if ( median_temp > 0 )
						stats.median = median_temp / 2.0;
					else
						stats.median = 0;
				}
			}
			else if ( samples_count == 1 )
				stats.min = stats.max = stats.mean = stats.median = stats.ninetieth_percentile = ( double )samples[0];
			else
				memset( &stats, 0, sizeof( stats ) );

			samples_lock.release();

			return stats;
		}

	protected:
		SampleType samples[ArraySize+1], min, max; SampleType total;
		size_t samples_pos, samples_count;
		LockType samples_lock;
	};
};

#endif
