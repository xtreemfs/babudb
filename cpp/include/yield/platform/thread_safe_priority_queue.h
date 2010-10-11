// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_THREAD_SAFE_PRIORITY_QUEUE_H
#define YIELD_PLATFORM_THREAD_SAFE_PRIORITY_QUEUE_H

#include "yield/platform/locks.h"

#include <queue>
#include <vector>


namespace YIELD
{
	template <class ElementType, class ComparatorType>
	class ThreadSafePriorityQueue : private std::priority_queue<ElementType, std::vector<ElementType>, ComparatorType>
	{
	public:
		bool enqueue( ElementType element )
		{
			lock.acquire();
			std::priority_queue<ElementType, std::vector<ElementType>, ComparatorType>::push( element );
			lock.release();
			return true;
		}

		ElementType try_dequeue()
		{
			lock.acquire();
			if ( std::priority_queue<ElementType, std::vector<ElementType>, ComparatorType>::empty() )
			{
				lock.release();
				return NULL;
			}
			else
			{
				ElementType element = std::priority_queue<ElementType, std::vector<ElementType>, ComparatorType>::top();
				std::priority_queue<ElementType, std::vector<ElementType>, ComparatorType>::pop();
				lock.release();
				return element;
			}
		}

	private:
		Mutex lock;
	};
};

#endif
