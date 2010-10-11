// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_LOCKS_H
#define YIELD_PLATFORM_LOCKS_H

#include "yield/platform/platform_types.h"


namespace YIELD
{
	class Mutex
	{
	public:
		Mutex();
		~Mutex();

		// These calls are modeled after the pthread calls they delegate to
		// Have a separate function for timeout_ns == 0 (never block) to avoid an if branch on a critical path
		bool acquire(); // Blocking
		bool try_acquire(); // Never blocks
		bool timed_acquire( timeout_ns_t timeout_ns ); // May block for timeout_ns
		void release();

	private:
		void*  os_handle;
	};


	class CountingSemaphore
	{
	public:
		CountingSemaphore( unsigned int max = 0 ); // max = 0 -> no maximum
		~CountingSemaphore();

		bool acquire(); // Blocking
		bool try_acquire(); // Never blocks
		bool timed_acquire( timeout_ns_t timeout_ns ); // May block for timeout_ns
		void release();

	private:
		void* os_handle;
	};


	class NOPLock
	{
	public:
		inline bool acquire() { return true; }
		inline bool try_acquire() { return true; }
		inline bool timed_acquire( timeout_ns_t ) { return true; }
		inline void release() { }
	};
};

#endif
