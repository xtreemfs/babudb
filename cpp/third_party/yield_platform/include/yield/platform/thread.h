// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_THREAD_H
#define YIELD_PLATFORM_THREAD_H

#include "yield/platform/platform_types.h"


namespace YIELD
{
	class ProcessorSet;


	class Thread
	{
	public:
		static unsigned long getCurrentThreadId();
		static void setCurrentThreadName( const char* thread_name ) { setThreadName( getCurrentThreadId(), thread_name ); }
		static unsigned long createTLSKey();
		static void setTLS( unsigned long key, void* value );
		static void* getTLS( unsigned long key );

		static void yield();
		static void sleep( timeout_ns_t timeout_ns );

		Thread(); // runnable = this
		virtual ~Thread();

		virtual void startThread();
		bool isRunning() { return is_running; }
		unsigned long getId() { return os_id; }
		void setName( const char* name ) { setThreadName( getId(), name ); }
		bool setProcessorAffinity( unsigned short logical_processor_i );
		bool setProcessorAffinity( const ProcessorSet& logical_processor_set );

		virtual void run() = 0;

	private:
#ifdef _WIN32
		static unsigned long __stdcall thread_stub( void* );
#else
		static void* thread_stub( void* );
#endif

		unsigned long os_id;
#ifdef _WIN32
		void* os_handle;
#else
		unsigned long os_handle;
#endif
		bool is_running;

		static void setThreadName( unsigned long, const char* );
	};
};

#endif
