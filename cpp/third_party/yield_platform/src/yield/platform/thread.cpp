// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/thread.h"
using namespace YIELD;

#ifdef _WIN32
#include "yield/platform/windows.h"
#else
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(__linux__)
#include <sched.h>
#include <sys/syscall.h>
#elif defined(__sun)
#include <thread.h>
#include <sys/processor.h>
#include <sys/pset.h>
#endif
#endif

#include "yield/platform/processor_set.h"
#include "yield/platform/time.h"
#include "yield/platform/debug.h"

#include <iostream>
using std::cerr;
using std::endl;


unsigned long Thread::getCurrentThreadId()
{
#if defined(_WIN32)
	return GetCurrentThreadId();
#elif defined(__linux__)
	return syscall( SYS_gettid );
#elif defined(__sun)
	return thr_self();
#else
	return 0;
#endif
}

unsigned long Thread::createTLSKey()
{
#ifdef _WIN32
	return TlsAlloc();
#else
	unsigned long key;
	pthread_key_create( ( pthread_key_t* )&key, NULL );
	return key;
#endif
}

void Thread::setTLS( unsigned long key, void* value )
{
#ifdef _WIN32
    TlsSetValue( key, value );
#else
	pthread_setspecific( key, value );
#endif
}

void* Thread::getTLS( unsigned long key )
{
#ifdef _WIN32
    return TlsGetValue( key );
#else
    return pthread_getspecific( key );
#endif
}

#ifdef _WIN32
//
// Usage: SetThreadName (-1, "MainThread");
// from http://msdn.microsoft.com/library/default.asp?url=/library/en-us/vsdebug/html/vxtsksettingthreadname.asp
//
typedef struct tagTHREADNAME_INFO
{
	DWORD dwType; // must be 0x1000
	LPCSTR szName; // pointer to name (in user addr space)
	DWORD dwThreadID; // thread ID (-1=caller thread)
	DWORD dwFlags; // reserved for future use, must be zero
}
THREADNAME_INFO;
#endif

void Thread::setThreadName( unsigned long os_id, const char* thread_name )
{
#ifdef _WIN32
	THREADNAME_INFO info;
	info.dwType = 0x1000;
	info.szName = thread_name;
	info.dwThreadID = os_id;
	info.dwFlags = 0;

	__try
	{
	    RaiseException( 0x406D1388, 0, sizeof( info ) / sizeof( DWORD ), ( DWORD* ) & info );
	}
	__except( EXCEPTION_CONTINUE_EXECUTION )
	{}
#endif
}

void Thread::yield()
{
#if defined(_WIN32)
	SwitchToThread();
#elif defined(__MACH__)
	pthread_yield_np();
#elif defined(__sun)
	sleep( 0 );
#else
	pthread_yield();
#endif
}

void Thread::sleep( timeout_ns_t timeout_ns )
{
#ifdef _WIN32
	Sleep( ( DWORD )( timeout_ns / NS_IN_MS ) );
#else
	struct timespec sleep_ts = Time::getNSFromNullTimespec( timeout_ns );
	nanosleep( &sleep_ts, NULL );
#endif
}

Thread::Thread()
{
	os_handle = 0;
	os_id = 0;
	is_running = false;
}

Thread::~Thread()
{
#ifdef _WIN32
	if ( os_handle ) CloseHandle( os_handle );
#endif
}

bool Thread::setProcessorAffinity( unsigned short logical_processor_i )
{
	if ( os_id != 0 )
	{
#if defined(_WIN32)
		return SetThreadAffinityMask( os_handle, ( 1L << logical_processor_i ) ) != 0;
#else
#if defined(__linux__)
		cpu_set_t cpu_set;
		CPU_ZERO( &cpu_set );
		CPU_SET( logical_processor_i, &cpu_set );
		return sched_setaffinity( 0, sizeof( cpu_set ), &cpu_set ) == 0;
#elif defined(__sun)
		return processor_bind( P_LWPID, thr_self(), logical_processor_i, NULL ) == 0;
#endif
#endif
	}
	else
		return false;
}

bool Thread::setProcessorAffinity( const ProcessorSet& logical_processor_set )
{
	if ( os_id != 0 )
	{
#if defined(_WIN32)
		return SetThreadAffinityMask( os_handle, logical_processor_set.mask ) != 0;	
#elif defined(__linux__)
		return sched_setaffinity( 0, sizeof( cpu_set_t ), ( cpu_set_t* )logical_processor_set.cpu_set ) == 0;
#elif defined(__sun)
		return pset_bind( logical_processor_set.psetid, P_LWPID, os_id, NULL ) == 0;
#endif
	}
	else
		return false;
}

void Thread::startThread()
{
	if ( !this->is_running )
	{
#ifdef _WIN32
		os_handle = CreateThread( NULL, 0, thread_stub, this, NULL, &os_id );
#else
		pthread_attr_t attr;

		pthread_attr_init( &attr );
		pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_DETACHED );

		pthread_create( ( pthread_t* )&os_handle, &attr, &thread_stub, ( void* )this );

		pthread_attr_destroy( &attr );
#endif
	}
}

#ifdef _WIN32
unsigned long __stdcall Thread::thread_stub( void* pnt )
#else
void* Thread::thread_stub( void* pnt )
#endif
{
	Thread* this_thread = ( Thread* )pnt;
	if ( !this_thread->is_running )
	{
		this_thread->is_running = true;
#if defined(__linux__)
		this_thread->os_id = syscall( SYS_gettid );
#elif defined(__MACH__)
		this_thread->os_id = 0; // ???
#elif defined(__sun)
		this_thread->os_id = thr_self();
#endif
		this_thread->run();
		this_thread->is_running = false;
		return 0;
	}
	else
		return 0;
}

