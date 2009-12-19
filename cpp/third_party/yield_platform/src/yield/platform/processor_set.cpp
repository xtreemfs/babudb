// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/processor_set.h"
using namespace YIELD;


#if defined(_WIN32)
#include "yield/platform/windows.h"
#elif defined(__linux)
#include <sched.h>
#elif defined(__sun)
#include <sys/pset.h>
#endif


ProcessorSet::ProcessorSet()
{
#if defined(_WIN32)
	mask = 0;
#elif defined(__linux)
	cpu_set = new cpu_set_t;
#elif defined(__sun)
	psetid = PS_NONE; // Don't pset_create until we actually use the set, to avoid leaving state in the system
#endif
}

ProcessorSet::~ProcessorSet()
{
#if defined(__linux)
	delete ( cpu_set_t* )cpu_set;
#elif defined(__sun)
	if ( psetid != PS_NONE ) pset_destroy( psetid );
#endif
}

void ProcessorSet::set( unsigned short processor_i )
{
#if defined(_WIN32)
	mask |= ( 1L << processor_i );
#elif defined(__linux)
	CPU_SET( processor_i, ( cpu_set_t* )cpu_set );
#elif defined(__sun)
	if ( psetid == PS_NONE ) pset_create( &psetid );
	pset_assign( psetid, processor_i, NULL );
#endif
}

bool ProcessorSet::isset( unsigned short processor_i )
{
#if defined(_WIN32)
	unsigned long bit = ( 1L << processor_i );
	return ( bit & mask ) == bit;
#elif defined(__linux)
	return CPU_ISSET( processor_i, ( cpu_set_t* )cpu_set );
#elif defined(__sun)
	return psetid != PS_NONE && pset_assign( PS_QUERY, processor_i, NULL ) == psetid;
#endif
}

void ProcessorSet::clear( unsigned short processor_i )
{
#if defined(_WIN32)
	unsigned long bit = ( 1L << processor_i );
	if ( ( bit & mask ) == bit )
		mask ^= bit;
#elif defined(__linux)
	CPU_CLR( processor_i, ( cpu_set_t* )cpu_set );
#elif defined(__sun)
	if ( psetid != PS_NONE )
		pset_assign( PS_NONE, processor_i, NULL );
#endif
}
