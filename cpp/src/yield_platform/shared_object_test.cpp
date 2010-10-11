// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/shared_object.h"
using namespace YIELD;


class ExposedSharedObject : public SharedObject
{
public:
	ExposedSharedObject()
	{
		deleted = false;
	}

	~ExposedSharedObject()
	{
		deleted = true;
	}

	static bool deleted;
};

bool ExposedSharedObject::deleted = false;

TEST( SharedObject, babudb )
{
	ExposedSharedObject* so = new ExposedSharedObject;
	SharedObject::decRef( so );
	ASSERT_TRUE( ExposedSharedObject::deleted );

	so = new ExposedSharedObject;
	SharedObject::incRef( so );
	SharedObject::decRef( so );
	ASSERT_FALSE( ExposedSharedObject::deleted );
	SharedObject::decRef( so );
	ASSERT_TRUE( ExposedSharedObject::deleted );
}

TEST( auto_SharedObject, babudb )
{
	// Construct, go out of scope
	{
		auto_SharedObject<ExposedSharedObject> auto_so = new ExposedSharedObject;
	}
	ASSERT_TRUE( ExposedSharedObject::deleted );

	// Construct, copy, let copy go out of scope
	{
		auto_SharedObject<ExposedSharedObject> auto_so = new ExposedSharedObject;
		{
			auto_SharedObject<ExposedSharedObject> auto_so2( auto_so );
		}
		ASSERT_FALSE( ExposedSharedObject::deleted );
	}
	ASSERT_TRUE( ExposedSharedObject::deleted );

	// Construct, reassign to NULL
	{
		auto_SharedObject<ExposedSharedObject> auto_so = new ExposedSharedObject;
		auto_so = NULL;
		ASSERT_TRUE( ExposedSharedObject::deleted );
		ASSERT_TRUE( auto_so.get() == NULL );
	}

	// Construct, get
	{
		ExposedSharedObject* so = new ExposedSharedObject;
		auto_SharedObject<ExposedSharedObject> auto_so = so;
		ASSERT_TRUE( auto_so.get() == so );
	}
	ASSERT_TRUE( ExposedSharedObject::deleted );

	// Construct, release
	{
		auto_SharedObject<ExposedSharedObject> auto_so = new ExposedSharedObject;
		ExposedSharedObject* so = auto_so.release();
		ASSERT_FALSE( ExposedSharedObject::deleted );
		ASSERT_TRUE( auto_so.get() == NULL );
		SharedObject::decRef( so );
		ASSERT_TRUE( ExposedSharedObject::deleted );
	}

	// Construct to NULL, reset
	{
		ExposedSharedObject* so = new ExposedSharedObject;
		auto_SharedObject<ExposedSharedObject> auto_so;
		auto_so.reset( so );
		ASSERT_TRUE( auto_so.get() == so );
	}
	ASSERT_TRUE( ExposedSharedObject::deleted );
}
