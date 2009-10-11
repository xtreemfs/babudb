// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// Index profile for string keys

#ifndef BABUDB_STRINGCONFIGURATION_H
#define BABUDB_STRINGCONFIGURATION_H

#include <cstring>

#include "babudb/key.h"
#include "babudb/buffer.h"
#include "babudb/Database.h"

namespace babudb {

class StringOrder : public KeyOrder {
public:
	virtual bool less(const Buffer& l, const Buffer& r) const {
		int order = strncmp((const char*)l.data,(const char*)r.data, std::min(l.size,r.size));
		if(order == 0)	// on most significant positions they are the same
			return l.size < r.size; // second is longer, so first is before
		else
			return order < 0;
	}

	virtual bool match(const Buffer& l, const Buffer& r) const {
		return strstr((const char*)l.data,(const char*)r.data) == (const char*)l.data;
	}
};

class StringSetOperation {
public:
	StringSetOperation() {}
	StringSetOperation(const string& db, lsn_t lsn, const string& key, const string& value)
		: db(db), lsn(lsn), key(key), value(value) {}
	StringSetOperation(const string& db, lsn_t lsn, const string& key)
		: db(db), lsn(lsn), key(key) {}

	virtual void applyTo(Database& target) const {
		Buffer key_copy = Buffer::createFrom((void*)key.c_str(),key.size());
		if(value.empty())
			target.Remove(db, lsn, key_copy);
		else
			target.Add(db, lsn, key_copy, Buffer::createFrom((void*)value.c_str(),value.size()));
	}

	/* serialize to the log */
	virtual Buffer serialize(Buffer data) const {
		string out = string("s ") + db + ":" + key + "=" + value;
		memcpy((char*)data.data,out.c_str(),out.size()+1);

		return Buffer(data.data,strlen((char*)data.data)+1);
	}

	virtual bool isMarked() const { return false; }

	/* deserialize from the log */
	void deserialize(Buffer data) {
		string op = (char*)data.data;
		size_t del1 = op.find_first_of(":");
		size_t del2 = op.find_first_of("=");

		db = op.substr(2,del1-2);
		key = op.substr(del1+1,del2-del1-1);
		value = op.substr(del2+1);
	}

private:
	string db, key, value;
  lsn_t lsn;
};

};

#endif
