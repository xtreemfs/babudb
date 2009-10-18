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
#include "babudb/database.h"
#include "babudb/log/log_section.h"

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

class StringSetOperation : public Serializable {
public:
	StringSetOperation() {}
	StringSetOperation(const string& db, const string& key, const string& value)
		: db(db), key(key), value(value) {}
	StringSetOperation(const string& db, const string& key)
		: db(db), key(key) {}

	virtual void ApplyTo(Database& target, lsn_t lsn) const {
		if(value.empty())
			target.Remove(db, lsn, DataHolder(key));
		else
			target.Add(db, lsn, DataHolder(key), DataHolder(value));
	}

  size_t GetSize() const {
    return 2 + db.size() + 1 + key.size() + 1 + value.size() + 1;
  }

	/* serialize to the log */
	virtual void Serialize(const Buffer& data) const {
		string out = string("s ") + db + ":" + key + "=" + value;
		memcpy((char*)data.data, out.c_str(), out.size()+1);
	}

	/* deserialize from the log */
	void Deserialize(Buffer data) {
		string op = (char*)data.data;
		size_t del1 = op.find_first_of(":");
		size_t del2 = op.find_first_of("=");

		db = op.substr(2,del1-2);
		key = op.substr(del1+1,del2-del1-1);
		value = op.substr(del2+1);
	}

	string db, key, value;
};

};

#endif
