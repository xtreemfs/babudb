// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_STRINGCONFIGURATION_H
#define BABUDB_STRINGCONFIGURATION_H

#include <cstring>

#include "babudb/KeyOrder.h"
#include "babudb/Operation.h"

namespace babudb {

class StringOrder : public KeyOrder {
public:
	virtual bool less(const Data& l, const Data& r) const {
		int order = strncmp((const char*)l.data,(const char*)r.data, std::min(l.size,r.size));
		if(order == 0)	// on most significant positions they are the same
			return l.size < r.size; // second is longer, so first is before
		else
			return order < 0;
	}

	virtual bool match(const Data& l, const Data& r) const {
		return strstr((const char*)l.data,(const char*)r.data) == (const char*)l.data;
	}
};

class StringSetOperation : public Operation {
public:
	StringSetOperation() {}
	StringSetOperation(const string& db, const string& key, const string& value)
		: db(db), key(key), value(value) {}
	StringSetOperation(const string& db, const string& key)
		: db(db), key(key) {}

	virtual void addYourself(OperationTarget& target) const {
		Data key_copy = Data::createFrom((void*)key.c_str(),key.size());
		if(value.empty())
			target.remove(db, key_copy);
		else
			target.set(db, key_copy, Data::createFrom((void*)value.c_str(),value.size()));
	}

	/* serialize to the log */
	virtual Data serialize(Data data) const {
		string out = string("s ") + db + ":" + key + "=" + value;
		memcpy((char*)data.data,out.c_str(),out.size()+1);

		return Data(data.data,strlen((char*)data.data)+1);
	}

	/* to identify the type in the log */
	virtual operation_type_t getType() const {
		return 1;
	}

	virtual bool isMarked() const { return false; }

	/* deserialize from the log */
	void deserialize(Data data) {
		string op = (char*)data.data;
		size_t del1 = op.find_first_of(":");
		size_t del2 = op.find_first_of("=");

		db = op.substr(2,del1-2);
		key = op.substr(del1+1,del2-del1-1);
		value = op.substr(del2+1);
	}

private:
	string db,key,value;
};

class StringOperationFactory : public OperationFactory {
public:
	virtual Operation* createOperation(Data data, operation_type_t type) const {
		StringSetOperation* op = new StringSetOperation();
		op->deserialize(data);
		return op;
	}
};

};

#endif
