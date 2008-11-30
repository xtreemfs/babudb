// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_TEST_HELPER_H
#define BABUDB_TEST_HELPER_H

#include <string>
using std::string;

#include "babudb/Operation.h"

class DummyOperation : public babudb::Operation {
public:
	DummyOperation(int i) : value(i) {}

	virtual void addYourself(babudb::OperationTarget& target) const {}

	virtual babudb::operation_type_t getType() const { return 1; }
	virtual bool isMarked() const { return value % 2 == 0; }
	virtual babudb::Data serialize(babudb::Data data) const {
		*((int*)data.data) = value;
		return babudb::Data(data.data, sizeof(int));
	}

	virtual DummyOperation& deserialize(babudb::Data data) {
		value = *((int*)data.data);
		return *this;
	}

	int value;
};

#endif
