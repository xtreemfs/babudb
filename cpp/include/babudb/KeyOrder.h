// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_KEYORDER_H
#define BABUDB_KEYORDER_H

#include "Operation.h"

namespace babudb {

class KeyOrder {
public:
	/* a strict less than operator < */
	virtual bool less(const Data& l, const Data& r) const = 0;
};

/* The operator that implements less with prefix matches for std::map using KeyOrder */
class MapCompare {
public:
	explicit MapCompare(KeyOrder& order) : order(order) {}
	bool operator () (const Data& l, const Data& r) const {
		return order.less(l,r);
	}

private:
	KeyOrder& order;
};


/*inline bool MapCompare::operator () (const std::pair<Data,bool>& l, const std::pair<Data,bool>& r) const {
//	ASSERT_FALSE(l.second && r.second, "Map Wrong");

	if(l.second && order.match(r.first,l.first)) // l is prefix key
		return false;
	else if(r.second && order.match(l.first,r.first)) // r is prefix key
		return false;
	else
		return order.less(l.first,r.first);
}
*/

/* The operator that implements simple less for std::map using KeyOrder */
class SimpleMapCompare {
public:
	explicit SimpleMapCompare(KeyOrder& order) : order(order) {}
	bool operator () (const Data& l, const Data& r) const {
		return order.less(l,r);
	}

private:
	KeyOrder& order;
};

};

#endif

