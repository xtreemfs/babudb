// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_INTCONFIGURATION_H
#define BABUDB_INTCONFIGURATION_H

#include "babudb/KeyOrder.h"

namespace babudb {

class Int32Order : public KeyOrder {
public:
	virtual bool less(const Data& l, const Data& r) const {
		return *(int*)l.data < *(int*)r.data;
	}

	virtual bool match(const Data& l, const Data& r) const {
		return *(int*)l.data == *(int*)r.data;
	}
};

/*	Represents an array of N fixed-size columns of type T.

	TODO: replace with recursive template
*/

template <class T, int N>
class MultiOrder : public KeyOrder {
public:
	virtual bool less(const Data& l, const Data& r) const {
		T* left = (T*)l.data;
		T* right = (T*)r.data;

		for(i=N-1; i >= 0; i--) {
			if(left[i] >= right[i])
				return false;
		}

		return true;
	}

	virtual bool match(const Data& l, const Data& r) const {
		T* left = (T*)l.data;
		T* right = (T*)r.data;

		for(i=N-1; i >= 0; i--) {
			if(left[i] != right[i])
				return false;
		}

		return true;
	}
};

};
#endif
