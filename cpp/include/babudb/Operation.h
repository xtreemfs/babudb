// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_OPERATION_H
#define BABUDB_OPERATION_H

#include <cstring>

#include <string>
using std::string;

namespace babudb {

class Data {
public:
	Data(const Data& data) {
		this->data = data.data;
		this->size = data.size;
	}

	explicit Data(void* data, size_t size) {
		this->data = data;
		this->size = size;
	}


	// Data::create* functions allocate memory and copy the data
	// must be .free()d later

	static Data create(size_t size) {
		Data result;
		result.data = (void*)new char[size];
		result.size = size;
		return result;
	}

	static Data createFrom(const string& data) {
		return createFrom((void*)data.c_str(),data.size());
	}

	static Data createFrom(void* data, size_t size) {
		Data result = create(size);
		memcpy(result.data,data,size);
		return result;
	}

	static Data createFrom(int data) {
		Data result = create(sizeof(int));
		memcpy(result.data,&data,sizeof(int));
		return result;
	}


	// Data::wrap only wrap data without allocation and copying

	static Data wrap(int& data) {
		return Data(&data,sizeof(int));
	}

	static Data wrap(long long& data) {
		return Data(&data,sizeof(long long));
	}

	static Data wrap(const string& str) {
		return Data((void*)str.c_str(),str.size());
	}

	/*
	template <class T>
	static Data createFrom(T data) {
		Data result = create(sizeof(T));
		memcpy(result.data,&data,sizeof(T));
		return result;
	}
	*/
	int getAsInt() {
		return *(int*)data;
	}


	// Represents a deleted data item in overlay indices.

	static Data Deleted() {
		return Data(0,-1);
	}

	bool isDeleted() const {
		return data == 0 && size == -1;
	}


	// An empty value

	static Data Empty() {
		return Data(0,0);
	}

	bool isEmpty() const {
		return data == 0 && size != -1;
	}

	void* operator * () {
		return data;
	}

	void free() {
		delete [] (char*)data;
		data = 0; size = 0;
	}

	Data clone() const {
		if(isEmpty())
			return Empty();
		else if(isDeleted())
			return Deleted();
		else
			return createFrom(data,size);
	}

	bool operator == (const string& s) const {
		return string((char*)this->data,this->size) == s;
	}

	bool operator == (const Data& s) const {
		if(this->size != s.size) return false;

		for(int i = 0; i < (int)this->size; ++i )
			if( ((char*)this->data)[i] != ((char*)s.data)[i])
				return false;

		return true;
	}

	void* data;
	size_t size;

private:
	Data() : data(0), size(0) {}
};

class DataHolder {
public:
	DataHolder(const string& str)
		: data(Data::createFrom(str)) {}

	~DataHolder() {
		data.free();
	}

	operator Data () const {
		return data;
	}

	operator Data& () {
		return data;
	}

	Data data;
};

#define MAX_LSN 0xFFFFffff
typedef unsigned int lsn_t;

/** Application-level operation on data

	The template type must also include a
	static createOperation() factory method
*/
#define OPERATION_MAX_SIZE 65536
typedef unsigned char operation_type_t;

class OperationTarget {
public:
	virtual void set(const string& index, const Data& key, const Data& value) = 0;
	virtual void remove(const string& index, const Data& key) = 0;
};

class Operation {
public:
	virtual void applyTo(OperationTarget&) const = 0;
	virtual operation_type_t getType() const = 0;
	virtual Data serialize(Data) const = 0;
};

class OperationFactory {
public:
	virtual Operation* createOperation(Data, operation_type_t) const = 0;
};

};

#endif
