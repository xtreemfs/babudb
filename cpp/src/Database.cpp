// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/Database.h"
#include "babudb/LogIterator.h"
#include "babudb/LookupIterator.h"

#include "Log.h"
#include "LogIndex.h"
#include "DataIndex.h"
#include "IndexMerger.h"
using namespace babudb;

#include <sstream>
using std::pair;

#include "yield/platform/memory_mapped_file.h"
using namespace YIELD;

Database::Database(const string& name, const OperationFactory& op_f)
: name(name), factory(op_f), merger(NULL), is_running(false) {
	log = new Log(name);
}

Database::~Database() {
	delete log;

	for(map<string,DataIndex*>::iterator i = indices.begin(); i != indices.end(); ++i)
		delete i->second;
}

void Database::startup() {
	ASSERT_FALSE(is_running);

	// load latest intact ImmutableIndex and find minimum LSN to recover from
	lsn_t min_lsn = MAX_LSN;
	for(map<string,DataIndex*>::iterator i = indices.begin(); i != indices.end(); ++i) {
		lsn_t last_persistent_lsn = i->second->loadLatestIntactImmutableIndex();
		i->second->advanceTail(last_persistent_lsn + 1);
		min_lsn = std::min(min_lsn, last_persistent_lsn);
	}

	log->loadRequiredLogSections(min_lsn);
	DataIndexOperationTarget target(log,indices,-1);
	log->replayToLogIndices(target, factory, min_lsn);

	is_running = true;
}

void Database::shutdown() {
	ASSERT_TRUE(is_running);
	log->close();
}

const OperationFactory& Database::getOperationFactory() {
	return factory;
}

void Database::registerIndex(const string& index_name, KeyOrder& order) {
	ASSERT_FALSE(is_running);
	indices.insert(std::make_pair(index_name,new DataIndex(name + "-" + index_name,order)));
}

void Database::execute(Operation& op) {
	lsn_t current_lsn = log->getTail()->appendOperation(op);
	DataIndexOperationTarget target(log,indices,current_lsn);
	op.applyTo(target);
}

void Database::commit() {
	log->getTail()->commitOperations();
}

Data Database::lookup(const string& index, const Data& key) {
	return indices[index]->lookup(key);
}

LookupIterator Database::lookup(const string& index, const Data& lower, const Data& upper) {
	return indices[index]->lookup(lower, upper);
}

LogIterator Database::begin() {
	return log->begin();
}

LogIterator Database::end(){
	return log->end();
}

void Database::cleanup(lsn_t from_lsn, const string& to) {
	for(map<string,DataIndex*>::iterator i = indices.begin(); i != indices.end(); ++i)
		i->second->cleanup(from_lsn, to);
	log->cleanup(from_lsn, to );
}

bool Database::migrate(const string& index, int steps) {
	if(merger) {
		if(!merger->finished()) {
			merger->proceed(steps); // merge in progress
			return false;
		}
		else {
			string target_name = merger->target_name;
			lsn_t target_last_lsn = merger->target_last_lsn;
			delete merger;
			merger = NULL;

			// replace new section with old section
			auto_ptr<YIELD::MemoryMappedFile> mfile(new YIELD::MemoryMappedFile(target_name, 1024*1024, O_RDONLY));
			indices[index]->advanceImmutableIndex(new ImmutableIndex(mfile, indices[index]->getOrder(), target_last_lsn));
			return true;
		}
	} else {
		log->advanceTail();  // close back() section, if any

		for(map<string,DataIndex*>::iterator i = indices.begin(); i != indices.end(); ++i)
			i->second->advanceTail(log->getLastLSN());

		LogIndex* log_index = indices[index]->getMigratableIndex();

		if(!log_index)
			return true;

		ImmutableIndex* base = indices[index]->getMigrationBase();

		if(base) {
			ASSERT_TRUE(base->getLastLSN() + 1 == log_index->getFirstLSN());
		} else {
			ASSERT_TRUE(log_index->getFirstLSN() == 1);
		}

		lsn_t lsn = log_index->getLastLSN();

		std::ostringstream pers_name;
		pers_name << name << "-" << index << "_" << lsn << ".idx";
		auto_ptr<YIELD::MemoryMappedFile> mfile(new YIELD::MemoryMappedFile(pers_name.str(), 1024*1024, O_CREAT|O_RDWR|O_SYNC));
		auto_ptr<ImmutableIndexWriter> target(new ImmutableIndexWriter(mfile, 64 * 1024));

		if(base)
			merger = new IndexMerger(target, *log_index, *base, indices[index]->getOrder());
		else
			merger = new IndexCreator(target, *log_index, indices[index]->getOrder());

		merger->target_name = pers_name.str();
		merger->target_last_lsn = lsn;
		return migrate(index, steps);
	}
}
