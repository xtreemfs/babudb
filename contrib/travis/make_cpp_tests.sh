#!/bin/bash
set -ev

BABUDB_DIR=$PWD

mkdir $BABUDB_DIR/cpp/build
cd $BABUDB_DIR/cpp/build

cmake ..
make

