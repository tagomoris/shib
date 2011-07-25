#!/bin/bash

test -f ~/.bashrc && source ~/.bashrc

APPDIR=$(dirname $0)/..
LIBDIR=$APPDIR/lib
cd $APPDIR
export NODE_PATH=$LIBDIR
exec node app.js
