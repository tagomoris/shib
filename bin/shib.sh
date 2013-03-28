#!/bin/bash

APPDIR=$(dirname $0)/..
cd $APPDIR

LIBDIR="$(pwd)"/lib
export NODE_PATH=$LIBDIR:$NODE_PATH

export NODE_ENV=production
exec node app.js
