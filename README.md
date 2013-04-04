# shib

* http://github.com/tagomoris/shib

## DESCRIPTION

'shib' is hive client application for HiveServer, run as web application on Node.js.

Some extension features are supported:

* Huahin-Manager (Job Controller Proxy with HTTP API) support: Kill hive mapreduce job correctly from shib, with Huahin-Manager
  * see: http://huahin.github.com/huahin-manager/
* Setup queries: options to specify queries executed before main query, like 'create functions ...'
* Default Database: option to specify default database for Hive 0.6 or later

### Versions

'shib' versions are:

* v0.1 series
  * uses KT, depends on node v0.6.x
  * see `v0.1` tag
* v0.2 series
  * current status of master branch
  * uses local filesystem instead of KT, depends on latest node (v0.8.x, v0.10.x)
  * higher performance and updated features

**There are no compatibilities of data (query and results) between v0.1 and v0.2**. And there are no convert tools now.

## INSTALL

### HiveServer

You should run HiveServer at any server near your hadoop cluster.

    $ hive --service hiveserver

### Node.js

To run shib, you must install node.js, and export PATH for installed node.

### shib

Install shib code.

    $ git clone git://github.com/tagomoris/shib.git

Install libraries, configure addresses of HiveServer (and other specifications).

    $ npm install
    $ vi config.js

And run.

    $ NODE_PATH=lib node app.js

Shib listens on port 3000. see http://localhost:3000/

You can also run shib with command below for 'production' environment, with production configuration file 'production.js':

    $ npm start

## Configuration

Basic configuration in config.js (or productions.js):

```js
var servers = exports.servers = {
  listen: 3000,
  fetch_lines: 1000,
  setup_queries: [],
  storage: {
    datadir: './var'
  },
  executer: {
    name: 'hiveserver', // or 'hiveserver2' (not implemented) or 'huahinmanager' (not implemented)
    host: 'localhost',
    port: 10000,
    support_database: true,
    default_database: 'default'
  },
  monitor: null
  /* not implemented
  monitor: {
    name : 'huahin_mrv1', // or 'huahin_yarn'
    host: 'localhost',
    port: 9010
  }
  */
};
```

With Hive 0.5 or earlier (without Database):

```js
  executer: {
    name: 'hiveserver', // or 'hiveserver2' (not implemented) or 'huahinmanager' (not implemented)
    host: 'localhost',
    port: 10000,
    support_database: false
  },
```

With some setup queries:

```js
  setup_queries: [
    "add jar /path/to/jarfile/foo.jar",
    "create temporary function foofunc as 'package.of.udf.FooFunc'",
    "create temporary function barfunc as 'package.of.udf.BarFunc'"
  ],
```

* * * * *

## TODO

* handle syntax error and other errors with real hiveserver
* set/test execute expiration
* more executer such as 'hiveserver2' and 'huahinmanager'
* more monitor such as 'huahin\_mrv1' and 'huahin\_yarn'

## License

Copyright 2011- TAGOMORI Satoshi (tagomoris)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
