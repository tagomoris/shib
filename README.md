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

Latest version of 'shib' is v0.2.0.

'shib' versions are:

* v0.1 series
  * uses KT, depends on node v0.6.x
  * see `v0.1` tag
* v0.2 series
  * current status of master branch
  * uses local filesystem instead of KT, depends on latest node (v0.8.x, v0.10.x)
  * higher performance, more safe Web UI and updated features

**There are no compatibilities of data (query and results) between v0.1 and v0.2**. And there are no convert tools now.

## INSTALL

### HiveServer

You should run HiveServer at any server near your hadoop cluster.

    $ hive --service hiveserver

Or, hiveserver2

    $ hive --service hiveserver2

NOTE: hiveserver should be configured as `hive.server2.authentication=NOSASL`, and engine `hiveserver2` does not support databases.

### Node.js

To run shib, you must install node.js, and export PATH for installed node.

### Huahin Manager

To show map/reduce status, and/or to kill actual map/reduce jobs behind hive query, shib can use Huahin Manager. Current version supports only 'Huahin Manager CDH4 + MRv1' only.

http://huahinframework.org/huahin-manager/

### shib

Install shib code.

    $ git clone git://github.com/tagomoris/shib.git

Install libraries, configure addresses of HiveServer (and other specifications).

    $ npm install
    $ vi config.js

And run.

    $ npm start

Shib listens on port 3000. see http://localhost:3000/

You can also run shib with command below for 'production' environment, with production configuration file 'production.js':

    $ NODE_ENV=production NODE_PATH=lib node app.js

## Configuration

Basic configuration in config.js (or productions.js):

```js
var servers = exports.servers = {
  listen: 3000,
  fetch_lines: 1000,
  query_timeout: null,
  setup_queries: [],
  storage: {
    datadir: './var'
  },
  executer: {
    name: 'hiveserver', // or 'hiveserver2'
    host: 'localhost',
    port: 10000,
    support_database: true,
    default_database: 'default'
  },
  monitor: null
  /* if you are using Huahin Manager MRv1
  monitor: {
    name : 'huahin_mrv1',
    host: 'localhost',
    port: 9010
  }
  */
};
```

With Hive 0.5 or earlier, or hiveserver2 (without database support):

```js
  executer: {
    name: 'hiveserver', // or 'hiveserver2'
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

* 'hiveserver2' database support (for Hive 0.13 or later?)
* More monitor over 'hiveserver2'
* Support multi engines at same time
* Support Presto

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
