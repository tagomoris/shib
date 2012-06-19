# shib

* http://github.com/tagomoris/shib

## DESCRIPTION

'shib' is hive client application for HiveServer, run as web application on Node.js (v0.6.x) and Kyoto Tycoon.

Some extension features are supported:

* Huahin-Manager (Job Controller Proxy with HTTP API) support: Kill hive mapreduce job correctly from shib, with Huahin-Manager
  * see: http://huahin.github.com/huahin-manager/
* Setup queries: options to specify queries executed before main query, like 'create functions ...'
* Default Database: option to specify default database for Hive 0.6 or later

## INSTALL

### HiveServer

You should run HiveServer at any server near your hadoop cluster.

    $ hive --service hiveserver

### Kyoto Tycoon

At first, you should install Kyoto Tycoon. See http://fallabs.com/kyototycoon/ .

and yuu can run ktserver on localhost with bin/ktserver.sh.

    $ bin/ktserver.sh

### Node.js and libraries

To run shib, you must install node.js v0.6.x (and coffee-script for setup). At now, nvm and npm is good. See https://github.com/creationix/nvm .

    $ git clone git://github.com/creationix/nvm.git ~/.nvm
    $ . ~/.nvm/nvm.sh
    $ nvm install <VERSION>

### shib

Install shib code.

    $ git clone git://github.com/tagomoris/shib.git

Install libraries, build kyoto-client, configure addresses of HiveServer and Kyoto Tycoon (and other specifications).

    $ cd shib
    $ npm coffee-script
    $ git submodule update --init
    
    $ cd lib/kyoto-client
    (kyoto-client)$ npm install
    (kyoto-client)$ cake build
    (kyoto-client)$ cd ../..
    
    $ npm install
    $ vi config.js

And run.

    $ bin/ktserver.sh
    $ NODE_PATH=lib node app.js

Shib listens on port 3000. see http://localhost:3000/

You can also run shib with command below for 'production' environment, with production configuration file 'production.js':

    $ npm start

## Configuration

Basic configuration in config.js (or productions.js):

    var servers = exports.servers = {
      hiveserver: {
        host: 'localhost',
        port: 10000,
        support_database: true,
        default_database: 'default',
        setup_queries: []
      },
      kyototycoon: {
        host: 'localhost',
        port: 1978
      },
      huahinmanager: {
        enable: true,
        host: 'localhost',
        port: 9010
      }
    };

Without Huahin-Manager:

      huahinmanager: {
        enable: false
      }

With Hive 0.5 or earlier (without Database):

      hiveserver: {
        host: 'localhost',
        port: 10000,
        support_database: false,
        setup_queries: []
      },

With some setup queries:

        hiveserver: {
          host: 'hiveserver.local',
          port: 10000,
          setup_queries: ["add jar /path/to/jarfile/foo.jar;",
                          "create temporary function foofunc as 'package.of.udf.FooFunc';",
                          "create temporary function barfunc as 'package.of.udf.BarFunc';"]
        },

* * * * *

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
