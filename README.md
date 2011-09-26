# shib

* http://github.com/tagomoris/shib

## DESCRIPTION

'shib' is hive client application for HiveServer, run as web application on Node.js (v0.4.x) and Kyoto Tycoon.

## INSTALL

### HiveServer

You should run HiveServer at any server near your hadoop cluster.

    hive --service hiveserver

### Kyoto Tyconn

At first, you should install Kyoto Tycoon. See http://fallabs.com/kyototycoon/ .

and yuu can run ktserver on localhost with bin/ktserver.sh.

    bin/ktserver.sh

### Node.js and libraries

To run shib, you must install node.js. At now, nvm and npm is good. See https://github.com/creationix/nvm .

    git clone git://github.com/creationix/nvm.git ~/.nvm
    . ~/.nvm/nvm.sh
    nvm install <VERSION>

And install libraries.

    npm install express kyoto-client jade

### shib

Install shib code.

    git clone git://github.com/tagomoris/shib.git

Configure addresses of HiveServer and Kyoto Tycoon.

    cd shib
    vi config.js

And run.

    bin/ktserver.sh
    node app.js

Shib listens on port 3000. see http://localhost:3000/

* * * * *

## License

Copyright 2011 TAGOMORI Satoshi (tagomoris)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
