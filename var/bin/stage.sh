#!/usr/bin/bash

#
#    RZO - A Business Application Framework
#
#    Copyright (C) 2024-2026 Frank Vanderham
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
set -eu

VAR="../var"
SRC="../src"
MODS="../node_modules"
BUNDLE="../var/web-config-bundle.json"
METADATA="./metadata.js"
BOOTSTRAP="../deps/bootstrap-5.3.8-dist"
POUCHDB="../deps/pouchdb-9.0.0"

if [[ $(basename "$(pwd)") = "build" ]]; then
    node "config-merge.js" "${BUNDLE}" "${METADATA}"

    cp -v "$VAR/img/rzo.png" .
    cp -v "$SRC/index.html" .
    cp -v "$SRC/index.css" .

    mkdir -p ./popperjs/
    rsync -av "$MODS/@popperjs/core/dist/esm/" ./popperjs/

    mkdir -p ./bootstrap/css
    cp -v "$BOOTSTRAP/css/bootstrap.min."* ./bootstrap/css/

    mkdir -p ./bootstrap/js
    cp -v "$BOOTSTRAP/js/bootstrap.esm.min."* ./bootstrap/js/

    mkdir -p ./pouchdb/
    cp -v  "$POUCHDB/pouchdb-9.0.0.js" ./pouchdb/
    cp -v  "$POUCHDB/pouchdb.find.js" ./pouchdb/

else
    echo "This script must be run from the 'build' directory"
fi

