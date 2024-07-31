#!/usr/bin/bash

#
#    RZO - A Business Application Framework
#
#    Copyright (C) 2024 Frank Vanderham
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
CFG="$VAR/conf"
CLT="$CFG/client"
SRC="../src"
MODS="../node_modules"

if [[ $(basename "$(pwd)") = "build" ]]; then
    echo "In build directory"

    node "config-merge.js" \
        "$CFG/entities.json" \
        "$CFG/personas.json" \
        "$CLT/collections.json" \
        "$CLT/config.json" \
        > "./metadata.js"

    cp -v "$VAR/img/rzo.png" .
    cp -v "$SRC/index.html" .
    cp -v "$SRC/index.css" .

    mkdir -p ./popperjs/
    rsync -av "$MODS/@popperjs/core/dist/esm/" ./popperjs/
else
    echo "This script must be run from the 'build' directory"
fi

