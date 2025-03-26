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
shopt -s globstar

SRC="../../src"
HEADER="../conf/licenseheader.ts"
CHECK="RZO - A Business Application Framework"

if [[ $(basename "$(pwd)") = "bin" ]]; then
    for file in "$SRC"/**/*.ts; do
        if ! grep -q "$CHECK" $file; then
            echo "Changing $file"
            cat $HEADER $file > "$file.tmp"
            mv -f "$file.tmp" "$file"
        fi
    done
else
    echo "This script must be run from the 'var/bin' directory"
fi

