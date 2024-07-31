/*
    RZO - A Business Application Framework

    Copyright (C) 2024 Frank Vanderham

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

import { readFile, writeFile } from "node:fs/promises";

import { RZO } from "./base/configuration.js";

import { PgCreator } from "./server/pg-ddl.js";

function urlFor(filename: string, subdir?: string): URL {
    if (subdir) {
        return new URL(`../var/conf/${subdir}/${filename}.json`,
                       import.meta.url);
    }
    return new URL(`../var/conf/${filename}.json`, import.meta.url);
}

function getUrl(filename: string, subdir?: string): URL {
    const result = urlFor(filename, subdir);
    console.log(`Loading ${result}`);
    return result;
}


let creator: PgCreator | null = null;

try {
    const contents = await Promise.all([
        readFile(getUrl("entities"), { encoding: 'utf8' }),
        readFile(getUrl("entities-server", "server"), { encoding: 'utf8' }),
        readFile(getUrl("collections", "server"), { encoding: 'utf8' }),
        readFile(getUrl("config-ddl", "server"), { encoding: 'utf8' })
    ]);
    await RZO.load(contents);

    creator = new PgCreator(RZO);

    await creator.allNewDDL(RZO.save());

    const filenameDate = new Date();
    const filenameDateStr = filenameDate.toISOString().replaceAll(":", "-").
        replaceAll(".", "-");
    const filename = `ddl-${filenameDateStr}.sql`;
    await writeFile(filename, creator.output);

} catch (err) {
    console.error(err);
}

