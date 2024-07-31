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

import pg from "pg";

import { Configuration } from "./base/configuration.js";

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

async function loadCurrentConfig(): Promise<string> {
    const client = new pg.Client();
    await client.connect();
    try {
        const statement = "select info from dbinfo where id = 'config'";
        const result = await client.query(statement);
        if (result.rows.length > 0) {
            return result.rows[0].info as string;
        } else {
            throw new Error("No existing config found in database");
        }
    } finally {
        await client.end();
    }
}

try {
    const intoContents = await Promise.all([
        readFile(getUrl("entities"), { encoding: 'utf8' }),
        readFile(getUrl("entities-server", "server"), { encoding: 'utf8' }),
        readFile(getUrl("collections", "server"), { encoding: 'utf8' }),
        readFile(getUrl("config", "server"), { encoding: 'utf8' })
    ]);
    const intoConfig = new Configuration();
    await intoConfig.load(intoContents);

    const fromContents = await loadCurrentConfig();
    const fromConfig = new Configuration();
    await fromConfig.load([fromContents]);

    const creator = new PgCreator(intoConfig);

    await creator.updateDDL(fromConfig, intoConfig.save());

    const filenameDate = new Date();
    const filenameDateStr = filenameDate.toISOString().replaceAll(":", "-").
        replaceAll(".", "-");
    const filename = `ddl-upd-${filenameDateStr}.sql`;

    await writeFile(filename, creator.output);

} catch (err) {
    console.error(err);
}

