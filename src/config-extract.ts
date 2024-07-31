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

import pg from "pg";

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
    const contents = await loadCurrentConfig();
    const json = JSON.parse(contents);
    console.log(JSON.stringify(json, null, 3));

} catch (err) {
    console.error(err);
}

