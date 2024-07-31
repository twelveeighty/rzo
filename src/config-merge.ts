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


import { readFile } from "node:fs/promises";
import { argv } from 'node:process';

import { TypeCfg, ClassSpec } from "./base/core.js";

try {
    if (argv.length < 3) {
        throw new Error(
            "Usage: node config-merge conf1 conf2 ...");
    }

    const files: Promise<Buffer>[] = [];
    for (const filename of argv.slice(2)) {
        files.push(readFile(new URL(filename, import.meta.url)));
    }

    const buffers = await Promise.all(files);

    let jsonConfig: TypeCfg<ClassSpec>[] = [];
    for (const buffer of buffers) {
        const configPart =
            JSON.parse(buffer.toString("utf8")) as TypeCfg<ClassSpec>[];
        jsonConfig = jsonConfig.concat(configPart);
    }

    console.log("export const METADATA =");
    console.log(JSON.stringify(jsonConfig, null, 3));
    console.log(";\n");

} catch (err) {
    console.error(err);
}

