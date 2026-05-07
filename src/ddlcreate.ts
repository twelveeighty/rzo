/*
    RZO - A Business Application Framework

    Copyright (C) 2024-2026 Frank Vanderham

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
import { ConfigBundleSpec, TypeCfg } from "./base/core.js";
import { RZO } from "./base/configuration.js";
import { PgCreator } from "./server/pg-ddl.js";

let creator: PgCreator | null = null;

try {
    const bundleUrl =
        new URL("../var/ddl-config-bundle.json", import.meta.url);
    console.log(`Using bundle: ${bundleUrl}`);
    const bundleContents = await readFile(bundleUrl, { encoding: "utf8" });
    const bundle = JSON.parse(bundleContents) as TypeCfg<ConfigBundleSpec>;
    const contentsP: Promise<string>[] = [];
    for (const config of bundle.spec.configurations) {
        const url = new URL(
            `${bundle.spec.home}/${config}.json`, import.meta.url);
        contentsP.push(readFile(url, { encoding: 'utf8' }));
    }
    const contents = await Promise.all(contentsP);
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

