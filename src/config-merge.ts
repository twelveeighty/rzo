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
import { argv } from 'node:process';
import { TypeCfg, ClassSpec, ConfigBundleSpec } from "./base/core.js";

try {
    if (argv.length != 4) {
        throw new Error(
            "Usage: node config-merge <bundle> <output>");
    }
    const bundleUrl = new URL(argv[2], import.meta.url);
    const outputUrl =  new URL(argv[3], import.meta.url);
    console.log(`Using bundle: ${bundleUrl}`);
    console.log(`Output to: ${outputUrl}`);
    const bundleContents = await readFile(bundleUrl, { encoding: "utf8" });
    const bundle = JSON.parse(bundleContents) as TypeCfg<ConfigBundleSpec>;
    const contentsP: Promise<string>[] = [];
    for (const config of bundle.spec.configurations) {
        const url = new URL(
            `${bundle.spec.home}/${config}.json`, import.meta.url);
        console.log(`> ${url}`);
        contentsP.push(readFile(url, { encoding: 'utf8' }));
    }
    const configuration = await Promise.all(contentsP);
    let jsonConfig: TypeCfg<ClassSpec>[] = [];
    for (const config of configuration) {
        const configPart = JSON.parse(config) as TypeCfg<ClassSpec>[];
        jsonConfig = jsonConfig.concat(configPart);
    }
    let output: string[] = ["export const METADATA = "];
    output = output.concat(JSON.stringify(jsonConfig, null, 3));
    output.push(";\n");
    await writeFile(outputUrl, output);
} catch (err) {
    console.error(err);
    process.exit(1);
}

