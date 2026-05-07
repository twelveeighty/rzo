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


import { readFile } from "node:fs/promises";
import { env, argv } from "node:process";
import { ConfigBundleSpec, TypeCfg } from "./base/core.js";
import { RZO } from "./base/configuration.js";
import { PolicyConfiguration } from "./server/policy.js";

function logEnvVars(): void {
    const dbId = "RZOID" in env ? "" + env.RZOID : "";
    const dbName = "PGDATABASE" in env ? "" + env.PGDATABASE : "";
    const dbUser = "PGUSER" in env ? "" + env.PGUSER : "";
    console.log(`RZOID = [${dbId}]`);
    console.log(`PGDATABASE = [${dbName}]`);
    console.log(`PGUSER = [${dbUser}]`);
}

function terminateServer(): void {
    console.log("Stopping all tasks...");
    RZO.stopAsyncTasks()
    .then(() => {
        console.log("Shutdown completed");
    })
    .catch((error) => {
        console.error(error);
    })
    .finally(() => {
        process.exit(0);
    });
}

try {
    logEnvVars();
    const policyConfig = new PolicyConfiguration();
    RZO.policyConfig = policyConfig;
    let bundleName = "config-bundle";
    if (argv.length == 3 && argv[2] == "--bootstrap") {
        console.log("WARNING --- RUNNING IN BOOTSTRAP MODE");
        bundleName = "bootstrap-config-bundle";
    }
    const bundleContents = await readFile(
        new URL(`../var/${bundleName}.json`, import.meta.url),
        { encoding: 'utf8' });
    const bundle = JSON.parse(bundleContents) as TypeCfg<ConfigBundleSpec>;
    console.log(JSON.stringify(bundle, null, 3));
    const contentsP: Promise<string>[] = [];
    for (const config of bundle.spec.configurations) {
        const url = new URL(
            `${bundle.spec.home}/${config}.json`, import.meta.url);
        console.log(`Loading config file: ${url}`);
        contentsP.push(readFile(url, { encoding: 'utf8' }));
    }
    const contents = await Promise.all(contentsP);
    const policiesP: Promise<string>[] = [];
    for (const config of bundle.spec.policies) {
        const url = new URL(
            `${bundle.spec.home}/${config}.json`, import.meta.url);
        console.log(`Loading policy file: ${url}`);
        policiesP.push(readFile(url, { encoding: 'utf8' }));
    }
    const policies = await Promise.all(policiesP);
    await RZO.load(contents);
    await policyConfig.load(policies, RZO);
    process.on("SIGTERM", () => {
        terminateServer();
    });
    console.log("Starting Async Tasks...");
    await RZO.startAsyncTasks();
    console.log("All Async Tasks have been started");

} catch (err) {
    console.error(err);
    terminateServer();
}

