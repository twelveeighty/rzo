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
import { env, argv } from "node:process";

import { RZO } from "./base/configuration.js";

import { PolicyConfiguration } from "./server/policy.js";

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
    let config = "config";
    if (argv.length == 3 && argv[2] == "--bootstrap") {
        console.log("WARNING --- RUNNING IN BOOTSTRAP MODE");
        config = "config-bootstrap";
    }
    const contents = await Promise.all([
        readFile(getUrl("entities"), { encoding: 'utf8' }),
        readFile(getUrl("entities-server", "server"), { encoding: 'utf8' }),
        readFile(getUrl("personas"), { encoding: 'utf8' }),
        readFile(getUrl("collections", "server"), { encoding: 'utf8' }),
        readFile(getUrl(config, "server"), { encoding: 'utf8' })
    ]);
    const policies = await Promise.all([
        readFile(getUrl("policies"), { encoding: 'utf8' })
    ]);
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

