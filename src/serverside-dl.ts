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

import {
    State, KeyValue, IService, IContext, Entity, SideEffects, Row, Logger
} from "./base/core.js";

import { RZO } from "./base/configuration.js";


type EntityLoad = {
    entity: string;
    operation?: string;
    id?: string;
    version?: string;
    values: KeyValue[];
}

type Ops = "post" | "put" | "delete";

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

async function loadEntityState(service: IService, context: IContext,
                               entityCfg: EntityLoad,
                               entity: Entity): Promise<State> {
    let state: State;
    if (entityCfg.id && entityCfg.version) {
        state = await entity.load(
            service, context, entityCfg.id, entityCfg.version);
    } else if (entityCfg.id) {
        state = await entity.load(service, context, entityCfg.id);
    } else {
        state = await entity.queryOne(service, entityCfg.values, context);
    }
    return state;
}

try {
    if (argv.length != 4) {
        throw new Error(
            "Usage: node serverside-dl.js <creds-file> <data-file>");
    }

    const configuration = await Promise.all([
        readFile(getUrl("entities"), { encoding: 'utf8' }),
        readFile(getUrl("personas"), { encoding: 'utf8' }),
        readFile(getUrl("collections", "client"), { encoding: 'utf8' }),
        readFile(getUrl("config", "serverside-client"), { encoding: 'utf8' })
    ]);

    const credsFile = await readFile(
        new URL(argv[2], import.meta.url), { encoding: 'utf8' });
    const credsRow = new Row(JSON.parse(credsFile));

    const fileData = await readFile(
        new URL(argv[3], import.meta.url), { encoding: 'utf8' });
    const loadData = JSON.parse(fileData) as EntityLoad[];

    await RZO.load(configuration);

    const logger = new Logger("client");
    logger.configure(RZO);

    await RZO.startAsyncTasks();

    try {
        const source = RZO.getSource("db");
        const service = source.service;
        const authenticator = RZO.getAuthenticator("auth").service;

        if (!service) {
            throw new Error("Invalid source");
        }
        const context = await authenticator.login(logger, credsRow);
        logger.log(`Session: ${JSON.stringify(context)}`);

        for (const entityCfg of loadData) {
            if (!entityCfg) {
                break;
            }
            let operation: Ops;
            if (entityCfg.operation) {
                switch (entityCfg.operation) {
                    case "post":
                        operation = "post";
                        break;
                    case "put":
                        operation = "put";
                        break;
                    case "delete":
                        operation = "delete";
                        break;
                    default:
                        throw new Error(`Invalid operation: ` +
                                        `${entityCfg.operation}`);
                }
            } else {
                operation = "post";
            }
            const entity = RZO.getEntity(entityCfg.entity);
            if (operation == "post") {
                const state = await entity.create(context, service);
                const validations: Promise<SideEffects>[] = [];
                for (const fieldCfg of entityCfg.values) {
                    validations.push(
                        entity.setValue(
                            state, fieldCfg.k, fieldCfg.v, context));
                }
                await Promise.all(validations);
                await entity.post(service, state, context);
            } else if (operation == "put") {
                const state = await loadEntityState(
                    service, context, entityCfg, entity);
                const validations: Promise<SideEffects>[] = [];
                for (const fieldCfg of entityCfg.values) {
                    if (entityCfg.id || !entity.keyFields.has(fieldCfg.k)) {
                        validations.push(entity.setValue(state, fieldCfg.k,
                                                         fieldCfg.v, context));
                    }
                }
                await Promise.all(validations);
                await entity.put(service, state, context);
            } else if (operation == "delete") {
                if (!entityCfg.id || !entityCfg.version) {
                    throw new Error("Missing 'id' and/or 'version' attribute");
                }
                const state = await loadEntityState(
                    service, context, entityCfg, entity);
                await entity.delete(service, state, context);
            }
        }
        await authenticator.logout(logger, context);
    } finally {
        await RZO.stopAsyncTasks();
    }

} catch (err) {
    console.log("(Main) Caught error");
    console.error(err);
}

