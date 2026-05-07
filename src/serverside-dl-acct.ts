/*
    RZO - A Business Application Framework

    Copyright (C) 2025 Frank Vanderham

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
         Row, Logger, ServiceSource, BizTrans, JsonObject
} from "./base/core.js";
import { DocumentBuilder, FinDoc } from "./accting/acc-core.js";
import { RZO } from "./base/configuration.js";


type DocumentRaw = {
    document: JsonObject;
    splits: JsonObject[];
}

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

try {
    if (argv.length != 4) {
        throw new Error(
            "Usage: node serverside-dl-acct.js <creds-file> <data-file>");
    }
    const configuration = await Promise.all([
        readFile(getUrl("entities"), { encoding: 'utf8' }),
        readFile(getUrl("accting"), { encoding: 'utf8' }),
        readFile(getUrl("entities-server", "serverside-client"),
                 { encoding: 'utf8' }),
        readFile(getUrl("personas"), { encoding: 'utf8' }),
        readFile(getUrl("collections", "client"), { encoding: 'utf8' }),
        readFile(getUrl("accting-collections", "serverside-client"),
                 { encoding: 'utf8' }),
        readFile(getUrl("config", "serverside-client"), { encoding: 'utf8' })
    ]);
    const credsFile = await readFile(
        new URL(argv[2], import.meta.url), { encoding: 'utf8' });
    const credsRow = new Row(JSON.parse(credsFile));
    const fileData = await readFile(
        new URL(argv[3], import.meta.url), { encoding: 'utf8' });
    const loadData = JSON.parse(fileData) as DocumentRaw[];
    await RZO.load(configuration);
    const logger = new Logger("client");
    logger.configure(RZO);
    await RZO.startAsyncTasks();
    try {
        const source = RZO.getSource("db");
        const service = (<ServiceSource>source.ensure(ServiceSource)).service;
        const authenticator = RZO.getAuthenticator("auth").service;
        // const accountEntity = RZO.getEntity("account");
        const findocEntity = RZO.getEntity("findoc") as FinDoc;
        const context = await authenticator.login(logger, credsRow);
        logger.log(`Session: ${JSON.stringify(context)}`);
        try {

            const builder = new DocumentBuilder(
                findocEntity, new BizTrans(), context, service);
            for (const inputDoc of loadData) {
                if (!inputDoc) {
                    break;
                }
                const bt = new BizTrans();
                builder.reset(bt);
                await builder.document(new Row(inputDoc.document));
                for (const inputSplit of inputDoc.splits) {
                    await builder.split(new Row(inputSplit));
                }
                await builder.postBizTrans();
                await service.processBizTrans(logger, bt);
            }
        } finally {
            await authenticator.logout(logger, context);
        }
    } finally {
        await RZO.stopAsyncTasks();
    }

} catch (err) {
    console.log("(Main) Caught error");
    console.error(err);
}

