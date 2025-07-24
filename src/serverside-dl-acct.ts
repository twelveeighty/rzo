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
    IService, IContext, Entity, Row, Logger, ServiceSource, Filter, State,
    MemResultSet, Nobody
} from "./base/core.js";

import { TxnRaw, Txn, AcctingServiceSource } from "./accting/acc-core.js";

import { RZO } from "./base/configuration.js";


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

async function loadAccount(logger: Logger, service: IService, context: IContext,
                           cache: Map<string,Row>, accountEntity: Entity,
                           name: string): Promise<Row> {
    let result = cache.get(name);
    if (result) {
        return result;
    }
    const filter = new Filter().op("name", "=", name);
    result = await service.getQueryOne(logger, context, accountEntity, filter);
    if (result.empty) {
        throw new Error(`Cannot find account: ${name}`);
    }
    cache.set(name, result);
    return result;
}

async function setTransAccount(logger: Logger, service: IService,
                               context: IContext, cache: Map<string,Row>,
                               accountEntity: Entity, row: Row,
                               state: State): Promise<void> {
    const account = await loadAccount(
        logger, service, context, cache, accountEntity, row.get("account"));
    state.field("account").value = account.get("name");
    state.field("account_id").value = account.get("_id");
}

async function setSplitAccount(logger: Logger, service: IService,
                               context: IContext, cache: Map<string,Row>,
                               accountEntity: Entity, row: Row,
                               state: State): Promise<void> {
    const account = await loadAccount(
        logger, service, context, cache, accountEntity, row.get("account"));
    state.field("account").value = account.get("name");
    state.field("account_id").value = account.get("_id");
    state.field("holding").value = account.get("holding");
    state.field("holding_id").value = account.get("holding_id");
    state.field("elementtype").value = account.get("elementtype");
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
        readFile(getUrl("accting-collections", "server"), { encoding: 'utf8' }),
        readFile(getUrl("config", "serverside-client"), { encoding: 'utf8' })
    ]);

    const credsFile = await readFile(
        new URL(argv[2], import.meta.url), { encoding: 'utf8' });
    const credsRow = new Row(JSON.parse(credsFile));

    const fileData = await readFile(
        new URL(argv[3], import.meta.url), { encoding: 'utf8' });
    const loadData = JSON.parse(fileData) as TxnRaw[];

    await RZO.load(configuration);

    const logger = new Logger("client");
    logger.configure(RZO);

    await RZO.startAsyncTasks();

    try {
        const source = RZO.getSource("db");
        const service = (<ServiceSource>source.ensure(ServiceSource)).service;
        const txnsource = RZO.getSource("txndb");
        const txnservice = (<AcctingServiceSource>txnsource.ensure(
                AcctingServiceSource)).service;
        const authenticator = RZO.getAuthenticator("auth").service;
        const accountEntity = RZO.getEntity("account");
        const transEntity = RZO.getEntity("acctrans");
        const splitEntity = RZO.getEntity("accsplit");
        const context = await authenticator.login(logger, credsRow);
        logger.log(`Session: ${JSON.stringify(context)}`);
        try {
            const accountCache: Map<string,Row> = new Map();
            for (const txnRaw of loadData) {
                const now = new Date();
                if (!txnRaw) {
                    break;
                }
                // const txn = rawToTxnUnparsed(txnRaw);
                const transState = await transEntity.create(context, service);
                const transRow = Row.dataToRow(txnRaw.transaction);
                for (const column of transRow.columns) {
                    if (column == "account") {
                        await setTransAccount(
                            logger, service, context, accountCache,
                            accountEntity, transRow, transState);
                    } else {
                        await transEntity.setValue(
                            transState, column, transRow.get(column), context);
                    }
                }
                await transEntity.setValue(transState, "created", now, context);
                const posted = transState.field("posted").value;
                const acctrans = transState.field("transnum").value;
                const memo = transState.field("memo").value;
                const acctrans_id = Nobody.ID;
                const splits = new MemResultSet();
                const txn: Txn = {
                    transaction: transEntity.stateToRow(transState),
                    splits: splits
                };
                for (const splitObj of txnRaw.splits) {
                    const splitRow = Row.dataToRow(splitObj);
                    const splitState = await splitEntity.create(
                        context, service);
                    splitState.field("acctrans").value = acctrans;
                    splitState.field("acctrans_id").value = acctrans_id;
                    for (const column of splitRow.columns) {
                        if (column == "account") {
                            await setSplitAccount(
                                logger, service, context, accountCache,
                                accountEntity, splitRow, splitState);
                        } else {
                            await splitEntity.setValue(
                                splitState, column, splitRow.get(column),
                                context);
                        }
                    }
                    const memoFieldState = splitState.field("memo");
                    if (memoFieldState.isNull) {
                        memoFieldState.value = memo;
                    }
                    await splitEntity.setValue(
                        splitState, "posted", posted, context);
                    await splitEntity.setValue(
                        splitState, "created", now, context);
                    splits.addRow(splitEntity.stateToRow(splitState));
                }
                await txnservice.postTxn(logger, context, txn);
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

