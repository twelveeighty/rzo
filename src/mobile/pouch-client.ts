/*
    RZO - A Business Application Framework

    Copyright (C) 2026 Frank Vanderham

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

import {
    Entity, IService, IResultSet, Query, Filter, Collection, IContext, Row,
    TypeCfg, ClassSpec, ServiceSource, IConfiguration, Logger, BizTrans,
    EmptyResultSet, MemResultSet
} from "../base/core.js";

import { MobileCollection } from "./pouch-collection.js";


class PouchClientError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }
}

export interface IReplicableService {
    get isReplicableService(): boolean;
    replicate(context: IContext, entity: Entity): Promise<void>;
}

type MangoSort = { [name: string]: "asc" | "desc" };

export type PouchClientSpec = ClassSpec & {
    replicateUrl: string;
};

export class PouchClient implements IService, IReplicableService {
    readonly name: string;
    dbs: Map<string, PouchDB.Database>;
    collections: Set<string>;
    pouchLogger: Logger;
    replicateUrl: string;

    constructor(config: TypeCfg<PouchClientSpec>) {
        this.name = config.metadata.name;
        this.replicateUrl = config.spec.replicateUrl;
        this.dbs = new Map();
        this.collections = new Set();
        this.pouchLogger = new Logger(`pouch/${this.name}`);
    }

    configure(configuration: IConfiguration): void {
    }

    private async initCollection(
            collection: Collection): Promise<MobileCollection> {
        if (!(collection instanceof MobileCollection)) {
            throw new PouchClientError(
                `Cannot use non mobile collection ${collection.name} for ` +
                `PouchDB`);
        }
        if (!this.collections.has(collection.name)) {
            const dbName = collection.entity.v.table;
            if (!collection.usesKeyIndex) {
                // Create an index for the 'filteredBy' of the collection.
                const db = await this.getDatabase(collection.entity.v);
                const fields = Array.from(collection.filteredBy);
                this.pouchLogger.info(
                    `Loading Pouch Index: ${dbName}-${collection.name} ` +
                    `with fields: [${fields.join()}]`);
                await db.createIndex({
                    index: {
                        fields: fields,
                        ddoc: collection.name
                    }
                });
            } else {
                this.pouchLogger.info(
                    `Collection ${collection.name} matches ${dbName}'s key ` +
                    `index, no additional index will be created.`);
            }
            this.collections.add(collection.name);
        }
        return collection;
    }

    private async getDatabase(entity: Entity): Promise<PouchDB.Database> {
        const dbName = entity.table;
        let db: PouchDB.Database | undefined = this.dbs.get(dbName);
        if (db) {
            return db;
        }
        this.pouchLogger.info(`Loading PouchDB: ${dbName}`);
        db = new PouchDB(dbName);
        this.dbs.set(dbName, db);
        // For each database create a natural key index for its entity
        const fields: string[] =
            Array.from(entity.keyFields.keys());
        if (fields.length) {
            this.pouchLogger.info(
                `Loading Pouch Index: ${dbName}-key with fields: ` +
                `[${fields.join()}]`);
            await db.createIndex({
                index: {
                    fields: fields,
                    ddoc: `${dbName}-key`
                }
            });
        }
        return db;
    }

    async getDBInfo(logger: Logger, context: IContext): Promise<Row> {
        throw new PouchClientError("Not implemented");
    }

    async getQueryOne(logger: Logger, context: IContext, entity: Entity,
                      filter: Filter): Promise<Row> {
        throw new PouchClientError("Not implemented");
    }

    async getOne(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev?: string): Promise<Row> {
        const db = await this.getDatabase(entity);
        const doc = await db.get(id);
        return Row.dataToRow(doc, entity);
    }

    private async find(logger: Logger, entity: Entity, query: Query,
                       filter: Filter, ddoc?: string): Promise<IResultSet> {
        const db = await this.getDatabase(entity);
        const selector = filter.selector;
        const sort = this.toSort(query);
        const findObj: PouchDB.Find.FindRequest<{}> = {
            selector: selector.raw()
        };
        if (sort.length > 0) {
            findObj["sort"] = sort;
        }
        if (ddoc) {
            findObj["use_index"] = ddoc;
        }
        this.log(logger, "find", entity, new Row(findObj));
        const result = await db.find(findObj);
        if (!result.docs || !Array.isArray(result.docs) ||
            !result.docs.length) {
            return new EmptyResultSet();
        } else {
            const rs = new MemResultSet();
            for (const raw of result.docs) {
                rs.addRow(Row.dataToRow(raw, entity));
            }
            return rs;
        }
    }

    async queryCollection(logger: Logger, context: IContext,
                          collection: Collection,
                          query?: Query): Promise<IResultSet> {
        const coll = await this.initCollection(collection);
        const entity = collection.entity.v;
        const filter = this.addOrderByToQueryFilter(query);
        const finalQuery = query || new Query(["*"], filter);
        if (filter.notEmpty) {
            const ddoc = coll.usesKeyIndex ?
                `${entity.table}-key` :
                collection.name;
            return this.find(logger, entity, finalQuery, filter, ddoc);
        } else {
            // Can't use find(), since there's no selector
            return this.getQuery(logger, context, entity, finalQuery);
        }
    }

    toSort(query: Query): MangoSort[] {
        const result: MangoSort[] = [];
        for (const orderBy of query.orderBy) {
            const sort: MangoSort = { [orderBy.field]: orderBy.order };
            result.push(sort);
        }
        return result;
    }

    private addOrderByToQueryFilter(query?: Query): Filter {
        /* Ensure that all orderBy fields are part of the selector. Add missing
         * orderBy fields as a "<field> is not null" filter.
         * TODO: support orderBy fields that can be null by adding an '$exists'
         * operator.
        */
        const filter = query?.filter || new Filter();
        if (query?.orderBy.length) {
            if (!query.filter) {
                query.filter = filter;
            }
            const selectorFields: Set<string> = filter.notEmpty ?
                filter.selectorFields : new Set();
            for (const orderBy of query.orderBy) {
                if (!selectorFields.has(orderBy.field)) {
                    filter.fieldExists(orderBy.field);
                    selectorFields.add(orderBy.field);
                }
            }
        }
        return filter;
    }

    async getQuery(logger: Logger, context: IContext, entity: Entity,
                   query: Query): Promise<IResultSet> {
        const filter = this.addOrderByToQueryFilter(query);
        const db = await this.getDatabase(entity);
        if (filter.notEmpty) {
            /* Attempt to auto-match the selector to an index.
             * Note that this is pretty much impossible to work at this moment,
             * but we'll keep this code for future expansion.
            */
            return this.find(logger, entity, query, filter);
        } else {
            // Use the allDocs() view
            this.log(logger, "allDocs", entity);
            const result = await db.allDocs({ include_docs: true });
            const resultRow = new Row(result);
            if (resultRow.get("total_rows") > 0) {
                const rs = new MemResultSet();
                for (const rowRaw of result.rows) {
                    if (rowRaw.id[0] != "_") {
                        const resultRow = new Row(rowRaw.doc);
                        rs.addRow(resultRow);
                    }
                }
                return rs;
            } else {
                return new EmptyResultSet();
            }
        }
    }

    async getGeneratorNext(logger: Logger, context: IContext,
                           generatorName: string): Promise<string> {
        throw new PouchClientError("Not implemented");
    }

    private log(logger: Logger, operation: string, entity: Entity,
                details?: Row): void {
        /* If the service logger's threshold logs more than the passed
         * logger, use the service logger instead.
         */
        const finalLogger = this.pouchLogger.threshold > logger.threshold ?
            this.pouchLogger :
            logger;
        finalLogger.debug(`${operation} on ${entity.table}`);
        if (details) {
            finalLogger.debugAny(details.raw());
        }
    }

    private selectorForDupes(entity: Entity, row: Row, id?: string): Row {
        const result = new Row();
        if (entity.keyFields.size) {
            for (const key of entity.keyFields.keys()) {
                result.add(key, row.get(key));
            }
            if (id) {
                result.add("_id", { $ne: id });
            }
        }
        return result;
    }

    async put(logger: Logger, context: IContext, entity: Entity, id: string,
              row: Row): Promise<Row> {
        if (!entity.canUpdate) {
            throw new PouchClientError(
                `Entity ${entity.name} is ${entity.species}, cannot update ` +
                `it this way`);
        }
        const db = await this.getDatabase(entity);
        // Check duplicate by key
        const selector = this.selectorForDupes(entity, row, row.get("_id"));
        if (!selector.empty) {
            this.log(logger, "find", entity, selector);
            const result = await db.find({
                selector: selector.raw(),
                use_index: `${entity.table}-key`
            });
            if (result.docs.length > 0) {
                throw new PouchClientError(
                    `Duplicate '${entity.name}': ${selector}`);
            }
        }
        row.updateOrAdd("updated", new Date());
        row.updateOrAdd("updatedby", context.userAccountId);
        this.log(logger, "put", entity, row);
        const result = await db.put(row.raw());
        const resultRow = new Row(result);
        if (resultRow.has("ok") && resultRow.get("ok")) {
            row.put("_rev", resultRow.get("rev"));
            return row;
        } else {
            throw new PouchClientError(
                `Cannot PUT entity ${entity.table}, ` +
                `error = ${result}`);
        }
    }

    async post(logger: Logger, context: IContext, entity: Entity,
               row: Row): Promise<Row> {
        const db = await this.getDatabase(entity);
        // Check duplicate by key
        const selector = this.selectorForDupes(entity, row);
        if (!selector.empty) {
            this.log(logger, "find", entity, selector);
            const result = await db.find({
                selector: selector.raw(),
                use_index: `${entity.table}-key`
            });
            if (result.docs.length > 0) {
                throw new PouchClientError(
                    `Duplicate '${entity.name}': ${selector}`);
            }
        }
        row.updateOrAdd("updated", new Date());
        row.updateOrAdd("updatedby", context.userAccountId);
        this.log(logger, "post", entity, row);
        const result = await db.post(row.raw());
        const resultRow = new Row(result);
        if (resultRow.has("ok") && resultRow.get("ok")) {
            row.add("_id", resultRow.get("id"));
            row.add("_rev", resultRow.get("rev"));
            return row;
        } else {
            throw new PouchClientError(
                `Cannot POST entity ${entity.table}, ` +
                `error = ${result}`);
        }
    }

    async delete(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev?: string): Promise<void> {
        throw new PouchClientError("Not implemented");
    }

    async processBizTrans(logger: Logger,
                          bizTrans: BizTrans): Promise<BizTrans> {
        throw new PouchClientError("Not implemented");
    }

    get isReplicableService(): boolean {
        return true;
    }

    async replicate(context: IContext, entity: Entity): Promise<void> {
        if (!this.replicateUrl) {
            throw new PouchClientError(
                `Pouch Client ${this.name} has no replicateUrl defined`);
        }
        const sourceDb = await this.getDatabase(entity);
        let baseUrl = this.replicateUrl;
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.slice(0, baseUrl.length-1);
        }
        const url = `${baseUrl}/${entity.name}`;
        this.pouchLogger.info(`Connecting to remote DB: ${url}`);
        const remoteDb = new PouchDB(url, {
            fetch: function (url, opts: any) {
                opts.credentials = "omit";
                opts.headers.set("rzo-sessionid", context.sessionId);
                return PouchDB.fetch(url, opts);
            }
        });
        this.pouchLogger.info(`Replicating to: ${url}`);
        const logger = this.pouchLogger;
        await sourceDb.sync(remoteDb, {
            filter: function(doc: any) {
                return doc._id[0] != "_";
            }
        })
        .on("change", function (info) {
          logger.info("'change' event fired");
          logger.logAny(info, 99);
        })
        .on("paused", function (err) {
        // replication paused (e.g. replication up to date, user went offline)
          logger.error("'paused' event fired");
          logger.logAny(err, 9);
        })
        .on("active", function () {
          // replicate resumed (e.g. new changes replicating, user went back online)
          logger.error("'active' event fired");
        })
        .on("denied", function (err) {
          // a document failed to replicate (e.g. due to permissions)
          logger.error("'denied' event fired");
          logger.logAny(err, 9);
        })
        .on("complete", function (info) {
          // handle complete
          logger.info("'complete' event fired");
          logger.logAny(info, 99);
        })
        .on("error", function (err) {
          // handle error
          logger.error("'error' event fired");
          logger.logAny(err, 9);
        });
        this.pouchLogger.info(`Replication COMPLETED to: ${url}`);
    }

}

export class PouchClientSource extends ServiceSource {
    _service: PouchClient;

    constructor(config: TypeCfg<PouchClientSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new PouchClient(config);
    }

    configure(configuration: IConfiguration) {
        this._service.configure(configuration);
    }

    get service(): IService {
        return this._service;
    }
}

