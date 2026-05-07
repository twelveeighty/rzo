/*
    RZO - A Business Application Framework

    Copyright (C) 2024-2025 Frank Vanderham

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
import Pool from "pg-pool";
import Cursor from "pg-cursor";

import { env } from "node:process";

import {
    Entity, IResultSet, IConfiguration, Query, DaemonWorker, EmptyResultSet,
    MemResultSet, Row, TypeCfg, ClassSpec, Collection, IContext, Filter,
    ServiceSource, _IError, Nobody, Persona, Cfg, IService, SideEffects,
    State, Logger, Epilogue, BizTrans, IReadOnlyService
} from "../base/core.js";

import { VERSION, NOCONTEXT } from "../base/configuration.js";

import {
    ISessionBackendService, SessionContext, serializeSubjectMap
} from "../base/session.js";

import { MvccResult, MvccController } from "./mvcc.js";
import { IElectorService } from "./election.js";
import { IHooverService } from "./hoover.js";

class PgClientError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

type PgConnectionSpec = ClassSpec & {
    connectionTimeoutMillis: number;
    idleTimeoutMillis: number;
    max: number;
    allowExitOnIdle: boolean;
    pageSize: number;
}

export class PgConnection extends DaemonWorker {
    private _conn_pool: Pool<pg.Client> | null;
    pageSize: number;

    constructor(config: TypeCfg<PgConnectionSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        if (config.spec.pageSize <= 0) {
            throw new PgClientError(
                `Invalid pageSize: ${config.spec.pageSize}`, 400);
        }
        this._conn_pool = new Pool({
            connectionTimeoutMillis: config.spec.connectionTimeoutMillis,
            idleTimeoutMillis: config.spec.idleTimeoutMillis,
            max: config.spec.max,
            allowExitOnIdle: config.spec.allowExitOnIdle
        });
        this.pageSize = config.spec.pageSize;
    }

    configure(configuration: IConfiguration): void {
        configuration.registerAsyncTask(this);
    }

    async stop(): Promise<any> {
        if (this._conn_pool) {
            console.log("Ending connection pool...");
            await this._conn_pool.end();
            console.log("Connection pool ended");
            this._conn_pool = null;
        }
    }

    get pool(): Pool<pg.Client> {
        if (this._conn_pool) {
            return this._conn_pool;
        } else {
            throw new PgClientError("Pool was closed");
        }
    }
}

export class PgBaseClient {
    private _conn_pool?: Pool<pg.Client>;
    sessionEntity: Cfg<Entity>;
    userEntity: Cfg<Entity>;
    conn: Cfg<PgConnection>;
    protected mvccController: MvccController;
    protected mvccLogger: Logger;

    static VC_COLS = [
        "vc.seq", "vc._id as vc_id", "vc._rev as vc_rev",
        "vc.updated", "vc.updatedby", "vc.versiondepth", "vc.ancestry",
        "vc.isleaf", "vc.isdeleted", "vc.isstub, vc.isconflict, vc.iswinner"
    ];

    static VC_COL_SELECT = PgBaseClient.VC_COLS.join(",");

    static PREVENT_EPILOGUE = true;
    static PROCESS_EPILOGUE = false;

    constructor(pool: string) {
        this.sessionEntity = new Cfg("session");
        this.userEntity = new Cfg("useraccount");
        this.conn = new Cfg(pool);
        this.mvccLogger = new Logger("server/mvcc");
        this.mvccController = new MvccController(this.mvccLogger);
    }

    configure(configuration: IConfiguration) {
        this.sessionEntity.v = configuration.getEntity("session");
        this.userEntity.v = configuration.getEntity("useraccount");
        const conn: unknown = configuration.workers.get(this.conn.name);
        if (conn instanceof PgConnection) {
            this.conn.v = conn as PgConnection;
        } else {
            throw new PgClientError(
                `Worker ${this.conn.name} is not a PgConnection`);
        }
        this.mvccLogger.configure(configuration);
    }

    get pool(): Pool<pg.Client> {
        if (this._conn_pool) {
            return this._conn_pool;
        } else {
            this._conn_pool = this.conn.v.pool;
            return this._conn_pool;
        }
    }

    protected convertDbRowToAppRow(row: Row, flagConflict?: boolean): Row {
        if (!row.has("_id")) {
            row.add("_id", row.get("vc_id"));
        } else if (!row.get("_id")) {
            row.put("_id", row.get("vc_id"));
        }
        if (!row.has("_rev")) {
            row.add("_rev", row.get("vc_rev"));
        } else if (!row.get("_rev")) {
            row.put("_rev", row.get("vc_rev"));
        }
        if (!row.has("att_")) {
            row.add("att_", null);
        }
        if (row.get("isdeleted")) {
            row.add("_deleted", true);
            const keep = ["_id", "_rev", "_deleted", "_revisions"];
            for (const colName of row.columns) {
                if (!keep.includes(colName)) {
                    row.deleteNoCheck(colName);
                }
            }
        } else {
            if (flagConflict && row.get("isconflict")) {
                row.add("_conflict", true);
            } else if (!row.get("isleaf")) {
                row.add("_notleaf", true);
            }
            row.deleteNoCheck("seq");
            row.deleteNoCheck("vc_id");
            row.deleteNoCheck("vc_rev");
            row.deleteNoCheck("versiondepth");
            row.deleteNoCheck("ancestry");
            row.deleteNoCheck("isleaf");
            row.deleteNoCheck("isdeleted");
            row.deleteNoCheck("isstub");
            row.deleteNoCheck("isconflict");
            row.deleteNoCheck("iswinner");
        }
        return row;
    }

    async getSequenceId(logger: Logger, context: IContext,
                        entity: Entity): Promise<string> {
        const statement =
            `select coalesce(max(updateseq), 0) "max" from ${entity.table}_vc`;
        this.log(logger, statement);
        const result = await this.pool.query(statement);
        if (result.rows.length === 0) {
            throw new PgClientError(`No rows returned for max(updateseq) ` +
                                    `${entity.table}_vc`, 404);
        }
        return "" + result.rows[0].max;
    }

    private async getQueryOneUnversioned(logger: Logger, entity: Entity,
                                         filter: Filter): Promise<Row> {
        const statement =
            `select * from ${entity.table} where (${filter.where}) limit 1`;
        this.log(logger, statement);
        const result = await this.pool.query(statement);
        if (result.rows.length === 0) {
            return new Row();
        }
        return Row.dataToRow(result.rows[0], entity);
    }

    protected async getQueryOneVersioned(logger: Logger, entity: Entity,
                                         filter: Filter,
                                         client?: pg.Client): Promise<Row> {
        const statement =
            `select ${PgBaseClient.VC_COL_SELECT}, e.* ` +
            `from ${entity.table} as e ` +
            `inner join ${entity.table}_vc as vc on (vc._id = e._id and ` +
            `vc._rev = e._rev) ` +
            `where (${filter.where}) limit 1`;
        this.log(logger, statement);
        const result = client !== undefined ? await client.query(statement) :
            await this.pool.query(statement);
        if (result.rows.length === 0) {
            return new Row();
        }
        return this.convertDbRowToAppRow(
            Row.dataToRow(result.rows[0], entity), true);
    }

    async getQueryOne(logger: Logger, context: IContext, entity: Entity,
                      filter: Filter): Promise<Row> {
        if (!entity.versioned) {
            return this.getQueryOneUnversioned(logger, entity, filter);
        }
        return this.getQueryOneVersioned(logger, entity, filter);
    }

    private async getOneUnversioned(logger: Logger, context: IContext,
                                    entity: Entity, id: string): Promise<Row> {
        const statement = `select * from ${entity.table} where _id = \$1`;
        const parameters = [id];
        this.log(logger, statement, parameters);
        const result = await this.pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return new Row();
        }
        return Row.dataToRow(result.rows[0], entity);
    }

    async getOne(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev?: string): Promise<Row> {
        if (!entity.versioned) {
            return await this.getOneUnversioned(logger, context, entity, id);
        }
        let row: Row = new Row();
        if (rev) {
            const statement =
                `select ${PgBaseClient.VC_COL_SELECT}, v.* ` +
                `from ${entity.table}_vc as vc ` +
                `left join ${entity.table}_v as v on ` +
                `(vc._id = v._id and vc._rev = v._rev) ` +
                `where vc._id = \$1 and vc._rev = \$2`;
            const parameters = [id, rev];
            this.log(logger, statement, parameters);
            const result = await this.pool.query(statement, parameters);
            if (result.rows.length > 0) {
                row = this.convertDbRowToAppRow(
                    Row.dataToRow(result.rows[0], entity), true);
            }
        } else {
            const statement =
                `select ${PgBaseClient.VC_COL_SELECT}, e.* ` +
                `from ${entity.table} as e ` +
                `inner join ${entity.table}_vc as vc on (vc._id = e._id and ` +
                `vc._rev = e._rev) ` +
                `where e._id = \$1`;
            const parameters = [id];
            this.log(logger, statement, parameters);
            const result = await this.pool.query(statement, parameters);
            if (result.rows.length > 0) {
                row = this.convertDbRowToAppRow(
                    Row.dataToRow(result.rows[0], entity), true);
            }
        }
        return row;
    }

    protected async postEntity(logger: Logger, userId: string,
                               client: pg.Client, entity: Entity, row: Row,
                               context?: IContext,
                               preventEpilogue?: boolean,
                               roService?: IReadOnlyService): Promise<Row> {
        if (entity.versioned) {
            const mvccResult = this.mvccController.postMvcc(row, userId);
            await this.applyMvccResults(logger, client, entity, mvccResult);
            return Row.must(mvccResult.leafTable.leafActionPost?.payload);
        } else {
            const id = Entity.generateId(row);
            // We must calculate _rev without the _id column present
            this.mvccController.convertToPayload(row);
            const rev = entity.immutable ?
                `1-${this.mvccController.newVersion(row).hash}` :
                "";
            row.add("_id", id);
            if (rev) {
                row.add("_rev", rev);
            }
            row.add("updated", new Date());
            row.add("updatedby", userId);
            const epilogue = !preventEpilogue && entity.hasEpilogue(row) ?
                entity.epilogue(row, roService, context) : null;
            const statement = `insert into ${entity.table} ` +
                `(${row.columns.join()}) ` +
                `values (` +
                `${row.columnNumbers.join()}` +
                `)`;
            const parameters = row.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
            if (epilogue) {
                await this.handleEpilogue(logger, client, epilogue, context);
            }
            return row;
        }
    }

    async getDBInfo(logger: Logger, context: IContext): Promise<Row> {
        const info = {
            uuid: "00000000000000000000000000000000",
            version: VERSION
        };
        const statement = "select db_uuid()";
        this.log(logger, statement);
        const result = await this.pool.query(statement);
        if (result.rows.length > 0) {
            info.uuid = result.rows[0].db_uuid;
        }
        return new Row(info);
    }

    private getSelfUpdateSet(entity: Entity, alias: string): string[] {
        const statements: string[] = [];
        for (const column of entity.allFieldColumns) {
            statements.push(`${column} = ${alias}.${column}`);
        }
        return statements;
    }

    protected async applyMvccResults(logger: Logger, client: pg.Client,
                                     entity: Entity,
                                     result: MvccResult): Promise<void> {
        const now = new Date();
        const putStatement = `update ${entity.table}_vc set ` +
            `updateseq = nextval('${entity.table}_vc_useq'), ` +
            `isleaf = \$1, isdeleted = \$2, isstub = \$3, isconflict = \$4, ` +
            `iswinner = \$5 where seq = \$6`;
        const postStatement = `insert into ${entity.table}_vc (` +
            `_id, _rev, updated, updatedby, versiondepth, ancestry, isleaf, ` +
            `isdeleted, isstub, isconflict, iswinner, updateseq) values (` +
            `\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9, \$10, \$11, ` +
            `nextval('${entity.table}_vc_useq'))`;
        for (const vc of result.vcTables) {
            const record = vc.record;
            if (vc.action == "put") {
                const eventualStub = (!record.isleaf && !record.isdeleted)
                    || record.isstub;
                const isStub = eventualStub &&
                    (entity.retention.style == "delete");
                const parameters = [
                    record.isleaf, record.isdeleted, isStub,
                    record.isconflict, record.iswinner, record.seq!
                ];
                this.log(logger, putStatement, parameters);
                await client.query(putStatement, parameters);
                if (entity.retention.style == "temporary" &&
                    (eventualStub || record.isdeleted)) {
                    const statement =
                        `insert into ${entity.table}_cy (` +
                        `_id, _rev, updated) values (` +
                        `\$1, \$2, \$3)`;
                    const parameters = [record._id, record._rev, now];
                    this.log(logger, statement, parameters);
                    await client.query(statement, parameters);
                } else if (entity.retention.style == "delete" && eventualStub) {
                    const statement =
                        `delete from ${entity.table}_v where ` +
                        `_id = \$1 and _rev = \$2`;
                    const parameters = [record._id, record._rev];
                    this.log(logger, statement, parameters);
                    await client.query(statement, parameters);
                }
            } else if (vc.action == "post") {
                const parameters = [
                    record._id!, record._rev!, record.updated!,
                    record.updatedby!, record.versiondepth!, record.ancestry!,
                    record.isleaf, record.isdeleted, record.isstub,
                    record.isconflict, record.iswinner
                ];
                this.log(logger, postStatement, parameters);
                await client.query(postStatement, parameters);
            }
        }
        // Version table
        if (result.versionTable.type == "post") {
            const row = result.versionTable.versionActionPost!.payload;
            const statement = `insert into ${entity.table}_v ` +
                `(${row.columns.join()}) ` +
                `values (${row.columnNumbers.join()})`;
            const parameters = row.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        } else if (result.versionTable.type == "delcopy" &&
                   entity.retention.style != "delete") {
            const id = result.versionTable.versionActionDelcopy!._id;
            const fromRev = result.versionTable.versionActionDelcopy!.fromRev;
            const toRev = result.versionTable.versionActionDelcopy!.toRev;
            const fieldCols = entity.allFieldColumns.join(", ");
            const statement =
                `insert into ${entity.table}_v ` +
                `(_id, _rev, att_, ${fieldCols}) ` +
                `select _id, '${toRev}', att_, ${fieldCols} ` +
                `from ${entity.table}_v ` +
                `where _id = \$1 and _rev = \$2`;
            const parameters = [id, fromRev];
            this.log(logger, statement, parameters);
            const pgResult = await client.query(statement, parameters);
            if (pgResult.rowCount != 1) {
                throw new PgClientError(
                    `MVCC _v delcopy: insert ${entity.table}_v, id = ${id} ` +
                    `from = ${fromRev} to = ${toRev} rowCount was not 1: ` +
                    `${pgResult.rowCount}`);
            }
        }
        // Entity table
        if (result.leafTable.type == "post") {
            const row = result.leafTable.leafActionPost!.payload;
            const statement = `insert into ${entity.table} ` +
                `(${row.columns.join()}) ` +
                `values (` +
                `${row.columnNumbers.join()}` +
                `)`;
            const parameters = row.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        } else if (result.leafTable.type == "delete") {
            const id = result.leafTable.leafActionDelete!._id;
            const statement = `delete from ${entity.table} where _id = \$1`;
            const parameters = [id];
            this.log(logger, statement, parameters);
            const pgResult = await client.query(statement, parameters);
            if (pgResult.rowCount != 1) {
                throw new PgClientError(
                    `MVCC Leaf delete: delete ${entity.name}, id = ${id} ` +
                    `rowCount was not 1: ${pgResult.rowCount}`);
            }
        } else if (result.leafTable.type == "swap") {
            const id = result.leafTable.leafActionSwap!._id;
            const toRev = result.leafTable.leafActionSwap!.toRev;
            /* Over time, the entity table diverts from the _v table, so only
             * pull the current columns using the entity definition.
             */
            const updateSet = this.getSelfUpdateSet(entity, "v");
            // Add _rev and att_ to the update list.
            updateSet.push("_rev = v._rev");
            updateSet.push("att_ = v.att_");
            const statement =
                `update ${entity.table} set ${updateSet.join(", ")} ` +
                `from ${entity.table}_v as v ` +
                `where ${entity.table}._id = v._id ` +
                `and v._id = \$1 and v._rev = \$2`;
            const parameters = [id, toRev];
            this.log(logger, statement, parameters);
            const pgResult = await client.query(statement, parameters);
            if (pgResult.rowCount != 1) {
                throw new PgClientError(
                    `MVCC Leaf swap: update/select ${entity.name}, id = ` +
                    `${id} ; rev = ${toRev} rowCount was not 1: ` +
                    `${pgResult.rowCount}`);
            }
        } else if (result.leafTable.type == "put") {
            /* The "put" operation requires the payload to contain
             * the *new* _rev and the leafAction._rev to MATCH the current _rev
             * on the entity table. If no match, an error is thrown and no
             * update is performed.
             */
            const row = result.leafTable.leafActionPut!.payload;
            const targetId = row.get("_id");
            const targetRev = result.leafTable.leafActionPut!._rev;
            // Copy row to exclude _id without changing the original row.
            const setRow = row.copyWithout(["_id"]);
            const setList = setRow.getUpdateSet();
            const statement =
                `update ${entity.table} set ${setList.join(", ")} ` +
                `where _id = \$${setList.length + 1} and ` +
                `_rev = \$${setList.length + 2}`;
            const parameters = setRow.values().concat(targetId, targetRev);
            this.log(logger, statement, parameters);
            const pgResult = await client.query(statement, parameters);
            if (pgResult.rowCount != 1) {
                throw new PgClientError(
                    `MVCC Leaf put: update ${entity.name}, id = ` +
                    `${targetId} ; rev = ${targetRev} rowCount was not 1: ` +
                    `${pgResult.rowCount}`);
            }
        }
    }

    protected async handlePutVersionedEpilogue(
                logger: Logger, client: pg.Client,
                epilogue: Epilogue, userId: string): Promise<void> {
        /* A versioned epilogue does not support any operators other than '='.
         * We first pull the target entity, which allows us to apply the
         * filter at source, if present.
         * This means that the epilogue must have '_id' as its key present.
         * If no row is found, and a filter was present, we silently do nothing,
         * however, if no filter was present, no row returned is treated as
         * an error.
         */
        if (!epilogue.key) {
            throw new PgClientError(
                "Epilogue 'put' is missing the 'key' attribute");
        }
        if (epilogue.key.keyColumn != "_id") {
            throw new PgClientError(
                "Epilogue 'put' on a versioned entity must have its _id " +
                "specified as a key");
        }
        const id = epilogue.key.keyValue;
        const hasFilter = !!epilogue.filter;
        const filter = epilogue.filter || new Filter();
        filter.op("e._id", "=", id);
        const row = await this.getQueryOneVersioned(
            logger, epilogue.entity, filter, client);
        if (!row.empty) {
            // Apply the update(s) to the row
            for (const col of epilogue.columns) {
                const operator = col.operator || "=";
                if (operator != "=") {
                    throw new PgClientError(
                        `Versioned epilogue updates must all have their ` +
                        `operators as '='`);
                }
                row.put(col.column, col.value);
            }
            const versions = await this.pullVcTable(
                logger, epilogue.entity, id, client);
            const mvccResult = this.mvccController.putMvcc(
                row, versions, false, userId);
            await this.applyMvccResults(
                logger, client, epilogue.entity, mvccResult);
        } else {
            if (!hasFilter) {
                throw new PgClientError(
                    `Epilogue entity ${epilogue.entity.name} with key ` +
                    `${id} not found`);
            }
        }
    }

    protected async handleDeleteLocalEpilogue(logger: Logger, client: pg.Client,
                                              epilogue: Epilogue)
                                                  : Promise<void> {
        const filter = epilogue.filter || new Filter();
        if (epilogue.key) {
            filter.op(epilogue.key.keyColumn, "=", epilogue.key.keyValue);
        }
        const where = filter.where;
        const statement =
            `delete from ${epilogue.entity.table} ` +
            `where ${where}`;
        this.log(logger, statement);
        await client.query(statement);
    }

    protected async handlePutLocalEpilogue(logger: Logger, client: pg.Client,
                                           epilogue: Epilogue): Promise<void> {
        if (!epilogue.key) {
            throw new PgClientError(
                "Epilogue 'put' is missing the 'key' attribute");
        }
        const filter = epilogue.filter || new Filter();
        filter.op(epilogue.key.keyColumn, "=", epilogue.key.keyValue);
        const where = filter.where;
        const columnOps: string[] = [];
        let pos = 1;
        const parameters: any[] = [];
        for (const col of epilogue.columns) {
            parameters.push(col.value);
            const operator = col.operator || "=";
            switch (operator) {
                case "=":
                    columnOps.push(`${col.column} = \$${pos}`);
                    break;
                case "+=":
                    columnOps.push(
                        `${col.column} = ${col.column} + \$${pos}`);
                    break;
                case "-=":
                    columnOps.push(
                        `${col.column} = ${col.column} - \$${pos}`);
                    break;
                default:
                    throw new PgClientError(
                        `Invalid epilogue operator: ${operator}`);
            }
            pos++;
        }
        const statement =
            `update ${epilogue.entity.table} ` +
            `set ${columnOps.join(", ")} ` +
            `where ${where}`;
        this.log(logger, statement, parameters);
        await client.query(statement, parameters);
    }

    protected async handleEpilogue(logger: Logger, client: pg.Client,
                                   epilogues: Epilogue[],
                                   context?: IContext) : Promise<void> {
        for (const entry of epilogues) {
            if (entry.action == "post") {
                const row = new Row();
                for (const col of entry.columns) {
                    row.add(col.column, col.value);
                }
                const userId = row.has("updatedby") ? row.get("updatedby") :
                    (context?.userAccountId || Nobody.ID);
                await this.postEntity(
                    logger, userId, client, entry.entity, row, context,
                    PgBaseClient.PREVENT_EPILOGUE);
            } else if (entry.action == "put") {
                if (entry.entity.local) {
                    await this.handlePutLocalEpilogue(logger, client, entry);
                } else if (entry.entity.versioned) {
                    /*
                     * NOTE: if no context is specified, we assume that this is
                     * not a user-initiated change, but likely due to
                     * replication, so we silently ignore 'put'
                     * versioned epilogues.
                     */
                    if (context) {
                        await this.handlePutVersionedEpilogue(
                            logger, client, entry, context.userAccountId);
                    }
                } else {
                    throw new PgClientError(
                        `Epilogue 'put' is attempting to modify the ` +
                        `immutable entity ${entry.entity.name}`);
                }
            } else if (entry.action == "delete") {
                if (entry.entity.local) {
                    await this.handleDeleteLocalEpilogue(logger, client, entry);
                } else {
                    throw new PgClientError(
                        `Epilogue 'delete' is attempting to delete a ` +
                        `non-local entity ${entry.entity.name}`);
                }
            } else {
                throw new PgClientError(
                    `Unknown or invalid epilogue action ${entry.action}`);
            }
        }
    }

    protected log(logger: Logger, statement: string, parameters?: any[]): void {
        logger.debug(statement);
        if (parameters && parameters.length) {
            logger.debugAny(parameters);
        }
    }

    protected async pullVcTable(logger: Logger, entity: Entity, id: string,
                                existingClient?: pg.Client)
                                    : Promise<IResultSet> {
        // Pull the version history for this id
        const statement = `select * from ${entity.table}_vc where _id = \$1`;
        const parameters = [id];
        this.log(logger, statement, parameters);
        let result;
        if (existingClient) {
            result = await existingClient.query(statement, parameters);
        } else {
            result = await this.pool.query(statement, parameters);
        }
        return new MemResultSet(result.rows);
    }
}

type PgClientSourceSpec = ClassSpec & {
    pool: string;
}

export class PgClient extends PgBaseClient implements IService,
                IElectorService, ISessionBackendService, IHooverService {
    configuration: Cfg<IConfiguration>;
    private electionLogger: Logger;

    constructor(spec: PgClientSourceSpec) {
        super(spec.pool);
        this.configuration = new Cfg("configuration");
        this.electionLogger = new Logger("server/election");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.configuration.v = configuration;
        this.electionLogger.configure(configuration);
    }

    get isElectorService(): boolean {
        return true;
    }

    get isSessionBackendService(): boolean {
        return true;
    }

    get isHooverService(): boolean {
        return true;
    }

    async getSession(logger: Logger, id: string): Promise<Row> {
        const row = await this.getOne(
            logger, NOCONTEXT, this.sessionEntity.v, id);
        if (!row || row.empty) {
            throw new PgClientError("Session expired", 401);
        }
        if (row.get("expiry") <= Date.now()) {
            // no need to await the deletion
            this.deleteLocal(logger, this.sessionEntity.v, id);
            throw new PgClientError("Session expired", 401);
        }
        return row;
    }

    async createInMemorySession(logger: Logger, userId: string,
                                expiryOverride?: Date,
                                personaOverride?: Persona): Promise<State> {
        const useraccount = await this.getOne(
            logger, NOCONTEXT, this.userEntity.v, userId);
        if (!useraccount || useraccount.empty) {
            throw new PgClientError(`useraccount not found: ${userId}`, 404);
        }
        const persona = personaOverride ||
            this.configuration.v.getPersona(useraccount.get("persona"));
        const session = await this.sessionEntity.v.create(NOCONTEXT, this);
        await this.sessionEntity.v.setValue(
                session,
                "useraccountnum", useraccount.get("useraccountnum"),
                NOCONTEXT)
        const validations: Promise<SideEffects>[] = [];
        if (personaOverride) {
            validations.push(
                this.sessionEntity.v.setValue(
                    session,
                    "persona", personaOverride.name,
                    NOCONTEXT)
            );
        }
        const expiry = expiryOverride ||
            new Date(Date.now() + SessionContext.DEFAULT_TIMEOUT);
        validations.push(
            this.sessionEntity.v.setValue(
                session,
                "expiry", expiry,
                NOCONTEXT)
        );
        const subjectMap: Map<string, string> = new Map();
        const memberships =
            persona.membershipCfgs.filter((cfg) => cfg.through == "subject");
        if (memberships.length > 0) {
            for (const membership of memberships) {
                const membershipEntity =
                    this.configuration.v.getEntity(membership.entity);
                const members =
                    await membershipEntity.getMembers(
                        this, NOCONTEXT, userId, "subject");
                if (members.length > 0) {
                    subjectMap.set(membership.entity, members[0]);
                }
            }
        }
        if (subjectMap.size > 0) {
            validations.push(
                this.sessionEntity.v.setValue(
                session,
                "subjects", serializeSubjectMap(subjectMap),
                NOCONTEXT)
            );
        }
        await Promise.all(validations);
        return session;
    }

    async createSession(logger: Logger, userId: string, expiryOverride?: Date,
                        personaOverride?: Persona): Promise<Row> {
        const session = await this.createInMemorySession(
            logger, userId, expiryOverride, personaOverride);
        const sessionRow =
            await this.sessionEntity.v.post(this, session, NOCONTEXT);
        return sessionRow;
    }

    async deleteSession(logger: Logger, id: string): Promise<void> {
        await this.delete(logger, NOCONTEXT, this.sessionEntity.v, id);
    }

    async deleteSessionsUpTo(logger: Logger, expiry: Date): Promise<void> {
        const statement =
            `delete from ${this.sessionEntity.v.table} where expiry <= \$1`;
        const parameters = [expiry];
        this.log(logger, statement, parameters);
        await this.pool.query(statement, parameters);
    }

    async hoover(logger: Logger, entity: Entity): Promise<void> {
        if (entity.immutable || !entity.retention ||
            entity.retention.style != "temporary" || !entity.retention.age) {
            throw new PgClientError(
                `Entity ${entity.name} is not eligible for hoover()`);
        }
        const interval = entity.retention.age!;
        this.log(logger,
                 `Vacuuming entity ${entity.name} with interval '${interval}'`);
        const client = await this.pool.connect();
        try {
            let statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            statement =
                `select now() - interval '${interval}' as now`;
            this.log(logger, statement);
            let result = await client.query(statement);
            let parameters = [result.rows[0].now];

            statement =
                `delete from ${entity.table}_v as v ` +
                `using ${entity.table}_cy as cy ` +
                `where (v._id = cy._id and v._rev = cy._rev) ` +
                `and cy.updated < \$1 `;
            this.log(logger, statement, parameters);
            result = await client.query(statement, parameters);

            if (result.rowCount) {
                this.log(logger, `Vacuumed ${result.rowCount} _v rows`);
                statement =
                    `update ${entity.table}_vc as vc ` +
                    `set isstub = true ` +
                    `from ${entity.table}_cy as cy ` +
                    `where (vc._id = cy._id and vc._rev = cy._rev) ` +
                    `and cy.updated < \$1 `;
                this.log(logger, statement, parameters);
                result = await client.query(statement, parameters);
                this.log(logger, `Stubbed ${result.rowCount} mvcc (vc) rows`);

                statement =
                    `delete from ${entity.table}_cy where updated < \$1`;
                this.log(logger, statement, parameters);
                result = await client.query(statement, parameters);
                this.log(logger,
                         `Cleared ${result.rowCount} recycle (cy) rows`);
            } else {
                this.log(logger, `No rows to recycle for ${entity.name}`);
            }

            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);

        } catch (err: any) {
            const statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

    async castBallot(logger: Logger, serverId: string, rowId: number,
                     interval: string): Promise<Row> {
        const statement =
            `update leaderelect set lastping = now(), leader = $1 ` +
            `where id = $2 and lastping < (now() - interval '${interval}') ` +
            `returning leader, lastping`;
        const parameters = [serverId, rowId];
        this.log(logger, statement, parameters);
        const result = await this.pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return new Row();
        }
        return Row.dataToRow(result.rows[0]);
    }

    async leaderPing(logger: Logger, serverId: string,
                     rowId: number): Promise<Row> {
        const statement =
            `update leaderelect set lastping = now() ` +
            `where id = $1 and leader = $2 returning leader, lastping`;
        const parameters = [rowId, serverId];
        this.log(logger, statement, parameters);
        const result = await this.pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return new Row();
        }
        return Row.dataToRow(result.rows[0]);
    }

    async getGeneratorNext(logger: Logger, context: IContext,
                           generatorName: string): Promise<string> {
        const dbid = "RZOID" in env ? "" + env.RZOID : "";
        const statement = `select nextval('${generatorName}')`;
        logger.debug(statement);
        const result = await this.pool.query(statement);
        if (result.rows.length === 0) {
            throw new PgClientError(`No rows returned for sequence ` +
                                    `${generatorName}`, 404);
        }
        if (dbid) {
            return `${result.rows[0].nextval}-${dbid}`;
        }
        return "" + result.rows[0].nextval;
    }

    async queryCollection(logger: Logger, context: IContext,
                          collection: Collection,
                          query?: Query): Promise<IResultSet> {
        /* It doesn't make much sense to call queryCollection() for a
         * database client, but we'll simply loop back to the collection
         * and construct the actual query. It may make more sense to simply
         * throw an Error here, but as long as the collection itself doesn't
         * call queryCollection() and cause a recursion, we're OK.
         */
        const finalQuery = await collection.createQuery(context, query);
        return this.getQuery(logger, context, collection.entity.v, finalQuery);
    }

    async getQuery(logger: Logger, context: IContext, entity: Entity,
                   query: Query): Promise<IResultSet> {
        const fields = query.fields.join();
        const fromClause =
            query.hasFromClause ? query.fromClause : `from ${entity.table}`;
        let statement = `select ${fields} ${fromClause}`;
        if (query.filter && query.filter.notEmpty) {
            const where = query.filter.where;
            statement += ` where ${where}`;
        }
        if (query.orderBy.length > 0) {
            const orders: string[] = [];
            query.orderBy.forEach((clause) => {
                orders.push(`${clause.field} ${clause.order}`);
            });
            statement += ` order by ${orders.join()}`;
        }
        this.log(logger, statement);
        const client = await this.pool.connect();
        try {
            const cursor = client.query(new Cursor(statement, []));
            const rows = await cursor.read(this.conn.v.pageSize);
            if (rows.length == 0) {
                this.log(logger, `No rows returned for query: ${statement}`);
                return new EmptyResultSet();
            }
            const resultSet = new MemResultSet(rows);
            await cursor.close();
            return resultSet;
        } finally {
            await client.release();
        }
    }

    protected async putEntity(logger: Logger, userId: string,
                              client: pg.Client, entity: Entity, id: string,
                              row: Row): Promise<Row> {
        if (entity.versioned) {
            const versions = await this.pullVcTable(logger, entity, id, client);
            const mvccResult = this.mvccController.putMvcc(
                row, versions, false, userId);
            await this.applyMvccResults(logger, client, entity, mvccResult);
            return Row.must(mvccResult.leafTable.leafActionPut?.payload);
        } else if (entity.local) {
            row.updateOrAdd("updated", new Date());
            row.updateOrAdd("updatedby", userId);
            const setRow = row.copyWithout(["_id"]);
            const setList = setRow.getUpdateSet();
            const statement =
                `update ${entity.table} set ${setList.join(", ")} ` +
                `where _id = \$${setList.length + 1}`;
            const parameters = setRow.values().concat(id);
            this.log(logger, statement, parameters);
            const pgResult = await client.query(statement, parameters);
            if (pgResult.rowCount != 1) {
                throw new PgClientError(
                    `local put: update ${entity.name}, id = ${id} ; rowCount ` +
                    `was not 1: ${pgResult.rowCount}`);
            }
            return row;
        } else {
            throw new PgClientError(
                `Entity ${entity.name} is ${entity.species}, yet canUpdate ` +
                `is true, however cannot be updated this way`, 400);
        }
    }

    async put(logger: Logger, context: IContext, entity: Entity, id: string,
              row: Row): Promise<Row> {
        if (!entity.canUpdate) {
            throw new PgClientError(
                `Entity ${entity.name} is ${entity.species}, cannot update ` +
                `it this way`, 400);
        }
        const client = await this.pool.connect();
        try {
            await this.checkDupesForPut(logger, client, entity, row);
            return await this.putEntity(
                logger, context.userAccountId, client, entity, id, row);
        } finally {
            client.release();
        }
    }

    private async checkDupesForPut(logger: Logger, client: pg.Client,
                                   entity: Entity, row: Row): Promise<void> {
        // Check dups by key
        if (entity.keyFields.size) {
            let param = 1;
            const parameters = [];
            const keyWhere = [];
            for (const key of entity.keyFields.keys()) {
                keyWhere.push(`${key} = \$${param++}`);
                parameters.push(row.get(key));
            }
            parameters.push(row.get("_id"));
            const statement = `select _id from ${entity.table} ` +
                `where ${keyWhere.join(" and ")} and _id != \$${param} limit 1`;
            this.log(logger, statement, parameters);
            const result = client !== undefined ?
                await client.query(statement, parameters) :
                await this.pool.query(statement, parameters);
            if (result.rows.length) {
                throw new PgClientError(
                    `Duplicate '${entity.name}': ${parameters.join(", ")}`,
                    409);
            }
        }
    }

    private async checkDupesForPost(logger: Logger, entity: Entity, row: Row,
                                    client: pg.Client): Promise<void> {
        // Check dups by key
        if (entity.keyFields.size) {
            let param = 1;
            const parameters = [];
            const keyWhere = [];
            for (const key of entity.keyFields.keys()) {
                keyWhere.push(`${key} = \$${param++}`);
                parameters.push(row.get(key));
            }
            const statement = `select _id from ${entity.table} ` +
                `where ${keyWhere.join(" and ")} limit 1`;
            this.log(logger, statement, parameters);
            const result = client !== undefined ?
                await client.query(statement, parameters) :
                await this.pool.query(statement, parameters);
            if (result.rows.length) {
                throw new PgClientError(
                    `Duplicate '${entity.name}': ${parameters.join(", ")}`,
                    409);
            }
        }
    }

    async post(logger: Logger, context: IContext, entity: Entity,
               row: Row): Promise<Row> {
        const client = await this.pool.connect();
        try {
            let statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);
            await this.checkDupesForPost(logger, entity, row, client);
            const result = await this.postEntity(
                logger, context.userAccountId, client, entity, row, context,
                PgBaseClient.PROCESS_EPILOGUE, this);
            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);
            return result;
        } catch (err: any) {
            const statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

    async processBizTrans(logger: Logger,
                          bizTrans: BizTrans): Promise<BizTrans> {
        const result = new BizTrans();
        const context = bizTrans.context;
        const userId = context.userAccountId;
        const client = await this.pool.connect();
        try {
            let statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);
            for (const entry of bizTrans.entries) {
                let resultRow: Row;
                switch (entry.action) {
                    case "post":
                        await this.checkDupesForPost(
                            logger, entry.entity, entry.row, client);
                        resultRow = await this.postEntity(
                            logger, userId, client, entry.entity, entry.row,
                            context, PgBaseClient.PROCESS_EPILOGUE, this);
                        result.post(logger, context, entry.entity, resultRow);
                        break;
                    case "put":
                        if (!entry.entity.canUpdate) {
                            throw new PgClientError(
                                `Entity ${entry.entity.name} is ` +
                                `${entry.entity.species}, cannot update it ` +
                                `this way`, 400);
                        }
                        await this.checkDupesForPut(
                            logger, client, entry.entity, entry.row);
                        resultRow = await this.putEntity(
                            logger, userId, client, entry.entity, entry.id,
                            entry.row);
                        result.put(
                            logger, context, entry.entity, entry.id, resultRow);
                        break;
                    case "delete":
                        if (!entry.entity.canDelete) {
                            throw new PgClientError(
                                `Entity ${entry.entity.name} is ` +
                                `${entry.entity.species}, cannot delete it ` +
                                `this way`, 400);
                        }
                        if (entry.entity.versioned && !entry.hasRev) {
                            throw new PgClientError(
                                `Must specify 'rev' to delete ` +
                                `${entry.entity.name}`);
                        }
                        await this.deleteEntity(
                            logger, userId, client, entry.entity,
                            entry.id, entry.rev);
                        result.delete(
                            logger, context, entry.entity, entry.id, entry.rev);
                        break;
                    default:
                        throw new PgClientError(
                            `Unknown biztrans action: ${entry.action}`);
                }
            }
            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);
            return result;
        } catch (err: any) {
            const statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

    protected async deleteEntity(logger: Logger, userId: string,
                                 client: pg.Client, entity: Entity,
                                 id: string, rev?: string): Promise<void> {
        if (entity.local) {
            await this.deleteLocal(logger, entity, id, client);
            return;
        }
        const versions = await this.pullVcTable(logger, entity, id, client);
        const mvccResult = this.mvccController.deleteMvcc(
            id, rev!, versions, userId);
        try {
            let statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);
            await this.applyMvccResults(logger, client, entity, mvccResult);
            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);
        } catch (err: any) {
            const statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        }
    }

    private async deleteLocal(logger: Logger, entity: Entity, id: string,
                              client?: pg.Client): Promise<void> {
        const statement =
            `delete from ${entity.table} where _id = \$1`;
        const parameters = [id];
        this.log(logger, statement, parameters);
        if (client) {
            await client.query(statement, parameters);
        } else {
            await this.pool.query(statement, parameters);
        }
    }

    async delete(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev?: string): Promise<void> {

        if (!entity.canDelete) {
            throw new PgClientError(
                `Entity ${entity.name} is ${entity.species}, cannot delete ` +
                `it this way`);
        }
        if (entity.versioned && !rev) {
            throw new PgClientError(
                `Must specify 'rev' to delete ${entity.name}`);
        }
        const client = await this.pool.connect();
        try {
            await this.deleteEntity(
                logger, context.userAccountId, client, entity, id, rev);
        } finally {
            client.release();
        }
    }
}

export class PgClientSource extends ServiceSource {
    _service: PgClient;

    constructor(config: TypeCfg<PgClientSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new PgClient(config.spec);
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this._service.configure(configuration);
    }

    get service(): IService {
        return this._service;
    }
}

