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

import pg from "pg";
import Pool from "pg-pool";
import Cursor from "pg-cursor";

import { env } from "node:process";

import {
    Entity, IResultSet, IConfiguration, Query, AsyncTask,
    EmptyResultSet, MemResultSet, Row, TypeCfg, ClassSpec, Collection,
    IContext, Filter, ServiceSource, _IError, Nobody, DeferredToken,
    SummaryField, Persona, Cfg, IService, SideEffects, State, Logger
} from "../base/core.js";

import { VERSION, NOCONTEXT } from "../base/configuration.js";

import {
    ISessionBackendService, SessionContext, serializeSubjectMap
} from "../base/session.js";
import { ITaskRunner, Scheduler } from "../base/scheduler.js";

import {
    MvccResult, putMvcc, postMvcc, deleteMvcc, convertToPayload
} from "./mvcc.js";
import { IElectorService, LeaderElector } from "./election.js";

class PgClientError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

class DeferredContext implements IContext {
    sessionId?: string;
    persona: Persona;
    userAccountId: string;

    constructor(userAccountId: string) {
        this.userAccountId = userAccountId;
        this.persona = Nobody.INSTANCE;
    }

    getSubject(key: string): string {
        return "";
    }
}

export class PgBaseClient {
    sessionEntity: Cfg<Entity>;
    userEntity: Cfg<Entity>;
    protected _pool: Pool<pg.Client>;

    static VC_COL_DEFS = [
        "seq", "_id as vc_id", "_rev as vc_rev",
        "updated", "updatedby", "versiondepth", "ancestry",
        "isleaf", "isdeleted", "isstub, isconflict, iswinner"
    ];
    static VC_COL_ALIASES = [
        "seq", "vc_id", "vc_rev",
        "updated", "updatedby", "versiondepth", "ancestry",
        "isleaf", "isdeleted", "isstub", "iswinner"
    ];
    static VC_COL_NAMES = [
        "seq", "_id", "_rev",
        "updated", "updatedby", "versiondepth", "ancestry",
        "isleaf", "isdeleted", "isstub", "iswinner"
    ];

    constructor() {
        this.sessionEntity = new Cfg("session");
        this.userEntity = new Cfg("useraccount");
        this._pool = new Pool();
    }

    configure(configuration: IConfiguration) {
        this.sessionEntity.v = configuration.getEntity("session");
        this.userEntity.v = configuration.getEntity("useraccount");
    }

    protected convertDbRowToAppRow(row: Row): Row {
        if (!row.has("_id") || !row.get("_id")) {
            row.add("_id", row.get("vc_id"));
        }
        if (!row.has("_rev") || !row.get("_rev")) {
            row.add("_rev", row.get("vc_rev"));
        }
        if (row.get("isdeleted")) {
            row.add("_deleted", true);
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
        return row;
    }

    async getSequenceId(logger: Logger, context: IContext,
                  entity: Entity): Promise<string> {
        const statement =
            `select coalesce(max(seq), 0) "max" from ${entity.table}_vc`;
        this.log(logger, statement);
        const result = await this._pool.query(statement);
        if (result.rows.length === 0) {
            throw new PgClientError(`No rows returned for max(seq) ` +
                                    `${entity.table}_vc`, 404);
        }
        return "" + result.rows[0].max;
    }

    async getQueryOne(logger: Logger, context: IContext, entity: Entity,
                      filter: Filter): Promise<Row> {
        const statement =
            `select ${this.fullyQualifiedVCCols("vc").join()}, e.* ` +
            `from ${entity.table} as e ` +
            `inner join ${entity.table}_vc as vc on (vc._id = e._id and ` +
            `vc._rev = e._rev) ` +
            `where (${filter.where}) limit 1`;
        this.log(logger, statement);
        const result = await this._pool.query(statement);
        if (result.rows.length === 0) {
            return new Row();
        }
        return this.convertDbRowToAppRow(Row.dataToRow(result.rows[0], entity));
    }

    async getDBInfo(logger: Logger, context: IContext): Promise<Row> {
        const info = {
            uuid: "00000000000000000000000000000000",
            version: VERSION
        };
        const statement = "select db_uuid()";
        this.log(logger, statement);
        const result = await this._pool.query(statement);
        if (result.rows.length > 0) {
            info.uuid = result.rows[0].db_uuid;
        }
        return new Row(info);
    }

    protected fullyQualifiedVCCols(alias: string): string[] {
        const result: string[] = [];
        PgBaseClient.VC_COL_DEFS.forEach(
            (col) => result.push(`${alias}.${col}`));
        return result;
    }

    protected async applyMvccResults(logger: Logger, client: pg.Client,
                                   entity: Entity,
                                   result: MvccResult): Promise<void> {
        const putStatement = `update ${entity.table}_vc set ` +
            `isleaf = \$1, isdeleted = \$2, isstub = \$3, isconflict = \$4, ` +
            `iswinner = \$5 where seq = \$6`;
        const postStatement = `insert into ${entity.table}_vc (` +
            `_id, _rev, updated, updatedby, versiondepth, ancestry, isleaf, ` +
            `isdeleted, isstub, isconflict, iswinner) values (` +
            `\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9, \$10, \$11)`;
        let parameters: any[];
        for (const vc of result.vcTables) {
            const record = vc.record;
            if (vc.action == "put") {
                parameters = [
                    record.isleaf, record.isdeleted, record.isstub,
                    record.isconflict, record.iswinner, record.seq!
                ];
                this.log(logger, putStatement, parameters);
                await client.query(putStatement, parameters);
            } else if (vc.action == "post") {
                parameters = [
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
            const statement = `insert into ${entity.table}_v ` +
                `(${result.versionTable.payload!.columns.join()}) ` +
                `values (` +
                `${result.versionTable.payload!.columnNumbers.join()}` +
                `)`;
            parameters = result.versionTable.payload!.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        }
        // Entity table
        if (result.leafTable.type == "post") {
            const statement = `insert into ${entity.table} ` +
                `(${result.leafTable.payload!.columns.join()}) ` +
                `values (` +
                `${result.leafTable.payload!.columnNumbers.join()}` +
                `)`;
            parameters = result.leafTable.payload!.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        } else if (result.leafTable.type == "delete") {
            const statement = `delete from ${entity.table} where _id = \$1`;
            parameters = [result.leafTable._id];
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        } else if (result.leafTable.type == "swap") {
            let statement = `delete from ${entity.table} where _id = \$1`;
            parameters = [result.leafTable._id];
            this.log(logger, statement, parameters);
            /* Over time, the _v table could have diverted from the entity
             * table, so only pull the current columns from _v.
             */
            const entityCols = ["_id", "_rev"].concat(entity.allFieldColumns);
            statement =
                `insert into ${entity.table} ` +
                `select ${entityCols.join()} from ${entity.table}_v ` +
                `where _id = \$1 and _rev = \$2`;
            parameters = [result.leafTable._id, result.leafTable._rev];
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        }
    }

    protected log(logger: Logger, statement: string, parameters?: any[]): void {
        logger.debug(statement);
        if (parameters && parameters.length) {
            logger.debug(parameters);
        }
    }
}

type PgClientSourceSpec = ClassSpec & {
    leaderElector: string;
    pageSize: number;
}

export class PgClient extends PgBaseClient implements IService, IElectorService,
                                                      ISessionBackendService,
                                                      ITaskRunner {
    configuration: Cfg<IConfiguration>;
    leaderElector: Cfg<LeaderElector>;
    private _scheduler: Scheduler;
    private _spec: PgClientSourceSpec;
    private electionLogger: Logger;
    private deferredLogger: Logger;

    constructor(spec: PgClientSourceSpec) {
        super();
        this.configuration = new Cfg("configuration");
        this.leaderElector = new Cfg(spec.leaderElector);
        this._spec = spec;
        if (this._spec.pageSize <= 0) {
            throw new PgClientError(
                `Invalid pageSize: ${this._spec.pageSize}`, 400);
        }
        this._scheduler = new Scheduler(30000, this);
        this.electionLogger = new Logger("server/election");
        this.deferredLogger = new Logger("server/deferred");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.leaderElector.setIfCast(
            `Invalid PgClientSource: leaderElector `,
            configuration.workers.get(this.leaderElector.name),
            LeaderElector);
        this.configuration.v = configuration;
        this.leaderElector.v.onChange((leader) => {
            if (leader) {
                this.catchupInitialization();
            }
        });
        this.electionLogger.configure(configuration);
        this.deferredLogger.configure(configuration);
    }

    get isElectorService(): boolean {
        return true;
    }

    get isSessionBackendService(): boolean {
        return true;
    }

    start(): void {
        this._scheduler.start();
    }

    async stop(): Promise<void> {
        this._scheduler.stop();
        if (this._pool) {
            console.log("Ending connection pool...");
            await this._pool.end();
            console.log("Connection pool ended");
        }
    }

    private catchupInitialization(): void {
        this.deferredLogger.log("Running catch-up initialization");
        const now = new Date();
        let statement = "select * from deferredtoken where updated < \$1";
        const parameters = [now];

        this.log(this.deferredLogger, statement, parameters);
        this._pool.query(statement, parameters).then((result) => {
            if (result.rows.length > 0) {
                // Capture all the results before we delete these rows, to
                // avoid other parallel servers picking these up.
                const resultSet = new MemResultSet(result.rows);
                // Delete these rows, to avoid other parallel servers
                // picking these up.
                statement = "delete from deferredtoken where updated < \$1";
                this.log(this.deferredLogger, statement, parameters);
                this._pool.query(statement, parameters).then(() => {
                    while (resultSet.next()) {
                        const token = resultSet.getRow().raw() as DeferredToken;
                        const context = new DeferredContext(token.updatedby);
                        this.performTokenUpdate(
                            this.deferredLogger, context, token, true);
                    }
                });
            } else {
                this.deferredLogger.log("No catch-up tasks to do");
            }
        });
    }

    async getSession(logger: Logger, id: string): Promise<Row> {
        const row = await this.getOne(
            logger, NOCONTEXT, this.sessionEntity.v, id);
        if (!row || row.empty) {
            throw new PgClientError("Session expired", 401);
        }
        if (row.get("expiry") <= Date.now()) {
            // no need to await the deletion
            this.deleteImmutable(
                logger, NOCONTEXT, this.sessionEntity.v, id);
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
                "subjectMap", serializeSubjectMap(subjectMap),
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
        await this.deleteImmutable(
            logger, NOCONTEXT, this.sessionEntity.v, id);
    }

    async deleteSessionsUpTo(logger: Logger, expiry: Date): Promise<void> {
        const statement =
            `delete from ${this.sessionEntity.v.table} where expiry <= \$1`;
        const parameters = [expiry];
        this.log(logger, statement, parameters);
        await this._pool.query(statement, parameters);
    }

    async castBallot(logger: Logger, serverId: string, rowId: number,
                     interval: string): Promise<Row> {
        const statement =
            `update leaderelect set lastping = now(), leader = $1 ` +
            `where id = $2 and lastping < (now() - interval '${interval}') ` +
            `returning leader, lastping`;
        const parameters = [serverId, rowId];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
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
        const result = await this._pool.query(statement, parameters);
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
        const result = await this._pool.query(statement);
        if (result.rows.length === 0) {
            throw new PgClientError(`No rows returned for sequence ` +
                                    `${generatorName}`, 404);
        }
        if (dbid) {
            return `${result.rows[0].nextval}-${dbid}`;
        }
        return "" + result.rows[0].nextval;
    }

    private async getOneImmutable(logger: Logger, context: IContext,
                                  entity: Entity, id: string): Promise<Row> {
        const statement = `select * from ${entity.table} where _id = \$1`;
        const parameters = [id];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return new Row();
        }
        return Row.dataToRow(result.rows[0], entity);
    }

    async getOne(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev?: string): Promise<Row> {
        if (entity.immutable) {
            return await this.getOneImmutable(logger, context, entity, id);
        }
        let row: Row = new Row();
        if (rev) {
            const statement =
                `select ${this.fullyQualifiedVCCols("vc").join()}, v.* ` +
                `from ${entity.table}_v as v ` +
                `inner join ${entity.table}_vc as vc on ` +
                `(vc._id = v._id and vc._rev = v._rev) ` +
                `where v._id = \$1 and v._rev = \$2`;
            const parameters = [id, rev];
            this.log(logger, statement, parameters);
            const result = await this._pool.query(statement, parameters);
            if (result.rows.length > 0) {
                row = this.convertDbRowToAppRow(
                    Row.dataToRow(result.rows[0], entity));
            }
        } else {
            const statement =
                `select ${this.fullyQualifiedVCCols("vc").join()}, e.* ` +
                `from ${entity.table} as e ` +
                `inner join ${entity.table}_vc as vc on (vc._id = e._id and ` +
                `vc._rev = e._rev) ` +
                `where e._id = \$1`;
            const parameters = [id];
            this.log(logger, statement, parameters);
            const result = await this._pool.query(statement, parameters);
            if (result.rows.length > 0) {
                row = this.convertDbRowToAppRow(
                    Row.dataToRow(result.rows[0], entity));
            }
        }
        return row;
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
        const client = await this._pool.connect();
        try {
            const cursor = client.query(new Cursor(statement, []));
            const rows = await cursor.read(this._spec.pageSize);
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

    async put(logger: Logger, context: IContext, entity: Entity, id: string,
              row: Row): Promise<Row> {
        if (entity.immutable) {
            throw new PgClientError(
                `Entity '${entity.name}' is immutable`, 400);
        }

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
            const result = await this._pool.query(statement, parameters);
            if (result.rows.length) {
                throw new PgClientError(
                    `Duplicate '${entity.name}': ${parameters.join(", ")}`,
                    409);
            }
        }

        // Pull the version history for this id
        let statement = `select * from ${entity.table}_vc where id = \$1`;
        let parameters: any[] = [id];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        const versions = new MemResultSet(result.rows);
        const mvccResult = putMvcc(row, versions, false);
        const client = await this._pool.connect();
        try {
            statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            await this.applyMvccResults(logger, client, entity, mvccResult);

            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);

            return Row.must(mvccResult.leafTable.payload);
        } catch (err: any) {
            statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

    async post(logger: Logger, context: IContext, entity: Entity,
               row: Row): Promise<Row> {

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
            const result = await this._pool.query(statement, parameters);
            if (result.rows.length) {
                throw new PgClientError(
                    `Duplicate '${entity.name}': ${parameters.join(", ")}`,
                    409);
            }
        }
        if (entity.immutable) {
            convertToPayload(row);
            row.add("_id", Entity.generateId());
            row.add("updated", new Date());
            row.add("updatedby", context.userAccountId);
            const statement = `insert into ${entity.table} ` +
                `(${row.columns.join()}) ` +
                `values (` +
                `${row.columnNumbers.join()}` +
                `)`;
            const parameters = row.values();
            this.log(logger, statement, parameters);
            await this._pool.query(statement, parameters);
            return row;
        } else {
            let statement: string;
            const mvccResult = postMvcc(row, context);
            const client = await this._pool.connect();
            try {
                statement = "BEGIN";
                this.log(logger, statement);
                await client.query(statement);

                await this.applyMvccResults(logger, client, entity, mvccResult);

                statement = "COMMIT";
                this.log(logger, statement);
                await client.query(statement);

                return Row.must(mvccResult.leafTable.payload);
            } catch (err: any) {
                statement = "ROLLBACK";
                this.log(logger, statement);
                await client.query(statement);
                throw err;
            } finally {
                client.release();
            }
        }
    }

    async deleteImmutable(logger: Logger, context: IContext, entity: Entity,
                          id: string): Promise<void> {
        if (!entity.immutable) {
            throw new PgClientError(
                `Entity ${entity.name} is not immutable`, 500);
        }
        const statement =
            `delete from ${entity.table} where _id = \$1`;
        const parameters = [id];
        this.log(logger, statement, parameters);
        await this._pool.query(statement, parameters);
    }

    async delete(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev: string): Promise<void> {

        if (entity.immutable) {
            await this.deleteImmutable(logger, context, entity, id);
        }
        // Pull the version history for this id
        let statement = `select * from ${entity.table}_vc where id = \$1`;
        let parameters: any[] = [id];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        const versions = new MemResultSet(result.rows);
        const mvccResult = deleteMvcc(id, rev, versions);
        const client = await this._pool.connect();
        try {
            statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            await this.applyMvccResults(logger, client, entity, mvccResult);

            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);

        } catch (err: any) {
            statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

    async getDeferredToken(logger: Logger, context: IContext,
                           tokenUuid: string): Promise<DeferredToken | null> {
        const statement =
            `select * from deferredtoken where token = \$1`;
        const parameters = [tokenUuid];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return null;
        }
        return result.rows[0] as DeferredToken;
    }

    async queryDeferredToken(logger: Logger, context: IContext, parent: string,
                             contained: string, parentField: string,
                             containedField: string,
                             id: string): Promise<DeferredToken | null> {
        const statement =
            `select * from deferredtoken where ` +
            `parent = \$1 and contained = \$2 and parentfield = \$3 and ` +
            `containedfield = \$4 and id = \$5`;
        const parameters = [
            parent,
            contained,
            parentField,
            containedField,
            id
        ];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return null;
        }
        return result.rows[0] as DeferredToken;
    }

    private performTokenUpdate(logger: Logger, context: IContext,
                               token: DeferredToken,
                               skipDelete?: boolean): void {
        if (!skipDelete) {
            const statement = `delete from deferredtoken where token = \$1`;
            const parameters = [token.token];
            this.log(logger, statement, parameters);
            this._pool.query(statement, parameters);
            // Note: this delete command runs parallel to the following
            // statements.
        }

        const entity = this.configuration.v.getEntity(token.parent);
        const field = entity.getField(token.parentfield);
        if (field instanceof SummaryField) {
            (<SummaryField>field).performTokenUpdate(this, token, context);
        } else {
            throw new PgClientError(
                `Invalid token: field ${field.fqName} is not a 'SummaryField'`);
        }
    }

    runTask(context: IContext, row: Row): void {
        const tokenUuid = row.get("token");
        // Only if the current token is the same as 'our' token
        // do we take action. Otherwise, a further update has
        // occurred and we simply exit without doing anything.
        this.getDeferredToken(this.deferredLogger, context, tokenUuid)
        .then((currToken) => {
            if (currToken) {
                this.performTokenUpdate(
                    this.deferredLogger, context, currToken);
            } else {
                console.log(
                    `Deferred token ${tokenUuid} no longer exists`);
            }
        })
        .catch((error) => {
            console.log(
                `PgClient: cannot execute deferred update due to: ${error}`);
        });
    }

    async putDeferredToken(logger: Logger, context: IContext,
                           token: DeferredToken): Promise<number> {
        if (!token.token || !token.updatedby || !token.updated) {
            throw new PgClientError("Invalid Token", 400);
        }
        const existingToken = await this.queryDeferredToken(
            logger, context, token.parent, token.contained, token.parentfield,
            token.containedfield, token.id);
        if (existingToken && existingToken.updated) {
            // if this put comes in less than 30s after the previous put,
            // ignore it.
            const cutoff = Date.now() - 30000;
            if (existingToken.updated.getTime() > cutoff) {
                return 0;
            }
        }
        let statement: string;
        let parameters: any[];

        // Try an update first, if that didn't affect any rows, perform an
        // insert.
        statement =
            `update deferredtoken set ` +
            `token = \$1, updatedby = \$2, updated = \$3 ` +
            `where ` +
            `parent = \$4 and contained = \$5 and parentfield = \$6 and ` +
            `containedfield = \$7 and id = \$8`;
        parameters = [
            token.token,
            token.updatedby,
            token.updated,
            token.parent,
            token.contained,
            token.parentfield,
            token.containedfield,
            token.id
        ];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        if (!result.rowCount) {
            // No update was performed, do an insert instead.
            statement =
                `insert into deferredtoken ` +
                `(parent, contained, parentfield, containedfield, id, token, ` +
                `updatedby, updated) values (` +
                `\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8)`;
            parameters = [
                token.parent,
                token.contained,
                token.parentfield,
                token.containedfield,
                token.id,
                token.token,
                token.updatedby,
                token.updated
            ];
            this.log(logger, statement, parameters);
            await this._pool.query(statement, parameters);
        }
        // Schedule the execution of the token expiry
        this._scheduler.schedule(Row.dataToRow(token), context);
        return 0;
    }

}

export class PgClientSource extends ServiceSource implements AsyncTask {
    _service: PgClient;

    constructor(config: TypeCfg<PgClientSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new PgClient(config.spec);
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this._service.configure(configuration);
        configuration.registerAsyncTask(this);
    }

    get service(): IService {
        return this._service;
    }

    async start(): Promise<void> {
        this._service.start();
    }

    async stop(): Promise<void> {
        await this._service.stop();
    }
}

