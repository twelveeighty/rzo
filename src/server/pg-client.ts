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

import md5 from "md5";

import {
    Entity, IResultSet, IConfiguration, Query, AsyncTask,
    EmptyResultSet, MemResultSet, Row, TypeCfg, ClassSpec, Collection,
    IContext, Filter, Source, _IError, Nobody, DeferredToken,
    CoreColumns, SummaryField, Persona, Cfg, IService, SideEffects, State,
    Logger, JsonObject
} from "../base/core.js";

import { VERSION, NOCONTEXT } from "../base/configuration.js";

import {
    ISessionBackendService, SessionContext, serializeSubjectMap
} from "../base/session.js";

import { ITaskRunner, Scheduler } from "../base/scheduler.js";

import {
    STATE_TABLE, ReplicationResponse, ChangesFeedQuery,
    NormalChangeFeed, ChangeRecord, RevsDiffResponse, RevsDiffRequest,
    IReplicableService, RevsQuery, DeletedDoc, DocRevisions, PurgedDoc,
    BulkDocsRequest, BulkGetRequest, BulkGetResponse, BulkGetResponseObj
} from "./replication.js";

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

type PgClientSourceSpec = ClassSpec & {
    leaderElector: string;
    pageSize: number;
}

export class PgClient implements IReplicableService, IElectorService,
                                 ISessionBackendService, ITaskRunner {
    configuration: Cfg<IConfiguration>;
    leaderElector: Cfg<LeaderElector>;
    sessionEntity: Cfg<Entity>;
    userEntity: Cfg<Entity>;
    private _scheduler: Scheduler;
    private _pool: Pool<pg.Client>;
    private _spec: PgClientSourceSpec;
    private electionLogger: Logger;
    private deferredLogger: Logger;

    static VC_COL_DEFS = [
        "seq", "_id as vc_id", "updated as vc_updated",
        "updatedby as vc_updatedby", "versiondepth", "ancestry",
        "_rev as vc_rev", "isleaf", "isdeleted", "isstub"
    ];
    static VC_COL_ALIASES = [
        "seq", "vc_id", "vc_updated", "vc_updatedby", "versiondepth",
        "ancestry", "vc_rev", "isleaf", "isdeleted", "isstub"
    ];
    static VC_COL_NAMES = [
        "seq", "_id", "updated", "updatedby", "versiondepth",
        "ancestry", "_rev", "isleaf", "isdeleted", "isstub"
    ];

    constructor(spec: PgClientSourceSpec) {
        this.configuration = new Cfg("configuration");
        this.leaderElector = new Cfg(spec.leaderElector);
        this.sessionEntity = new Cfg("session");
        this.userEntity = new Cfg("useraccount");
        this._spec = spec;
        if (this._spec.pageSize <= 0) {
            throw new PgClientError(
                `Invalid pageSize: ${this._spec.pageSize}`, 400);
        }
        this._scheduler = new Scheduler(30000, this);
        this._pool = new Pool();
        this.electionLogger = new Logger("server/election");
        this.deferredLogger = new Logger("server/deferred");
    }

    configure(configuration: IConfiguration) {
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
        this.sessionEntity.v = configuration.getEntity("session");
        this.userEntity.v = configuration.getEntity("useraccount");
        this.electionLogger.configure(configuration);
        this.deferredLogger.configure(configuration);
    }

    get isReplicable(): boolean {
        return true;
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

    private log(logger: Logger, statement: string, parameters?: any[]): void {
        logger.debug(statement);
        if (parameters && parameters.length) {
            logger.debug(parameters);
        }
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

    async getChangesNormal(logger: Logger, entity: Entity,
                           query: ChangesFeedQuery): Promise<NormalChangeFeed> {
        if (query.style != "all_docs") {
            throw new PgClientError(
                `Change feed query style '${query.style}' is not yet ` +
                `implemented`, 400);
        }
        if (query.feed != "normal") {
            throw new PgClientError(
                `Unexpected 'feed' parameter: '${query.feed}'`, 400);
        }
        if (query.since == "now") {
            throw new PgClientError(
                "Cannot perform 'normal' change feed when 'since' is 'now'",
                400);
        }
        const whereClause = query.since != "0" ?
            "isleaf = true and seq > $1" : "isleaf = true" ;
        let statement =
            `select max(seq), count(*) from ${entity.table}_vc ` +
            `where ${whereClause}`;
        let parameters = query.since != "0" ?
            [ChangesFeedQuery.parseNumber(query.since)] : [];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        if (result.rows.length == 0) {
            throw new PgClientError(
                `No rows returned for summary version control query ` +
               `for entity ${entity.name}`, 400);
        }
        const recordCount = result.rows[0].count;
        const lastSeq = result.rows[0].max;
        if (!recordCount || recordCount == 0) {
            // nothing to do
            const changesFeed = {
                last_seq: lastSeq ? lastSeq as number : 0,
                pending: 0,
                results: []
            };
            return changesFeed;
        }
        // Start a Cursor to return the first 'pageSize' results.
        // It is possible that new rows are added between the previous summary
        // query and the upcoming details query, so we'll cap it at the
        // summary result for 'lastSeq', since that will have a consistent
        // accompanying 'recordCount' value. Otherwise, we will not know how
        // many records are 'pending'.
        statement =
            `select seq, _id, _rev, isdeleted from ${entity.table}_vc ` +
            `where ` + whereClause + " order by seq";
        const client = await this._pool.connect();
        try {
            this.log(logger, statement, parameters);
            const cursor = client.query(new Cursor(statement, parameters));
            const rows = await cursor.read(this._spec.pageSize);
            let count = 0;
            let lastProcessedSeq = 0;
            const changesFeed: NormalChangeFeed = {
                last_seq: 0,
                pending: 0,
                results: []
            };
            for (const row of rows) {
                const currentSeq = row.seq as number;
                if (currentSeq > lastSeq) {
                    break;
                }
                lastProcessedSeq = currentSeq;
                count++;
                const changeRecord: ChangeRecord = {
                    id: `${row["_id"]}`,
                    seq: `${currentSeq}`,
                    changes: [ { rev: `${row["_rev"]}` } ]
                };
                if (row.isdeleted) {
                    changeRecord.deleted = true;
                }
                changesFeed.results.push(changeRecord);
            }
            await cursor.close();

            changesFeed.last_seq = lastProcessedSeq;
            const pending = recordCount - count;
            changesFeed.pending = pending < 0 ? 0 : pending;

            return changesFeed;

        } finally {
            await client.release();
        }
    }

    fullyQualifiedVCCols(alias: string): string[] {
        const result: string[] = [];
        PgClient.VC_COL_DEFS.forEach((col) => result.push(`${alias}.${col}`));
        return result;
    }

    versionConflict(resultSet: IResultSet): boolean {
        resultSet.rewind();
        let leafFound = false;
        while (resultSet.next()) {
            if (resultSet.get("isleaf") && !resultSet.get("isdeleted")) {
                if (leafFound) {
                    return true;
                } else {
                    leafFound = true;
                }
            }
        }
        return false;
    }

    winningRev(resultSet: IResultSet,
               includeDeleted?: boolean): Row | undefined {
        const candidates: Row[] = [];
        const deleted: Row[] = [];
        let maxDepth = 0;
        let maxDelDepth = 0;
        resultSet.rewind();
        while (resultSet.next()) {
            if (resultSet.get("isleaf")) {
                const depth = resultSet.get("versiondepth");
                const isDeleted = resultSet.get("isdeleted");
                if (!isDeleted) {
                    if (depth > maxDepth) {
                        candidates.splice(0);
                        candidates.push(resultSet.getRow());
                        maxDepth = depth;
                    } else if (depth == maxDepth) {
                        candidates.push(resultSet.getRow());
                    }
                } else {
                    if (depth > maxDelDepth) {
                        deleted.splice(0);
                        deleted.push(resultSet.getRow());
                        maxDelDepth = depth;
                    } else if (depth == maxDelDepth) {
                        deleted.push(resultSet.getRow());
                    }
                }
            }
        }
        if (candidates.length) {
            return CoreColumns.versionWinner(candidates);
        } else {
            if (includeDeleted) {
                return CoreColumns.versionWinner(deleted);
            } else {
                return undefined;
            }
        }
    }

    revInAncestry(row: Row, rev: string): boolean {
        if (row.get("vc_rev") == rev) {
            return true;
        }
        // now check the ancestry field to see if the target rev is a non-leaf
        // parent.
        const ancestors = CoreColumns.ancestryToAncestors(row.get("ancestry"));
        return ancestors.includes(rev);
    }

    async postBulkGet(logger: Logger, entity: Entity, request: BulkGetRequest,
                      query: RevsQuery): Promise<BulkGetResponse> {
        /* We make some simplifications in handling the generic _bulk_get
         * API call. Since this is part of the Replication interface, we assume
         * the following:
         *      - The ?revs=true parameter is always assumed to be present
         *        (and set to 'true').
         *      - The ?latest=true parameter is always present (and set to
         *        'true'). Curiously, this is not even documented as a possible
         *        parameter in the CouchDB API documentation, but PouchDB sets
         *        this during replication calls.
         * Note that these assumptions make the passed RevsQuery parameter
         * irrelevant. We keep it for future expansions.
         */
        const results: BulkGetResponseObj[] = [];
        let statement: string;
        let parameters: any[];
        for (const docReq of request.docs) {
            const outerIdObj: BulkGetResponseObj = { id: docReq.id, docs: [] };
            results.push(outerIdObj);
            // Get all (possibly conflicting) leaf records
            statement =
                `select ${this.fullyQualifiedVCCols("vc").join()}, v.* ` +
                `from ${entity.table}_vc as vc ` +
                `left join ${entity.table}_v as v on (vc._id = v._id and ` +
                `vc._rev = v._rev) ` +
                `where vc._id = \$1 and vc.isleaf = \$2 ` +
                `order by vc.versiondepth desc`;
            parameters = [docReq.id, true];
            this.log(logger, statement, parameters);
            const leafResults = await this._pool.query(statement, parameters);
            let docReqRevFound = false;
            // These leaf results are always returned
            for (const dbRow of leafResults.rows) {
                const row = Row.dataToRow(dbRow);
                const revHashes = Entity.asString(
                    row.get("ancestry")).split(".").reverse();
                /* 1. Check to see if we had _v match on the original query
                 * 2. add _revisions column
                 * 3. Remove all _vc columns
                 */
                if (!row.has("_id") || !row.get("_id")) {
                    throw new PgClientError(
                        `Cannot find leaf record for entity ${entity.name}, ` +
                        `id = ${docReq.id}, rev = ${row.get("vc_rev")}`);
                }
                row.add("_revisions", {
                    start: row.get("versiondepth"),
                    ids: revHashes
                });
                for (const name of PgClient.VC_COL_ALIASES) {
                    row.delete(name);
                }
                if (docReq.rev && row.get("_rev") == docReq.rev) {
                    docReqRevFound = true;
                }
                outerIdObj.docs.push({ ok: row.raw() });
            }

            /* If a specific revision was queried, check if we've already
             * handled that with the leaf version(s), otherwise try to locate
             * it.
             */
            if (docReq.rev && !docReqRevFound) {
                statement =
                    `select ${this.fullyQualifiedVCCols("vc").join()}, v.* ` +
                    `from ${entity.table}_vc as vc ` +
                    `left join ${entity.table}_v as v on (vc._id = v._id and ` +
                    `vc._rev = v._rev) ` +
                    `where vc._id = \$1 and vc._rev = \$2 `;
                parameters = [docReq.id, docReq.rev];
                this.log(logger, statement, parameters);
                const reqResults = await this._pool.query(
                    statement, parameters);
                if (reqResults.rows.length) {
                    const row = Row.dataToRow(reqResults.rows[0]);
                    const revHashes = Entity.asString(
                        row.get("ancestry")).split(".").reverse();
                    const revisions = {
                        start: row.get("versiondepth"),
                        ids: revHashes
                    };
                    if (!row.has("_id") || !row.get("_id")) {
                        /* This is a stub, or otherwise missing payload.
                         * Add a stub record with _revisions
                         */
                        const stub = {
                            _id: docReq.id,
                            _rev: docReq.rev,
                            _revisions: revisions
                        };
                        outerIdObj.docs.push({ ok: stub });
                    } else {
                        row.add("_revisions", revisions);
                        for (const name of PgClient.VC_COL_ALIASES) {
                            row.delete(name);
                        }
                        outerIdObj.docs.push({ ok: row.raw() });
                    }
                } else {
                    // The requested version is not found
                    outerIdObj.docs.push(
                        {
                            error: {
                                id: docReq.id,
                                rev: docReq.rev,
                                error: "not_found",
                                reason: "missing"
                            }
                        }
                    );
                }
            } else if (!outerIdObj.docs.length) {
                // We have not found any matches or leafs
                outerIdObj.docs.push(
                    {
                        error: {
                            id: docReq.id,
                            rev: docReq.rev || "undefined",
                            error: "not_found",
                            reason: "missing"
                        }
                    }
                );
            }
        }
        return { results: results };
    }

    async getAllLeafRevs(logger: Logger, entity: Entity, id: string,
                         query: RevsQuery, multipart: boolean,
                         boundary?: string): Promise<string> {
        // For a given ID, we assume that there is never a large number
        // of versions present.
        const statement =
            `select ${this.fullyQualifiedVCCols("vc").join()}, v.* ` +
            `from ${entity.table}_vc as vc ` +
            `left join ${entity.table}_v as v on (vc._id = v._id and ` +
            `vc._rev = v._rev) ` +
            `where vc._id = \$1 ` +
            `order by vc.versiondepth desc`;
        const parameters = [id];
        this.log(logger, statement, parameters);
        const results = await this._pool.query(statement, parameters);
        if (!results.rows.length) {
            throw new PgClientError(
                `No rows returned for entity ${entity.name}, ` +
                `id ${id}`, 404);
        }
        const resultSet = new MemResultSet(results.rows);
        const winner = this.winningRev(resultSet, true);
        if (!winner) {
            throw new PgClientError(
                `Cannot establish winner from statement ${statement}, ` +
                `id ${id}`);
        }
        const winningRevision = winner.get("_rev");
        const resultRows: Row[] = [];
        for (const rev of query.open_revs) {
            if (rev == winningRevision) {
                continue;
            }
            const matchingRow = resultSet.find(
                (row) => this.revInAncestry(row, rev));
            if (matchingRow) {
                // only add this version if it's a leaf, therefore an exact
                // match to the vc_rev column, not a parent from the
                // ancestry column. Otherwise, just skip this version.
                if (matchingRow.get("vc_rev") == rev) {
                    resultRows.push(matchingRow);
                }
            } else {
                resultRows.push(new Row({"missing": rev}));
            }
        }
        if (winner) {
            resultRows.push(winner);
        }
        const renderedRows: any[] = [];
        for (const row of resultRows) {
            if (row.has("missing")) {
                renderedRows.push(row.raw());
            } else {
                const revHashes = Entity.asString(
                    row.get("ancestry")).split(".").reverse();
                if (row.get("isdeleted")) {
                    const deletedDoc: DeletedDoc = {
                        _id: row.get("vc_id"),
                        _rev: row.get("vc_rev"),
                        _deleted: true,
                        _revisions: {
                            start: row.get("versiondepth"),
                            ids: revHashes
                        }
                    };
                    renderedRows.push(deletedDoc);
                } else {
                    // 1. Check to see if we had _v match on the original query
                    // 2. add _revisions column
                    // 3. Remove all _vc columns
                    if (!row.has("_id") || !row.get("_id")) {
                        throw new PgClientError(
                            `Cannot find non-leaf, non-deleted record for ` +
                            `entity ${entity.name}, id = ${id}`);
                    }
                    row.add("_revisions", {
                        start: row.get("versiondepth"),
                        ids: revHashes
                    });
                    for (const name of PgClient.VC_COL_ALIASES) {
                        row.delete(name);
                    }
                    renderedRows.push(row.raw());
                }
            }
        }
        if (multipart) {
            if (!boundary) {
                throw new PgClientError("Empty boundary for multipart");
            }
            const buffer: string[] = [];
            for (const row of renderedRows) {
                buffer.push(`--${boundary}`);
                buffer.push("Content-Type: application/json");
                buffer.push("");
                buffer.push(JSON.stringify(row));
            }
            buffer.push(`--${boundary}--`);
            return buffer.join("\n");
        } else {
            return JSON.stringify(renderedRows);
        }
    }

    private ancestorFor(currentRevs: IResultSet,
                        ancestors: string[], start: number): Row | undefined {
        // if there's only one ancestor, we return undefined, since we have
        // already checked for duplicate revisions.
        if (ancestors.length <= 1 || start <= 1) {
            return undefined;
        }
        let idx = 1;
        let depth = start - 1;
        while (depth > 0 && idx < ancestors.length) {
            const checkRev = `${depth}-${ancestors[idx]}`;
            const targetLeaf = currentRevs.find((row) =>
                row.get("isleaf") && row.get("_rev") == checkRev
            );
            if (targetLeaf) {
                return targetLeaf;
            }
            idx++;
            depth--;
        }
        return undefined;
    }

    async postBulkDocs(logger: Logger, entity: Entity,
                 docsRequest: BulkDocsRequest): Promise<ReplicationResponse[]> {
        if (docsRequest["new_edits"] === undefined ||
                docsRequest["new_edits"]) {
            throw new PgClientError(
                "new_edits was expected to be 'false' at this point", 400);
        }
        const inputRS = new MemResultSet(docsRequest.docs);
        const result: ReplicationResponse[] = [];
        const defaultTimestamp = new Date();
        const mandatory = ["_id", "_rev", "_revisions"];
        const trackChanges = ["updated", "updatedby"];
        const requiredColumns = entity.requiredFieldColumns.concat(
            trackChanges);
        const legalColumns = entity.allFieldColumns.concat(mandatory,
            trackChanges, "_deleted");

        const client = await this._pool.connect();
        try {
            while (inputRS.next()) {
                const row = inputRS.getRow();
                const cols = row.columns;

                const nextResponse: ReplicationResponse = {
                    id: row.has("_id") ? row.getString("_id") : "?",
                    rev: row.has("_rev") ? row.getString("_rev") : "?",
                    ok: true
                }
                result.push(nextResponse);

                let statement: string;
                let parameters: any[];
                const timestamp = row.has("updated") ?
                    row.get("updated") : defaultTimestamp;
                const userAccountId = row.has("updatedby") ?
                    row.get("updatedby") : Nobody.ID;
                try {
                    statement = "BEGIN";
                    this.log(logger, statement);
                    await client.query(statement);

                    if (!mandatory.every((column) => cols.includes(column))) {
                        throw new PgClientError(
                            `Missing one of: ${mandatory}`);
                    }
                    const isDeleted = cols.includes("_deleted");
                    if (!isDeleted) {
                        if (!requiredColumns.every((column) =>
                                                  cols.includes(column))) {
                            throw new PgClientError(
                                `Missing one of: ${requiredColumns}`);
                        }
                    }
                    for (const column of cols) {
                        if (!legalColumns.includes(column)) {
                            throw new PgClientError(
                                `invalid attribute: '${column}'`);
                        }
                    }

                    const id = row.getString("_id");
                    const rev = row.getString("_rev");
                    const depth = CoreColumns.versionDepth(rev);
                    const docRevisions = row.get("_revisions");
                    if (docRevisions === null) {
                        throw new PgClientError(
                            "malformed '_revisions' attribute");
                    }
                    const revisions = docRevisions as DocRevisions;
                    const ancestry = revisions.ids.slice().reverse().join(".");

                    nextResponse["id"] = id;
                    nextResponse["rev"] = rev;

                    statement =
                        `select * from ${entity.table}_vc ` +
                        `where _id = \$1 ` +
                        `order by versiondepth desc`;
                    parameters = [id];

                    this.log(logger, statement, parameters);
                    const qResponse = await client.query(statement, parameters);
                    const vcResultSet = new MemResultSet(qResponse.rows);

                    if (qResponse.rows.some((row) => row["_rev"] == rev)) {
                        throw new PgClientError(
                            `id: ${id}, rev: ${rev} already exists`);
                    }

                    // Calculate the current winner, prior to any changes.
                    const oldWinner = this.winningRev(vcResultSet);
                    const oldInConflict = this.versionConflict(vcResultSet);

                    // _vc table
                    const vcRow = Row.emptyRow(PgClient.VC_COL_NAMES);
                    vcRow.delete("seq");
                    vcRow.put("_id", id);
                    vcRow.put("_rev", rev);

                    // If this row came from Couch/Pouch, there will
                    // not be anything else but the _deleted column, so
                    // we will have to use current time and NOBODY for
                    // updated and updatedby.
                    vcRow.put("updated", timestamp);
                    vcRow.put("updatedby", userAccountId);

                    vcRow.put("versiondepth", depth);
                    vcRow.put("ancestry", ancestry);
                    vcRow.put("isleaf", true);
                    vcRow.put("isdeleted", isDeleted);
                    vcRow.put("isstub", false);

                    statement =
                        `insert into ${entity.table}_vc ` +
                        `(${vcRow.columns.join()}) ` +
                        `values ` +
                        `(${vcRow.columnNumbers.join()}) `;
                    parameters = vcRow.values();
                    this.log(logger, statement, parameters);
                    await client.query(statement, parameters);

                    // The posted rev is *always* a leaf, therefore now check
                    // if this new leaf turns a previous leaf into non-leaf.
                    const vcParentRow = this.ancestorFor(
                        vcResultSet, revisions.ids, revisions.start);

                    // Add the posted rev to the list of revs
                    vcResultSet.addRow(vcRow);

                    if (vcParentRow) {
                        // To calculate the (new) winner with this change,
                        // mark the vcParentRow as non-leaf.
                        vcParentRow.put("isleaf", false);
                    }

                    if (!isDeleted) {
                        // _v table
                        const entityColumns = cols.filter(
                            (column) => column != "_revisions");
                        const vRow = Row.emptyRow(entityColumns);
                        for (const column of entityColumns) {
                            vRow.put(column, row.get(column));
                        }

                        statement =
                            `insert into ${entity.table}_v ` +
                            `(${vRow.columns.join()}) ` +
                            `values ` +
                            `(${vRow.columnNumbers.join()}) `;
                        parameters = vRow.values();
                        this.log(logger, statement, parameters);
                        await client.query(statement, parameters);
                    }

                    if (vcParentRow) {
                        statement =
                            `update ${entity.table}_vc ` +
                            `set isleaf = false where seq = \$1`;
                        parameters = [vcParentRow.get("seq")];
                        this.log(logger, statement, parameters);
                        await client.query(statement, parameters);
                    }

                    // Calculate the *new* winner, if any.
                    const winner = this.winningRev(vcResultSet);
                    const inConflict = this.versionConflict(vcResultSet);

                    // Outcomes:
                    //
                    //   oldWinner     winner   Result
                    //
                    //      -            -      Do nothing
                    //      -            Y      Create Y as entity, set
                    //                            inconflict if needed.
                    //      X            -      Remove entity X and all its
                    //                            ChangeLogs
                    //      X            X      Update inconflict on X if needed
                    //      X            Y      Replace entity X with Y and
                    //                            update the ChangeLogs, set
                    //                            inconflict as needed on Y

                    const oldWinnerRev = oldWinner ? oldWinner.get("_rev") : "";
                    const winnerRev = winner ? winner.get("_rev") : "";
                    // Entity table
                    if (oldWinnerRev && winnerRev) {
                        if (oldWinnerRev != winnerRev) {
                            // Replace entity X with Y
                            await this.replaceEntityVersion(
                                logger, client, timestamp, entity, id,
                                oldWinnerRev, winnerRev, inConflict,
                                userAccountId);
                        } else {
                            // Update inconflict on X if needed
                            if (oldInConflict != inConflict) {
                                statement =
                                    `update ${entity.table} ` +
                                    `set inconflict = \$1 ` +
                                    `where _id = \$2`;
                                parameters = [inConflict, id];
                                this.log(logger, statement, parameters);
                                await client.query(statement, parameters);
                            }
                        }
                    } else if (oldWinnerRev) {
                        // Remove entity X and all its ChangeLogs
                        await this.deleteEntity(
                            logger, client, timestamp, entity, id, oldWinnerRev,
                            userAccountId);
                    } else if (winnerRev) {
                        // Create Y as entity, set inconflict if needed.
                        await this.electEntityVersion(
                            logger, client, timestamp, entity, id, winnerRev,
                            inConflict, userAccountId);
                    }
                    statement = "COMMIT";
                    this.log(logger, statement);
                    await client.query(statement);
                } catch (error) {
                    console.log(error);
                    statement = "ROLLBACK";
                    this.log(logger, statement);
                    await client.query(statement);

                    let msg = "sorry";
                    if (error instanceof Error) {
                        msg = (<Error>error).message || "sorry";
                    }
                    nextResponse["ok"] = false;
                    nextResponse["error"] = "forbidden";
                    nextResponse["reason"] = msg;
                }
            }
        } finally {
            client.release();
        }
        return result;
    }

    async getRevsDiffRequest(logger: Logger, entity: Entity,
                     diffRequest: RevsDiffRequest): Promise<RevsDiffResponse> {
        const ids = Object.keys(diffRequest);
        const versions: string[] = [];
        for (const versionArray of Object.values(diffRequest)) {
            versions.push(...versionArray);
        }
        if (ids.length == 0 || versions.length == 0) {
            throw new PgClientError(
                `Invalid request json - no ids and/or versions found`, 400);
        }
        const idInList = "'" + ids.join("', '") + "'";
        const versionInList = "'" + versions.join("', '") + "'";
        const statement =
            `select _id, _rev from ${entity.table}_vc where ` +
            `_id in (${idInList}) and _rev in (${versionInList})`;
        this.log(logger, statement);
        const results = await this._pool.query(statement);
        // This query obviously can return more rows than intended if a given
        // version is identical between two different records. However, we
        // are only looking for those records that do NOT exist.
        const response: RevsDiffResponse = {};
        for (const [id, revs] of Object.entries(diffRequest)) {
            for (const rev of revs) {
                if (!results.rows.some((row) =>
                                     row["_id"] == id && row["_rev"] == rev)) {
                    if (id in response) {
                        response[id].missing.push(rev);
                    } else {
                        response[id] = { missing: [rev] };
                    }
                }
            }
        }
        return response;
    }

    async putReplicationState(logger: Logger, entity: Entity, id: string,
                              repState: JsonObject):
                                  Promise<ReplicationResponse> {
        const _id = `_local/${id}`;
        let _rev = "0-0";
        const client = await this._pool.connect();
        let statement: string;
        try {
            let parameters: any[];
            statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            let oldRev: string | null = null;
            // Update if exists, otherwise Create
            statement =
                `select entity, _id, _rev from ${STATE_TABLE} where ` +
                `entity = \$1 and _id = \$2 limit 1`;
            parameters = [entity.name, _id];
            this.log(logger, statement, parameters);
            const result = await client.query(statement, parameters);
            if (result.rows.length) {
                oldRev = result.rows[0]._rev;
            }
            const newVersionDepth = oldRev ?
                CoreColumns.versionDepth(oldRev) + 1 : 1;
            const versionHash = md5(JSON.stringify(repState));

            _rev = `${newVersionDepth}-${versionHash}`;

            if (oldRev) {
                statement =
                    `update ${STATE_TABLE} set _rev = \$1, contents = \$2 ` +
                    `where entity = \$3 and _id = \$4`;
                parameters = [_rev, repState, entity.name, _id];
            } else {
                statement =
                    `insert into ${STATE_TABLE} ` +
                    `(entity, _id, _rev, contents) values ` +
                    `(\$1, \$2, \$3, \$4)`;
                parameters = [entity.name, _id, _rev, repState];
            }

            this.log(logger, statement, parameters);
            await this._pool.query(statement, parameters);

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

        return { "id": _id, "ok": true, "rev": _rev };
    }

    async getReplicationState(logger: Logger, entity: Entity,
                             id: string): Promise<JsonObject | null> {
        const statement =
            `select * from ${STATE_TABLE} where entity = \$1 and ` +
            `_id = \$2`;
        const parameters = [entity.name, id];
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return null;
        }
        return result.rows[0].contents;
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

    async getOne(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev?: string): Promise<Row> {
        let statement: string;
        let parameters: any[];
        if (rev) {
            statement =
                `select * from ${entity.table}_v where _id = \$1 ` +
                `and _rev = \$2`;
            parameters = [id, rev];
        } else {
            statement = `select * from ${entity.table} where _id = \$1`;
            parameters = [id];
        }
        this.log(logger, statement, parameters);
        const result = await this._pool.query(statement, parameters);
        if (result.rows.length === 0) {
            return new Row();
        }
        return Row.dataToRow(result.rows[0]);
    }

    async getQueryOne(logger: Logger, context: IContext, entity: Entity,
                      filter: Filter): Promise<Row> {
        const statement =
            `select * from ${entity.table} where (${filter.where}) limit 1`;
        this.log(logger, statement);
        const result = await this._pool.query(statement);
        if (result.rows.length === 0) {
            return new Row();
        }
        return Row.dataToRow(result.rows[0]);
    }

    async queryCollection(logger: Logger, context: IContext,
                          collection: Collection,
                          query?: Query): Promise<IResultSet> {
        // It doesn't make much sense to call queryCollection() for a
        // database client, but we'll simply loop back to the collection
        // and construct the actual query. It may make more sense to simply
        // throw an Error here, but as long as the collection itself doesn't
        // call queryCollection() and cause a recursion, we're OK.
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
        row.delete("_id");
        const columns = row.columns;
        if (!columns.length) {
            throw new PgClientError(`No data provided for entity ` +
                                    `'${entity.name}', id '${id}'`, 400);
        }
        const expectedVersion = row.get("_rev");
        if (!expectedVersion) {
            throw new PgClientError(`Invalid version encountered for ` +
                                    `'${entity.name}', id '${id}'`, 409);
        }
        /*
            - Start transaction - BEGIN
            - Query current record using id, if not found, reject - ROLLBACK
            - Check version match. If no match, reject - ROLLBACK
            - Check duplicate by natural key(s), if conflict, reject - ROLLBACK
            - Insert current record into _v table. If fail - ROLLBACK
            - Update current record with row. If fail - ROLLBACK
            - COMMIT
        */
        const timestamp = new Date();
        const client = await this._pool.connect();
        let statement: string;
        try {
            let parameters: any[];
            statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            statement =
                `select e.*, vc.ancestry from ${entity.table} as e ` +
                `inner join ${entity.table}_vc as vc using (_id, _rev) ` +
                `where e._id = \$1`;
            parameters = [id];
            this.log(logger, statement, parameters);
            const result = await client.query(statement, parameters);
            if (result.rows.length === 0) {
                throw new PgClientError(`Entity not found: ` +
                                    `'${entity.name}', id '${id}'`, 404);
            }
            const oldRow = Row.dataToRow(result.rows[0]);
            const oldVersion = oldRow.get("_rev");
            if (oldVersion != expectedVersion) {
                throw new PgClientError(
                    `Your changes are for an outdated version ` +
                    `'${expectedVersion}' versus the current '${oldVersion}'` +
                    `, please re-apply changes against the current version`,
                        409);
            }

            // Check dups by key(s)
            if (entity.keyFields.size) {
                parameters = [];
                let param = 1;
                const keyWhere = [];
                for (const key of entity.keyFields.keys()) {
                    keyWhere.push(`${key} = \$${param++}`);
                    parameters.push(row.get(key));
                }
                keyWhere.push(`_id != \$${param++}`);
                parameters.push(id);
                statement = `select _id from ${entity.table} ` +
                    `where ${keyWhere.join(" and ")} limit 1`;
                this.log(logger, statement, parameters);
                const result = await client.query(statement, parameters);
                if (result.rows.length) {
                    throw new PgClientError(
                        `Duplicate '${entity.name}': ${parameters.join(", ")}`,
                        409);
                }
            }

            for (const changeLog of entity.changeLogs) {
                if (changeLog.shouldTrigger(oldRow, row)) {
                    const logRow =
                        changeLog.createChangeLogRow(row, context.userAccountId,
                                                     timestamp, oldRow);

                    statement = `insert into ${changeLog.table} ` +
                        `(${logRow.columns.join()}) ` +
                        `values (${logRow.columnNumbers.join()})`;
                    parameters = logRow.values();
                    this.log(logger, statement, parameters);
                    await client.query(statement, parameters);
                }
            }

            CoreColumns.addToRowForPut(row, context, timestamp, md5);

            const setList = row.getUpdateSet();
            parameters = row.values().concat(id);
            statement = `update ${entity.table} set ${setList.join(", ")} ` +
                `where _id = \$${setList.length + 1}`;
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);

            // _v table, re-add 'id' and remove 'inconflict' from the row.
            row.add("_id", id);
            row.delete("inconflict");
            statement = `insert into ${entity.table}_v ` +
                `(${row.columns.join()}) ` +
                `values (${row.columnNumbers.join()})`;
            parameters = row.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);

            // _vc table: update old version and insert new version with
            // ancestry.
            statement =
                `update ${entity.table}_vc set isleaf = false where ` +
                `_id = \$1 and _rev = \$2`;
            parameters = [id, oldVersion];
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);

            const version = row.get("_rev");
            const newHash = CoreColumns.versionHash(version);
            const newAncestry = `${oldRow.get("ancestry")}.${newHash}`;
            statement =
                `insert into ${entity.table}_vc (` +
                `_id, versiondepth, ancestry, _rev, updated, updatedby,` +
                `isleaf, isdeleted, isstub) ` +
                `values (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)`;
            parameters = [
                id,
                CoreColumns.versionDepth(version),
                newAncestry,
                version,
                timestamp,
                row.get("updatedby"),
                true,
                false,
                false
            ];
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);

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
        return row;
    }

    async post(logger: Logger, context: IContext, entity: Entity,
               row: Row): Promise<Row> {
        const client = await this._pool.connect();
        const timestamp = new Date();
        let statement: string;
        try {
            let parameters: any[];
            statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            // Check dups by key
            if (entity.keyFields.size) {
                let param = 1;
                parameters = [];
                const keyWhere = [];
                for (const key of entity.keyFields.keys()) {
                    keyWhere.push(`${key} = \$${param++}`);
                    parameters.push(row.get(key));
                }
                statement = `select _id from ${entity.table} ` +
                    `where ${keyWhere.join(" and ")} limit 1`;
                this.log(logger, statement, parameters);
                const result = await client.query(statement, parameters);
                if (result.rows.length) {
                    throw new PgClientError(
                        `Duplicate '${entity.name}': ${parameters.join(", ")}`,
                        409);
                }
            }

            CoreColumns.addToRowForPost(row, context, timestamp, md5);

            statement = `insert into ${entity.table} (${row.columns.join()}) ` +
                `values (${row.columnNumbers.join()})`;
            parameters = row.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);

            for (const changeLog of entity.changeLogs) {
                const logRow =
                    changeLog.createChangeLogRow(row, context.userAccountId,
                                                 timestamp);

                statement = `insert into ${changeLog.table} ` +
                    `(${logRow.columns.join()}) ` +
                    `values (${logRow.columnNumbers.join()})`;
                parameters = logRow.values();
                this.log(logger, statement, parameters);
                await client.query(statement, parameters);
            }

            if (!entity.immutable) {
                // _v table
                row.delete("inconflict");
                statement = `insert into ${entity.table}_v ` +
                    `(${row.columns.join()}) ` +
                    `values (${row.columnNumbers.join()})`;
                parameters = row.values();
                this.log(logger, statement, parameters);
                await client.query(statement, parameters);

                // _vc table
                const version = row.get("_rev");
                statement =
                    `insert into ${entity.table}_vc (` +
                    `_id, versiondepth, ancestry, _rev, updated, updatedby,` +
                    `isleaf, isdeleted, isstub) ` +
                    `values (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)`;
                parameters = [
                    row.get("_id"),
                    CoreColumns.versionDepth(version),
                    CoreColumns.versionHash(version),
                    version,
                    row.get("updated"),
                    row.get("updatedby"),
                    true,
                    false,
                    false
                ];
                this.log(logger, statement, parameters);
                await client.query(statement, parameters);
            }

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
        return row;
    }

    private async electEntityVersion(logger: Logger, client: pg.Client,
                                     timestamp: Date, entity: Entity,
                                     id: string, rev: string,
                                     inconflict: boolean,
                                     userAccountId: string): Promise<Row> {
        let statement: string;
        let parameters: any[];

        statement = `select * from ${entity.table}_v ` +
                    `where _id = \$1 and _rev = \$2`;
        parameters = [id, rev];
        this.log(logger, statement, parameters);
        let result = await client.query(statement, parameters);

        if (result.rows.length === 0) {
            throw new PgClientError(
                `Version not found: '${entity.name}', id '${id}', ` +
                `rev '${rev}'`, 404);
        }
        const row = Row.dataToRow(result.rows[0]);
        row.add("inconflict", inconflict);

        // Attempt to update the existing entity row, if it exists.
        const updateSet = row.getUpdateSet();
        statement = `update ${entity.table} set ${updateSet.join(", ")} ` +
            `where _id = \$${updateSet.length + 1}`;
        parameters = row.values().concat(id);
        this.log(logger, statement, parameters);
        result = await client.query(statement, parameters);

        // If the update didn't affect any rows, do an insert.
        if (!result.rowCount) {
            statement = `insert into ${entity.table} ` +
                        `(${row.columns.join()}) ` +
                        `values (${row.columnNumbers.join()})`;
            parameters = row.values();
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        }
        return row;
    }

    private async replaceEntityVersion(logger: Logger, client: pg.Client,
                                       timestamp: Date, entity: Entity,
                                       id: string, fromRev: string,
                                       toRev: string, inconflict: boolean,
                                       userAccountId: string): Promise<void> {
        let statement: string;
        let parameters: any[];

        statement = `select * from ${entity.table}_v ` +
                    `where _id = \$1 and _rev = \$2`;
        parameters = [id, fromRev];
        this.log(logger, statement, parameters);
        let result = await client.query(statement, parameters);

        if (result.rows.length === 0) {
            throw new PgClientError(`Entity not found: ` +
                                `'${entity.name}', id '${id}', ` +
                                `rev '${fromRev}'`, 404);
        }
        const oldRow = Row.dataToRow(result.rows[0]);

        const row = await this.electEntityVersion(
            logger, client, timestamp, entity, id, toRev, inconflict,
            userAccountId);

        for (const changeLog of entity.changeLogs) {
            if (changeLog.shouldTrigger(oldRow, row)) {
                const logRow =
                    changeLog.createChangeLogRow(
                        row, userAccountId, timestamp, oldRow);

                statement = `insert into ${changeLog.table} ` +
                    `(${logRow.columns.join()}) ` +
                    `values (${logRow.columnNumbers.join()})`;
                parameters = logRow.values();
                this.log(logger, statement, parameters);
                await client.query(statement, parameters);
            }
        }
    }

    private async cascadeDelete(logger: Logger, client: pg.Client,
                                timestamp: Date, entity: Entity, id: string,
                                version: string,
                                userAccountId: string): Promise<void> {
        let statement: string;
        let parameters: any[];

        // A contained entity's FieldChangeLog entities will also
        // be in 'our' list of entity.contains. These are all marked
        // 'immutable'. We deal with these first, since they all depend on their
        // respective targetEntity, which is a child of the parent entity.
        for (const contained of entity.contains) {
            if (!contained.immutable) {
                continue;
            }
            const parentKey = contained.parentKeyFor(entity.name);
            const parentIdName = parentKey.idName;
            // If immutable, the parentIdName is the id column name that
            // the FieldChangeLog's targetEntity reports as pointing to
            // 'contained'. And the _id field for the immutable entity is
            // always equal to its targetEntity.
            statement =
                `delete from ${contained.table} where ` +
                `_id in (select _id from ${parentKey.entity.table} ` +
                `  where ${parentIdName} = \$1)`;
            parameters = [id];
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        }

        // We delete all 'contained' entities, making sure to update their
        // _vc tables accordingly. Because there could have been conflicting
        // rows in the contained's _vc table, we have to ensure to mark ALL
        // leaf versions as deleted.
        for (const contained of entity.contains) {
            if (contained.immutable) {
                continue;
            }
            const parentIdName = contained.parentKeyFor(entity.name).idName;
            const entityColumns = contained.allFieldColumns;
            // Query all non-deleted contained leafs
            statement =
                `select ${this.fullyQualifiedVCCols("vc").join()}, v.* ` +
                `from ${contained.table}_vc as vc ` +
                `left join ${contained.table}_v as v on (vc._id = v._id and ` +
                `vc._rev = v._rev) ` +
                `where vc._id in (select _id from ${contained.table} ` +
                `  where ${parentIdName} = \$1) ` +
                `and vc.isleaf = true and vc.isdeleted = false ` +
                `order by vc._id`;
            parameters = [id];
            this.log(logger, statement, parameters);
            const result = await client.query(statement, parameters);

            const resultSet = new MemResultSet(result.rows);
            while (resultSet.next()) {
                const ancestry = resultSet.get("ancestry");
                const containedVersion = resultSet.get("vc_rev");
                const containedId = resultSet.get("vc_id");

                let targetedEntityRow: Row;

                // If the left join resulted in a non-null value for the _v
                // table, that means the version to be deleted exist in the
                // _v table.
                if (resultSet.get("_id")) {
                    targetedEntityRow = Row.emptyRow(
                        CoreColumns.addToV(entityColumns));
                    targetedEntityRow.copyFrom(resultSet.getRow());
                } else {
                    // This is very unlikely to happen, but not
                    // considered an error: a leaf version
                    // does not have a payload for it in the _v table.
                    //
                    // Since all we have is its version hash, we'll
                    // use that to calculate the deleted hash.
                    const purgedDoc: PurgedDoc = {
                        _id: id,
                        _rev: containedVersion,
                        purged_hash: containedVersion
                    };
                    targetedEntityRow = Row.dataToRow(purgedDoc);
                }
                // insert a new row into _vc
                const delVersion = CoreColumns.newVersion(
                    targetedEntityRow, "DELETE", md5);
                const delHash = CoreColumns.versionHash(delVersion);
                const delAncestry = `${ancestry}.${delHash}`;
                statement =
                    `insert into ${contained.table}_vc (` +
                    `_id, versiondepth, ancestry, _rev, updated, ` +
                    `updatedby, isleaf, isdeleted, isstub) ` +
                    `values (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)`;
                parameters = [
                    containedId,
                    CoreColumns.versionDepth(delVersion),
                    delAncestry,
                    delVersion,
                    timestamp,
                    userAccountId,
                    true,
                    true,
                    false
                ];
                this.log(logger, statement, parameters);
                await client.query(statement, parameters);

                // Mark the _vc row as isleaf = false
                statement =
                    `update ${contained.table}_vc set isleaf = false ` +
                    `where _id = \$1 and _rev = \$2`;
                parameters = [containedId, containedVersion];
                this.log(logger, statement, parameters);
                await client.query(statement, parameters);
            }

            // The contained entity itself.
            statement =
                `delete from ${contained.table} where ${parentIdName} = \$1`;
            parameters = [id];
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        }
    }

    private async deleteEntity(logger: Logger, client: pg.Client,
                               timestamp: Date, entity: Entity, id: string,
                               version: string, userAccountId: string,
                               cascade?: boolean): Promise<void> {
        let statement: string;
        let parameters: any[];

        // The caller has already taken care of the parent entity's _vc table.
        statement =
            `delete from ${entity.table} where _id = \$1 and _rev = \$2`;
        parameters = [id, version];
        this.log(logger, statement, parameters);
        await client.query(statement, parameters);

        // The ChangeLogs for the parent entity.
        // ChangeLogs always have the same _id as their parent.
        for (const changeLog of entity.changeLogs) {
            statement =
                `delete from ${changeLog.table} where _id = \$1`;
            parameters = [id];
            this.log(logger, statement, parameters);
            await client.query(statement, parameters);
        }

        if (cascade) {
            await this.cascadeDelete(
                logger, client, timestamp, entity, id, version, userAccountId);
        }
    }

    private async handleVCDelete(logger: Logger, context: IContext,
                                 client: pg.Client, timestamp: Date,
                                 entity: Entity, id: string, version: string,
                                 ancestry: string): Promise<void> {
        let statement: string;
        let parameters: any[];
        // Query the _v table for the targeted version
        statement =
            `select * from ${entity.table}_v ` +
            `where _id = \$1 and _rev = \$2`;
        parameters = [id, version];
        this.log(logger, statement, parameters);
        const vResult = await client.query(statement, parameters);
        let targetedEntityRow: Row;
        if (vResult.rows.length) {
            targetedEntityRow = Row.dataToRow(vResult.rows[0]);
        } else {
            // This is very unlikely to happen, but not
            // considered an error: a leaf version
            // is deleted, but we have (no longer?) a payload
            // for it in the _v table.
            //
            // Since all we have is its version hash, we'll
            // use that to calculate the deleted hash.
            const purgedDoc: PurgedDoc = {
                _id: id,
                _rev: version,
                purged_hash: version
            };
            targetedEntityRow = Row.dataToRow(purgedDoc);
        }
        // insert a new row into _vc
        const delVersion = CoreColumns.newVersion(
            targetedEntityRow, "DELETE", md5);
        const delHash = CoreColumns.versionHash(delVersion);
        const delAncestry = `${ancestry}.${delHash}`;
        statement =
            `insert into ${entity.table}_vc (` +
            `_id, versiondepth, ancestry, _rev, updated, ` +
            `updatedby, isleaf, isdeleted, isstub) ` +
            `values (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)`;
        parameters = [
            id,
            CoreColumns.versionDepth(delVersion),
            delAncestry,
            delVersion,
            timestamp,
            context.userAccountId,
            true,
            true,
            false
        ];
        this.log(logger, statement, parameters);
        await client.query(statement, parameters);

        // Update the existing _vc row and mark as non-leaf,
        // and isstub if we couldn't find the _v record.
        statement =
            `update ${entity.table}_vc set ` +
            `isleaf = false, ` +
            `isstub = \$1 ` +
            `where _id = \$2 and _rev = \$3`;
        parameters = [
            (vResult.rows.length == 0),
            id,
            version
        ];
        this.log(logger, statement, parameters);
        await client.query(statement, parameters);
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
        const client = await this._pool.connect();
        let statement: string;
        const timestamp = new Date();
        try {
            let parameters: any[];

            statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            // Query all leaves for this id that are not deleted.
            statement =
                `select * from ${entity.table}_vc ` +
                `where _id = \$1 and isleaf = true and isdeleted = false`;
            parameters = [id];
            this.log(logger, statement, parameters);
            const vcResult = await client.query(statement, parameters);
            const vcResultSet = new MemResultSet(vcResult.rows);

            // Check to make sure our targeted _rev actually exists.
            const targetedVCRow = vcResultSet.find((row) =>
                                                row.get("_rev") == rev);

            if (!targetedVCRow) {
                throw new PgClientError(
                    `Record not found: Entity ${entity.name}, id: ${id}, ` +
                    `version: ${rev}`, 404);
            }

            const ancestry = targetedVCRow.get("ancestry");

            await this.handleVCDelete(
                logger, context, client, timestamp, entity, id, rev,
                ancestry);

            // Query the current elected winner, if any
            statement =
                `select _rev, inconflict from ${entity.table} ` +
                `where _id = \$1`;
            parameters = [id];
            const eResult = await client.query(statement, parameters);
            const oldWinner = eResult.rows.length ?
                eResult.rows[0]["_rev"] : "";
            const currentConflict = eResult.rows.length ?
                eResult.rows[0]["inconflict"] : false;

            // Mark the targeted VC as non-leaf and calculate the new winner
            targetedVCRow.put("isleaf", false);
            const newWinnerRow = this.winningRev(vcResultSet);
            const newConflict = this.versionConflict(vcResultSet);
            const newWinner = newWinnerRow ? newWinnerRow.get("_rev") : "";

            if (oldWinner) {
                if (newWinner) {
                    if (newWinner == oldWinner) {
                        // Result: this is a delete of a conflicted branch, that
                        // wasn't the winner.
                        // If inconflict was changed, update entity table.
                        if (currentConflict != newConflict) {
                            statement =
                                `update ${entity.table} ` +
                                `set inconflict = \$1 ` +
                                `where _id = \$2 and _rev = \$3`;
                            parameters = [newConflict, id, newWinner];
                            this.log(logger, statement, parameters);
                            await client.query(statement, parameters);
                        }
                    } else {
                        // Result: newWinner replaces oldWinner.
                        // Remove oldWinner from the entity table, copy
                        // newWinner to the entity table and then update
                        // changeLogs and recalculate.
                        await this.replaceEntityVersion(
                            logger, client, timestamp, entity, id, oldWinner,
                            newWinner, newConflict, context.userAccountId);
                    }
                } else {
                    // Result: this is an actual delete, since there is no
                    // more 'entity' winner. Delete oldWinner then cascade it
                    // to all 'contained' entities and changeLogs for oldWinner.
                    await this.deleteEntity(
                        logger, client, timestamp, entity, id, oldWinner,
                        context.userAccountId, true);
                }
            } else {
                if (newWinner) {
                    // Race-conditions aside, this should not be possible:
                    // we are deleting a particular rev, so that should only
                    // *reduce* the number of possible 'winners', not somehow
                    // elect a winner where there wasn't one before.
                    //
                    // Let's just bail out for now with an error.
                    throw new PgClientError(
                        `Deletion of entity ${entity.name}, id: ${id} ` +
                        `rev: ${rev} results in winning version ` +
                        `${newWinner} which wasn't the current winner`, 409);
                }
            }

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
}

export class PgClientSource extends Source implements AsyncTask {
    pgClient: PgClient;

    constructor(config: TypeCfg<PgClientSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.pgClient = new PgClient(config.spec);
    }

    configure(configuration: IConfiguration) {
        this.pgClient.configure(configuration);
        configuration.registerAsyncTask(this);
    }

    get service(): IService {
        return this.pgClient;
    }

    async start(): Promise<void> {
        this.pgClient.start();
    }

    async stop(): Promise<void> {
        await this.pgClient.stop();
    }
}

