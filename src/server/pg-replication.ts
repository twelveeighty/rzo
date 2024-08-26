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

import Cursor from "pg-cursor";

import md5 from "md5";

import {
    Entity, IConfiguration, AsyncTask, IContext, MemResultSet,
    Row, TypeCfg, ClassSpec, _IError, Logger, JsonObject
} from "../base/core.js";

import { putMvcc, ancestryToAncestors, versionDepth } from "./mvcc.js";
import { PgBaseClient } from "./pg-client.js";

import {
    STATE_TABLE, ReplicationResponse, ChangesFeedQuery,
    NormalChangeFeed, ChangeRecord, RevsDiffResponse, RevsDiffRequest,
    IReplicationService, RevsQuery, BulkDocsRequest,
    BulkGetRequest, BulkGetResponse, BulkGetResponseObj, ReplicationSource
} from "./replication.js";


class PgReplicationError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

type PgReplicationSourceSpec = ClassSpec & {
    pageSize: number;
}

export class PgReplication extends PgBaseClient implements IReplicationService {
    private _spec: PgReplicationSourceSpec;

    constructor(spec: PgReplicationSourceSpec) {
        super();
        this._spec = spec;
        if (this._spec.pageSize <= 0) {
            throw new PgReplicationError(
                `Invalid pageSize: ${this._spec.pageSize}`, 400);
        }
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.sessionEntity.v = configuration.getEntity("session");
        this.userEntity.v = configuration.getEntity("useraccount");
    }

    get isReplicable(): boolean {
        return true;
    }

    start(): void {
    }

    async stop(): Promise<void> {
        if (this._pool) {
            console.log("Ending connection pool...");
            await this._pool.end();
            console.log("Connection pool ended");
        }
    }

    async getChangesNormal(logger: Logger, entity: Entity,
                           query: ChangesFeedQuery): Promise<NormalChangeFeed> {
        if (query.style != "all_docs") {
            throw new PgReplicationError(
                `Change feed query style '${query.style}' is not yet ` +
                `implemented`, 400);
        }
        if (query.feed != "normal") {
            throw new PgReplicationError(
                `Unexpected 'feed' parameter: '${query.feed}'`, 400);
        }
        if (query.since == "now") {
            throw new PgReplicationError(
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
            throw new PgReplicationError(
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

    private revInAncestry(row: Row, rev: string): boolean {
        if (row.get("vc_rev") == rev) {
            return true;
        }
        // now check the ancestry field to see if the target rev is a non-leaf
        // parent.
        const ancestors = ancestryToAncestors(row.get("ancestry"));
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
                    throw new PgReplicationError(
                        `Cannot find leaf record for entity ${entity.name}, ` +
                        `id = ${docReq.id}, rev = ${row.get("vc_rev")}`);
                }
                row.add("_revisions", {
                    start: row.get("versiondepth"),
                    ids: revHashes
                });
                for (const name of PgReplication.VC_COL_ALIASES) {
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
                        for (const name of PgReplication.VC_COL_ALIASES) {
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
        /* ?latest=true and ?revs=true are always assumed.
         * If query.open_revs is empty, we assume ?open_revs=all
         * Since ?latest=true is always assumed to be present (even if it
         * wasn't passed in), we return the leaf record(s) for which the
         * requested ?open_revs array match.
         * A match for open_revs is done through the ancestry. So if a current
         * leaf is 3-abc, and has 2-xyz as an ancestor, if openrevs=[2-xyz],
         * we return that 3-abc leaf. If openrevs=[2-pqr], then
         * {missing: "2-pqr"} is returned, since no leaf has that as its
         * ancestor.
         * The Couch replication protocol is ambigious on whether or not the
         * actual leaf (3-abc) is returned regardless of openrevs.
         * Because PouchDB doesn't return the {missing} object, but throws an
         * error, we are assuming that we only respond to the openrevs specified
         * versions, and leave out the 3-abc version if it's not specifically
         * asked for through one of its ancestors.
         */
        const statement =
            `select ${this.fullyQualifiedVCCols("vc").join()}, v.* ` +
            `from ${entity.table}_vc as vc ` +
            `inner join ${entity.table}_v as v on ` +
            `(vc._id = v._id and vc._rev = v._rev) ` +
            `where vc._id = \$1 and vc.isleaf = \$2 `;
        const parameters = [id, true];
        this.log(logger, statement, parameters);
        const results = await this._pool.query(statement, parameters);
        if (!results.rows.length) {
            throw new PgReplicationError(
                `No rows returned for entity ${entity.name}, ` +
                `id ${id}`, 404);
        }
        const resultSet = new MemResultSet(results.rows);
        const resultRows: Row[] = [];
        if (!query.open_revs.length) {
            // open_revs=all, so return all leafs
            while (resultSet.next()) {
                resultRows.push(resultSet.getRow());
            }
        } else {
            for (const rev of query.open_revs) {
                const matchingRow = resultSet.find(
                    (row) => this.revInAncestry(row, rev));
                if (matchingRow) {
                    resultRows.push(matchingRow);
                } else {
                    resultRows.push(new Row({"missing": rev}));
                }
            }
        }
        const renderedRows: any[] = [];
        for (const row of resultRows) {
            if (row.has("missing")) {
                renderedRows.push(row.raw());
            } else {
                const revisions = {
                    start: row.get("versiondepth"),
                    ids: Entity.asString(
                        row.get("ancestry")).split(".").reverse()
                };
                this.convertDbRowToAppRow(row);
                row.add("_revisions", revisions);
                renderedRows.push(row.raw());
            }
        }
        if (multipart) {
            if (!boundary) {
                throw new PgReplicationError("Empty boundary for multipart");
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

    async postBulkDocs(logger: Logger, entity: Entity,
                 docsRequest: BulkDocsRequest): Promise<ReplicationResponse[]> {
        if (docsRequest["new_edits"] === undefined ||
                docsRequest["new_edits"]) {
            throw new PgReplicationError(
                "new_edits was expected to be 'false' at this point", 400);
        }
        const inputRS = new MemResultSet(docsRequest.docs);
        const result: ReplicationResponse[] = [];
        const mandatory = ["_id", "_rev", "_revisions"];
        const requiredColumns = mandatory.concat(entity.requiredFieldColumns);

        const client = await this._pool.connect();
        try {
            while (inputRS.next()) {
                let statement: string;
                let parameters: any[];
                const row = inputRS.getRow();
                if (!row.hasAll(requiredColumns)) {
                    logger.error(
                        `_bulk_docs POST entity ${entity.name} payload ` +
                        `${JSON.stringify(row.raw())} is missing a required ` +
                        `field`);
                    if (row.hasAll(["_id", "_rev"])) {
                        result.push({
                            id: row.get("_id"),
                            rev: row.get("_rev"),
                            error: "forbidden",
                            reason: "missing required field(s)"
                        });
                    }
                    continue;
                }
                const id = row.get("_id");
                const rev = row.get("_rev");
                // Pull the version history for this id
                statement = `select * from ${entity.table}_vc where id = \$1`;
                parameters = [id];
                this.log(logger, statement, parameters);
                const versionResult = await client.query(statement, parameters);
                const versions = new MemResultSet(versionResult.rows);
                const mvccResult = putMvcc(row, versions, true);
                try {
                    statement = "BEGIN";
                    this.log(logger, statement);
                    await client.query(statement);

                    await this.applyMvccResults(
                        logger, client, entity, mvccResult);

                    statement = "COMMIT";
                    this.log(logger, statement);
                    await client.query(statement);

                    result.push({ id: id, rev: rev, ok: true });
                } catch (err: any) {
                    statement = "ROLLBACK";
                    this.log(logger, statement);
                    await client.query(statement);
                    result.push({
                        id: id,
                        rev: rev,
                        error: "forbidden",
                        reason: (err instanceof Error ? (<Error>err).message :
                                 "unknown")
                    });
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
            throw new PgReplicationError(
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
                if (!results.rows.some(
                    (row) => row["_id"] == id && row["_rev"] == rev)) {
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
            const newVersionDepth = oldRev ? versionDepth(oldRev) + 1 : 1;
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

    async getSequenceId(logger: Logger, context: IContext,
                        entity: Entity): Promise<string> {
        const statement =
            `select coalesce(max(seq), 0) "max" from ${entity.table}_vc`;
        this.log(logger, statement);
        const result = await this._pool.query(statement);
        if (result.rows.length === 0) {
            throw new PgReplicationError(
                `No rows returned for max(seq) ${entity.table}_vc`, 404);
        }
        return "" + result.rows[0].max;
    }

}

export class PgReplicationSource extends ReplicationSource
                                 implements AsyncTask {
    _service: PgReplication;

    constructor(config: TypeCfg<PgReplicationSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new PgReplication(config.spec);
    }

    configure(configuration: IConfiguration) {
        this._service.configure(configuration);
        configuration.registerAsyncTask(this);
    }

    get service(): IReplicationService {
        return this._service;
    }

    async start(): Promise<void> {
        this._service.start();
    }

    async stop(): Promise<void> {
        await this._service.stop();
    }
}

