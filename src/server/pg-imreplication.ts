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

import Cursor from "pg-cursor";

import {
    Entity, IConfiguration, IContext, MemResultSet, Row, TypeCfg, _IError,
    Logger
} from "../base/core.js";

import {
    ReplicationResponse, ChangesFeedQuery,
    NormalChangeFeed, ChangeRecord,
    IReplicationService, RevsQuery, BulkDocsRequest,
    BulkGetRequest, BulkGetResponse, BulkGetResponseObj, ReplicationSource,
    ChangeStatements
} from "./replication.js";

import { PgReplication, PgReplicationSourceSpec } from "./pg-replication.js";

class PgImReplicationError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export class PgImReplication extends PgReplication
                             implements IReplicationService {

    constructor(spec: PgReplicationSourceSpec) {
        super(spec);
    }

    protected getChangeStatements(logger: Logger, entity: Entity,
                                  query: ChangesFeedQuery): ChangeStatements {
        const result = {
            groupFields: ["count(*)", "max(seq)"],
            selectFields: ["seq", "_id", "_rev"],
            from: `${entity.table}`,
            where: query.since != "0" ? "(seq > $1)" : ""
        };
        if (query.filter) {
            const filterArgs = this.filterArguments(query.filter);
            const repFilter = this.filters.get(filterArgs[0]);
            if (repFilter) {
                /* A source filter changes the 'where' clause, a data filter
                 * changes the select clause. A given filter could have both.
                 */
                if (repFilter.hasSourceFilter()) {
                    result.where =
                        result.where + " and ( " +
                        repFilter.getSourceFilter(filterArgs[1]) +
                        " )";
                }
                if (repFilter.hasDataFilter()) {
                    result.selectFields = ["*"];
                }
            } else {
                throw new PgImReplicationError(
                    `Unknown filter: ${query.filter}`);
            }
        }
        return result;
    }

    async getChangesNormal(logger: Logger, entity: Entity,
                           query: ChangesFeedQuery): Promise<NormalChangeFeed> {
        if (!entity.immutable) {
            throw new PgImReplicationError(
                `Entity ${entity.name} is not immutable`, 400);
        }
        if (query.style != "all_docs") {
            throw new PgImReplicationError(
                `Change feed query style '${query.style}' is not implemented`,
                400);
        }
        if (query.feed != "normal") {
            throw new PgImReplicationError(
                `Unexpected 'feed' parameter: '${query.feed}'`, 400);
        }
        if (query.since == "now") {
            throw new PgImReplicationError(
                "Cannot perform 'normal' change feed when 'since' is 'now'",
                400);
        }
        /* We do not store the converted bigint 'since' value, since we only
         * pass big numbers as strings to pg. But we do check if the passed
         * 'since' query parameter can be interpreted as a bigint.
         */
        this.mvccController.toBigInt(query.since);
        const stmts = this.getChangeStatements(logger, entity, query);
        /* The lastseq return value is always the max(updateseq) of the _vc
         * table, no matter what the 'since' value is. We have to run this
         * query first because the where clause in the 2nd query could return
         * zero rows and therefore 'hide' the max(updateseq) as null.
         * We cannot wait with running this query until after we've seen the
         * 'null' value on the 'since' based query, because there is a (very
         * small) chance rows were added in the time between the two queries
         * and that data would be hidden from the target replicator forever.
         */
        let statement = `select max(seq) from ${entity.table}`;
        this.log(logger, statement);
        let result = await this.pool.query(statement);
        const fallBackLastSeq = this.mvccController.toBigInt(
            result.rows[0].max, 0n);
        statement =
            `select ${stmts.groupFields.join(", ")} from ` +
            `${stmts.from} where ${stmts.where}`;
        const parameters = query.since != "0" ? [query.since] : [];
        this.log(logger, statement, parameters);
        result = await this.pool.query(statement, parameters);
        if (result.rows.length == 0) {
            throw new PgImReplicationError(
                `No rows returned for summary version control query ` +
               `for entity ${entity.name}`, 400);
        }
        /* We use the latest max(seq) pulled from the 2nd query to ensure
         * count(*) and max(seq) are in-sync, in case rows were added in
         * the time between running the two queries.
         */
        const lastSeq = this.mvccController.toBigInt(
            result.rows[0].max, fallBackLastSeq);
        // Count is returned as a string
        const recordCount: number = this.mvccController.toInteger(
            result.rows[0].count, 0);
        if (recordCount == 0) {
            // Nothing to do
            const changesFeed = {
                last_seq: `${lastSeq}`,
                pending: 0,
                results: []
            };
            return changesFeed;
        }
        if (query.limit && query.limit == 0) {
            // Return the count and lastseq
            const changesFeed = {
                last_seq: `${lastSeq}`,
                pending: recordCount,
                results: []
            };
            return changesFeed;
        }
        // Start a Cursor to return 'pageSize' results.
        statement =
            `select ${stmts.selectFields.join(", ")} from ` +
            `${stmts.from} where ${stmts.where} order by seq`;
        const client = await this.pool.connect();
        try {
            this.log(logger, statement, parameters);
            const cursor = client.query(new Cursor(statement, parameters));
            let lastProcessedSeq: bigint = lastSeq;
            const changeResults: ChangeRecord[] = [];
            const changesFeed: NormalChangeFeed = {
                last_seq: `${lastProcessedSeq}`,
                pending: 0,
                results: changeResults
            };
            let moreRows: boolean = true;
            /* The record count is different from the length of the
             * changeFeed.results, since we are grouping the results by _id.
             * But to keep things consistent with the count(*) used above,
             * we count the actual rows processed to calculate the
             * 'pending' value.
             */
            let rowCount = 0;
            while (moreRows) {
                const rows = await cursor.read(this.conn.v.pageSize);
                for (const row of rows) {
                    /* First check if adding this pushes us over
                     * the query.limit (if applicable).
                     */
                    if (query.limit &&
                        changeResults.length >= query.limit) {
                        // This pushes us over the limit, so stop.
                        moreRows = false;
                        break;
                    }
                    const currentSeq = this.mvccController.toBigInt(row["seq"]);
                    const id = row["_id"];
                    const rev = row["_rev"];
                    const changeRecord = {
                        id: id,
                        seq: `${currentSeq}`,
                        changes: [{ rev: rev }]
                    };
                    changeResults.push(changeRecord);
                    rowCount++;
                    lastProcessedSeq = currentSeq;
                }
                /* Keep going unless we've reached the query.limit and broke
                 * out of the for loop or the cursor is exhausted.
                 */
                moreRows = moreRows && (rows.length >= this.conn.v.pageSize);
            }
            await cursor.close();
            changesFeed.last_seq = `${lastProcessedSeq}`;
            const pending = recordCount - rowCount;
            changesFeed.pending = pending < 0 ? 0 : pending;

            return changesFeed;

        } finally {
            await client.release();
        }
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
         *        Because PouchDB uses this during replication instead of
         *        getAllLeafRevs(), we use the same logic: if the requested rev
         *        is not a leaf or deleted, return the leaf and/or deleted that
         *        has the requested rev in its ancestry.
         * For conflicts, even if all conflicted docs were deleted except for
         * the 'winning' one, this returns multiple documents for a requested
         * rev, because conflicts and the deleted ones are returned as well.
         * Note that these assumptions make the passed RevsQuery parameter
         * irrelevant. We keep it for future expansions.
         */
        if (!entity.immutable) {
            throw new PgImReplicationError(
                `Entity ${entity.name} is not immutable`, 400);
        }
        const results: BulkGetResponseObj[] = [];
        for (const docReq of request.docs) {
            const outerIdObj: BulkGetResponseObj = { id: docReq.id, docs: [] };
            results.push(outerIdObj);
            const statement = `select * from ${entity.table} where _id = \$1`;
            const parameters = [docReq.id];
            this.log(logger, statement, parameters);
            const queryResults = await this.pool.query(statement, parameters);
            if (!queryResults.rows.length) {
                outerIdObj.docs.push({ error: {
                    id: docReq.id,
                    rev: docReq.rev || "undefined",
                    error: "not_found",
                    reason: "missing"
                }});
            } else {
                const row = new Row(queryResults.rows[0]);
                const rev = row.getString("_rev");
                if (!docReq.rev || docReq.rev == rev) {
                    this.convertDbRowToAppRow(row);
                    const revisions = {
                        start: this.mvccController.versionDepth(rev),
                        ids: this.mvccController.versionHash(rev)
                    };
                    row.add("_revisions", revisions);
                    outerIdObj.docs.push({ ok: row.raw() });
                } else {
                    outerIdObj.docs.push({ error: {
                        id: docReq.id,
                        rev: docReq.rev,
                        error: "not_found",
                        reason: "missing"
                    }});
                }
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
        if (!entity.immutable) {
            throw new PgImReplicationError(
                `Entity ${entity.name} is not immutable`, 400);
        }
        const statement = `select * from ${entity.table} where _id = \$1`;
        const parameters = [id];
        this.log(logger, statement, parameters);
        const results = await this.pool.query(statement, parameters);
        if (!results.rows.length) {
            throw new PgImReplicationError(
                `No rows returned for entity ${entity.name}, ` +
                `id ${id}`, 404);
        }
        const queryRow = new Row(results.rows[0]);
        const resultRows: Row[] = [];
        if (!query.open_revs.length) {
            // open_revs=all, so return all leafs
            resultRows.push(queryRow);
        } else {
            for (const rev of query.open_revs) {
                if (queryRow.get("_rev") == rev) {
                    resultRows.push(queryRow);
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
                const rev = row.get("_rev");
                const revisions = {
                    start: this.mvccController.versionDepth(rev),
                    ids: this.mvccController.versionHash(rev)
                };
                this.convertDbRowToAppRow(row);
                row.add("_revisions", revisions);
                renderedRows.push(row.raw());
            }
        }
        if (multipart) {
            if (!boundary) {
                throw new PgImReplicationError("Empty boundary for multipart");
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

    private stripColumnsForPost(row: Row): void {
        for (const col of ["seq", "_revisions"]) {
            row.delete(col);
        }
    }

    async postBulkDocs(logger: Logger, entity: Entity,
                       docsRequest: BulkDocsRequest):
                           Promise<ReplicationResponse[]> {
        if (docsRequest["new_edits"] === undefined ||
                docsRequest["new_edits"]) {
            throw new PgImReplicationError(
                "new_edits was expected to be 'false' at this point", 400);
        }
        if (!entity.immutable) {
            throw new PgImReplicationError(
                `Entity ${entity.name} is not immutable`, 400);
        }
        const inputRS = new MemResultSet(docsRequest.docs);
        const result: ReplicationResponse[] = [];
        const requiredColumns = ["_id", "_rev", "updated", "updatedby"].concat(
            entity.requiredFieldColumns);
        const client = await this.pool.connect();
        try {
            while (inputRS.next()) {
                let statement: string;
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
                this.stripColumnsForPost(row);
                const id = row.getString("_id");
                const rev = row.getString("_rev");
                const epilogue = entity.hasEpilogue(row) ?
                    entity.epilogue(row) : null;
                try {
                    statement = "BEGIN";
                    this.log(logger, statement);
                    await client.query(statement);

                    statement = `insert into ${entity.table} ` +
                        `(${row.columns.join()}) ` +
                        `values (` +
                        `${row.columnNumbers.join()}` +
                        `)`;
                    const parameters = row.values();
                    this.log(logger, statement, parameters);
                    await client.query(statement, parameters);

                    if (epilogue) {
                        await this.handleEpilogue(logger, client, epilogue);
                    }

                    statement = "COMMIT";
                    this.log(logger, statement);
                    await client.query(statement);

                    result.push({ id: id, rev: rev, ok: true });
                } catch (err: any) {
                    logger.exc(err);
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

    protected getRevsTable(entity: Entity): string {
        return `${entity.table}`;
    }

    async getSequenceId(logger: Logger, context: IContext,
                        entity: Entity): Promise<string> {
        if (!entity.immutable) {
            throw new PgImReplicationError(
                `Entity ${entity.name} is not immutable`, 400);
        }
        const statement =
            `select coalesce(max(seq), 0) "max" from ${entity.table}`;
        this.log(logger, statement);
        const result = await this.pool.query(statement);
        if (result.rows.length === 0) {
            throw new PgImReplicationError(
                `No rows returned for max(updateseq) ${entity.table}`, 404);
        }
        return "" + result.rows[0].max;
    }
}

export class PgImReplicationSource extends ReplicationSource {
    _service: PgImReplication;

    constructor(config: TypeCfg<PgReplicationSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new PgImReplication(config.spec);
    }

    configure(configuration: IConfiguration) {
        this._service.configure(configuration);
    }

    get service(): IReplicationService {
        return this._service;
    }
}

