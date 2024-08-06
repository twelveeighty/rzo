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

import { IncomingMessage, ServerResponse } from "http";

import {
    _IError, IService, JsonObject, Entity, Row, DaemonWorker, Cfg, TypeCfg,
    IConfiguration
} from "../base/core.js";

import { IAdapter, AdapterError, AdapterSpec, getHeader } from "./adapter.js";

class ReplicationError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export interface IReplicableService extends IService {
    get isReplicable(): boolean;
    getReplicationLogs(entity: Entity, id: string): Promise<ReplicationState>;
    putReplicationState(destination: Destination,
                     repState: ReplicationState): Promise<ReplicationResponse>;
    getChangesNormal(entity: Entity,
                     query: ChangesFeedQuery): Promise<NormalChangeFeed>;
    getRevsDiffRequest(entity: Entity,
                       diffRequest: RevsDiffRequest): Promise<RevsDiffResponse>;
    getAllLeafRevs(entity: Entity, id: string, query: RevsQuery,
                   multipart: boolean, boundary?: string): Promise<string>;
    postBulkDocs(entity: Entity,
                 docsRequest: BulkDocsRequest): Promise<ReplicationResponse[]>;
}

export type DocRevisions = {
    ids: string[];
    start: number;
}

export type DeletedDoc = {
    _id: string;
    _rev: string;
    _deleted: boolean;
    _revisions: DocRevisions;
};

type ReplicationSession = {
    doc_write_failures?: number;
    docs_read?: number;
    docs_written?: number;
    end_last_seq?: number;
    end_time?: string;
    missing_checked?: number;
    missing_found?: number;
    recorded_seq: number;
    session_id: string;
    start_last_seq?: number;
    start_time?: string;
}

export type ReplicationState = {
    _id: string;
    _rev: string;
    _revisions?: DocRevisions;
    history: ReplicationSession[];
    replication_id_version: number;
    session_id: string;
    source_last_seq: number;
}

export type ReplicationResponse = {
    id: string;
    ok: boolean;
    rev: string;
    error?: string;
    reason?: string;
}

type ChangeRevisions = {
    rev: string;
}

export type ChangeRecord = {
    id: string;
    seq: string;
    changes: ChangeRevisions[];
    deleted?: boolean;
}

export type NormalChangeFeed = {
    last_seq: number;
    pending?: number;
    results: ChangeRecord[];
}

export type RevsDiffRequest = {
    [name: string]: string[];
};

export type RevsDiffResponse = {
    [name: string]: {
        missing: string[];
    };
};

export type BulkDocsRequest = {
    docs: JsonObject[];
    new_edits: boolean;
}

export type PurgedDoc = {
    _id: string;
    _rev: string;
    purged_hash: string;
}

export class Destination {
    static TABLE = "local_replication";

    entity: Entity;

    constructor(entity: Entity) {
        this.entity = entity;
    }

    get columns(): string[] {
        return [
            "seq",
            "_rev",
            "entity",
            "replicationid",
            "replication_id_version",
            "doc_write_failures",
            "docs_read",
            "docs_written",
            "end_last_seq",
            "end_time",
            "missing_checked",
            "missing_found",
            "recorded_seq",
            "session_id",
            "start_last_seq",
            "start_time"
        ];
    }

    static formatUTC(rowData: unknown): string {
        if (!(rowData instanceof Date)) {
            throw new ReplicationError("Invalid date object encountered");
        }
        const rowDate = rowData as Date;
        return rowDate.toUTCString();
    }

    static rowsToReplicationState(rows: any[]): ReplicationState {
        if (!rows.length) {
            throw new ReplicationError(`No logs found`, 404);
        }
        const firstRow = rows[0];
        const sessions: ReplicationSession[] = [];
        const state: ReplicationState = {
            "_id": firstRow["replicationid"],
            "_rev": firstRow["_rev"],
            "history": sessions,
            "replication_id_version": firstRow["replication_id_version"],
            "session_id": firstRow["session_id"],
            "source_last_seq": firstRow["recorded_seq"]
        }
        rows.forEach((row) => {
            sessions.push({
                "doc_write_failures": row["doc_write_failures"],
                "docs_read": row["docs_read"],
                "docs_written": row["docs_written"],
                "end_last_seq": row["end_last_seq"],
                "end_time": Destination.formatUTC(row["end_time"]),
                "missing_checked": row["missing_checked"],
                "missing_found": row["missing_found"],
                "recorded_seq": row["recorded_seq"],
                "session_id": row["session_id"],
                "start_last_seq": row["start_last_seq"],
                "start_time": Destination.formatUTC(row["start_time"])
            });
        });
        return state;
    }

    optional(session: JsonObject, name: string,
             convert?: (value: any) => any): any {
        if (Object.hasOwn(session, name) && session[name]) {
            if (convert) {
                return convert(session[name]);
            }
            return session[name];
        }
        return null;
    }

    replicationStateToRow(repState: ReplicationState): Row {
        const session = repState.history.find(
            (session) => session.recorded_seq == repState.source_last_seq);
        if (!session) {
            throw new ReplicationError(
                `Cannot find session for source_last_seq = ` +
                `${repState.source_last_seq}`);
        }
        const row = Row.emptyRow(this.columns);
        row.put("entity", this.entity.name);
        row.put("replicationid", repState._id);
        row.put("replication_id_version", repState.replication_id_version);
        row.put("doc_write_failures",
                this.optional(session, "doc_write_failures"));
        row.put("docs_read",
                this.optional(session, "docs_read"));
        row.put("docs_written",
                this.optional(session, "docs_written"));
        row.put("end_last_seq",
                this.optional(session, "end_last_sequence"));
        row.put("end_time",
                this.optional(session, "end_time", (input) => new Date(input)));
        row.put("missing_checked",
                this.optional(session, "missing_checked"));
        row.put("missing_found",
                this.optional(session, "missing_found"));
        row.put("recorded_seq", session.recorded_seq);
        row.put("session_id", session.session_id);
        row.put("start_last_seq",
                this.optional(session, "start_last_seq"));
        row.put("start_time",
                this.optional(session, "start_time",
                              (input) => new Date(input)));
        return row;
    }

    static creationDDL(dropFirst?: boolean): string {
        const drop   = `
drop table if exists ${Destination.TABLE};
`       ;
        const create = `
create table ${Destination.TABLE} (
   seq                     bigserial primary key,
   _rev                    varchar(43) not null,
   entity                  varchar(46) not null,
   replicationid           varchar(128) not null,
   replication_id_version  integer not null,
   doc_write_failures      integer not null,
   docs_read               integer not null,
   docs_written            integer not null,
   end_last_seq            integer not null,
   end_time                timestamptz not null,
   missing_checked         integer not null,
   missing_found           integer not null,
   recorded_seq            integer not null,
   session_id              text not null,
   start_last_seq          integer not null,
   start_time              timestamptz not null
);
create index ${Destination.TABLE}_entity
   on ${Destination.TABLE} (entity, replicationid, seq desc);

`       ;
        const fullCreate = dropFirst ? drop + create : create;
        return fullCreate;
    }
}

export class RevsQuery {
    revs: boolean;
    open_revs: string[];
    latest: boolean;

    constructor(queryString: string) {
        this.revs = false;
        this.latest = false;
        this.open_revs = [];
        if (queryString) {
            const terms = queryString.split("&");
            for (const term of terms) {
                const split = term.split("=");
                if (split.length != 2) {
                    throw new ReplicationError(
                        `Unexpected query term: ${term}`);
                }
                switch (split[0]) {
                    case "revs":
                        this.revs = (split[1] == "true");
                        break;
                    case "latest":
                        this.latest = (split[1] == "true");
                        break;
                    case "open_revs":
                        this.open_revs =
                            decodeURIComponent(split[1]).
                            replaceAll(/[\[\]\"]/g, "").
                            split(",");
                        break;
                    default:
                        throw new ReplicationError(
                            `Unexpected query parameter: ${split[0]}`);
                }
            }
        }
    }
}

export class ChangesFeedQuery {
    doc_ids?: string;
    conflicts: boolean;
    descending: boolean;
    feed: "normal" | "longpoll" | "continuous" | "eventsource";
    filter?: string;
    heartbeat: number;
    include_docs: boolean;
    attachments: boolean;
    att_encoding_info: boolean;
    last_event_id?: number;
    limit?: number;
    since: string;
    style: "main_only" | "all_docs";
    timeout: number;
    view?: string;
    seq_interval?: number;

    static parseNumber(value: string): number {
        try {
            return Number.parseInt(value);
        } catch (error) {
            throw new ReplicationError(
                `Cannot parse ${value} to a number`, 500, { cause: error });
        }
    }

    constructor(queryString: string) {
        this.conflicts = false;
        this.descending = false;
        this.feed = "normal";
        this.heartbeat = 60000;
        this.include_docs = false;
        this.attachments = false;
        this.att_encoding_info = false;
        this.since = "0";
        this.style = "main_only";
        this.timeout = 60000;
        if (queryString) {
            const terms = queryString.split("&");
            for (const term of terms) {
                const split = term.split("=");
                if (split.length != 2) {
                    throw new ReplicationError(
                        `Unexpected query term: ${term}`);
                }
                switch (split[0]) {
                    case "doc_ids":
                        this.doc_ids = split[1];
                        break;
                    case "conflicts":
                        this.conflicts = (split[1] == "true");
                        break;
                    case "descending":
                        this.descending = (split[1] == "true");
                        break;
                    case "feed":
                        switch (split[1]) {
                            case "normal":
                                this.feed = "normal";
                                break;
                            case "longpoll":
                                this.feed = "longpoll";
                                break;
                            case "continuous":
                                this.feed = "continuous";
                                break;
                            case "eventsource":
                                this.feed = "eventsource";
                                break;
                            default:
                                throw new ReplicationError(
                                    `Invalid value for 'feed': '${split[1]}'`);
                        }
                        break;
                    case "filter":
                        this.filter = split[1];
                        break;
                    case "heartbeat":
                        this.heartbeat = ChangesFeedQuery.parseNumber(split[1]);
                        break;
                    case "include_docs":
                        this.include_docs = (split[1] == "true");
                        break;
                    case "attachments":
                        this.attachments = (split[1] == "true");
                        break;
                    case "att_encoding_info":
                        this.att_encoding_info = (split[1] == "true");
                        break;
                    case "last-event-id":
                        this.last_event_id =
                            ChangesFeedQuery.parseNumber(split[1]);
                        break;
                    case "limit":
                        this.limit = ChangesFeedQuery.parseNumber(split[1]);
                        break;
                    case "since":
                        this.since = split[1];
                        break;
                    case "style":
                        switch (split[1]) {
                            case "main_only":
                                this.style = "main_only";
                                break;
                            case "all_docs":
                                this.style = "all_docs";
                                break;
                            default:
                                throw new ReplicationError(
                                    `Invalid value for 'style': '${split[1]}'`);
                        }
                        break;
                    case "timeout":
                        this.timeout = ChangesFeedQuery.parseNumber(split[1]);
                        break;
                    case "view":
                        this.view = split[1];
                        break;
                    case "seq_interval":
                        this.seq_interval =
                            ChangesFeedQuery.parseNumber(split[1]);
                        break;
                    default:
                        throw new ReplicationError(
                            `Unexpected query parameter: ${split[0]}`);
                }
            }
        }
    }
}

export class ReplicationAdapter extends DaemonWorker implements IAdapter {
    readonly name: string;
    source: Cfg<IReplicableService>;
    entities: Cfg<Map<string, Entity>>;

    constructor(config: TypeCfg<AdapterSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.source = new Cfg(config.spec.source);
        this.entities = new Cfg("entities");
    }

    get isAdapter(): boolean {
        return true;
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        const service: unknown =
            configuration.getSource(this.source.name).service;
        if (!((<any>service).isReplicable)) {
            throw new ReplicationError(
                `Source ${this.source.name} is not a replicable source`);
        }
        this.source.v = <IReplicableService>service;
        this.entities.v = configuration.entities;
    }

    handleGetReplicateRevs(entity: Entity, request: IncomingMessage,
                           response: ServerResponse,
                           uriElements: string[]): void {
        /*
         *     0   1       2          3                4
         * GET r entity    id         ?           openrevs=['']
         *                                        Fetch specific versions
         */
        const id = uriElements[2];
        const multipart =
            getHeader(request.headers, "accept") == "multipart/mixed";
        const revsQuery = new RevsQuery(uriElements[4]);
        const boundary = Entity.generateId().replaceAll("-", "");
        this.source.v.getAllLeafRevs(entity, id, revsQuery, multipart,
                                              boundary)
        .then((result) => {
            if (multipart) {
                response.setHeader("Content-Type",
                        `multipart/mixed; boundary="${boundary}"`);
                return result;
            } else {
                return result;
            }
        })
        .then((msg) => {
            response.end(msg);
        })
        .catch((error) => {
            console.log(error);
            AdapterError.toResponse(error, response);
        });
    }

    handleGetReplicateChanges(entity: Entity, request: IncomingMessage,
                              response: ServerResponse,
                              uriElements: string[]): void {
        /*
         *        0   1       2          3                4
         * GET    r entity _changes      ?         feed=continues&...
         *                                             Changes feed
         */
        const changesQuery = new ChangesFeedQuery(uriElements[4]);
        if (changesQuery.feed == "normal") {
            this.source.v.getChangesNormal(entity, changesQuery)
            .then((feed) => {
                return JSON.stringify(feed);
            })
            .then((msg) => {
                response.end(msg);
            })
            .catch((error) => {
                AdapterError.toResponse(error, response);
            });
        } else {
            throw new ReplicationError("Not yet implemented");
        }
    }

    handleGetReplicate(entity: Entity, request: IncomingMessage,
                       response: ServerResponse, uriElements: string[]): void {
        /* https:/host/
         *             0   1       2          3                4
         *
         * GET         r entity                           Get max(seq) info
         *
         * GET         r entity    id         ?           openrevs=['']
         *                                                  Fetch specific
         *                                                  versions
         * GET         r entity _local   replicationid    Get replication logs
         *
         * GET         r entity _changes      ?           feed=continues&...
         *                                                  Changes feed
         */
        if (uriElements.length == 2) {
            this.source.v.getSequenceId(entity)
            .then((maxseq) => {
                const result = {
                    "instance_start_time": "0",
                    "update_seq": maxseq
                };
                response.end(JSON.stringify(result));
            })
            .catch((error) => {
                AdapterError.toResponse(error, response);
            });
        } else if (uriElements.length == 4 && uriElements[2] == "_local") {
            this.source.v.getReplicationLogs(entity,
                                                      uriElements[3])
            .then((state) => {
                response.end(JSON.stringify(state));
            })
            .catch((error) => {
                AdapterError.toResponse(error, response);
            });
        } else if (uriElements.length == 5 && uriElements[2] == "_changes") {
            this.handleGetReplicateChanges(entity, request, response,
                                           uriElements);
        } else if (uriElements.length == 5 && uriElements[3] == "?") {
            this.handleGetReplicateRevs(entity, request, response, uriElements);
        } else {
            throw new ReplicationError(`Invalid request: invalid URI ` +
                                      `components for Replicate GET`);
        }
    }

    handleRevsDiff(entity: Entity, payload: string): Promise<RevsDiffResponse> {
        const diffRequest = JSON.parse(payload) as RevsDiffRequest;
        return this.source.v.getRevsDiffRequest(entity, diffRequest);
    }

    handleBulkDocs(entity: Entity,
                   payload: string): Promise<ReplicationResponse[]> {
        const docs = JSON.parse(payload) as BulkDocsRequest;
        return this.source.v.postBulkDocs(entity, docs);
    }

    handlePostReplicate(entity: Entity, request: IncomingMessage,
                        response: ServerResponse, uriElements: string[]): void {
        /* https:/host/
         *             0   1       2
         * POST        r entity _revs_diff     Calculate Revision Difference
         * POST        r entity _bulk_docs     Upload Batch of Documents
         */
        if (uriElements.length != 3 || (uriElements[2] != "_revs_diff" &&
               uriElements[2] != "_bulk_docs")) {
            throw new ReplicationError(`Invalid request: invalid URI ` +
                                      `components for replicate POST`);
        }
        const chunks: Uint8Array[] = [];
        request.on("data", (chunk) => {
            chunks.push(chunk);
        });
        request.on("end", () => {
            try {
                const payload = Buffer.concat(chunks).toString();
                let processorResult: Promise<any>;
                if (uriElements[2] == "_revs_diff") {
                    processorResult = this.handleRevsDiff(entity, payload);
                } else {
                    processorResult = this.handleBulkDocs(entity, payload);
                }
                processorResult
                .then((response) => {
                    return JSON.stringify(response);
                })
                .then((msg) => {
                    response.end(msg);
                })
                .catch((error) => {
                    AdapterError.toResponse(error, response);
                });
            } catch (error) {
                AdapterError.toResponse(error, response);
            }
        });
        request.on("error", (error) => {
            AdapterError.toResponse(error, response);
        });
    }

    handlePutReplicate(entity: Entity, request: IncomingMessage,
                       response: ServerResponse, uriElements: string[]): void {
        /* https:/host/
         *             0   1       2        3
         * PUT         r entity _local replicationid    Insert replication log
         */
        if (uriElements.length == 4 && uriElements[2] == "_local") {
            const destination = new Destination(entity);
            const chunks: Uint8Array[] = [];
            request.on("data", (chunk) => {
                chunks.push(chunk);
            });
            request.on("end", () => {
                try {
                    const payload = Buffer.concat(chunks).toString();
                    const repState = JSON.parse(payload) as ReplicationState;
                    this.source.v.putReplicationState(
                        destination, repState)
                    .then((response) => {
                        return JSON.stringify(response);
                    })
                    .then((msg) => {
                        response.end(msg);
                    })
                    .catch((error) => {
                        AdapterError.toResponse(error, response);
                    });
                } catch (error) {
                    AdapterError.toResponse(error, response);
                }
            });
            request.on("error", (error) => {
                AdapterError.toResponse(error, response);
            });
        } else {
            throw new ReplicationError(`Invalid request: invalid URI ` +
                                      `components for replicate PUT`);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        if (uriElements.length <= 1) {
            throw new ReplicationError("Missing entity in request", 400);
        }
        const entityName = uriElements[1];
        const entity = this.entities.v.get(entityName);
        if (!entity) {
            throw new ReplicationError(`Invalid entity: ${entityName}`, 404);
        }
        switch (request.method) {
            case "HEAD":
                response.end();
                return;
            case "GET":
                return this.handleGetReplicate(
                    entity, request, response, uriElements);
            case "POST":
                return this.handlePostReplicate(
                    entity, request, response, uriElements);
            case "PUT":
                return this.handlePutReplicate(
                    entity, request, response, uriElements);
            default:
                throw new ReplicationError(
                    `Invalid Replicate request: ${request.method}`);
        }
    }
}

