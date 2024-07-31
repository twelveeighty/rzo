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

import { _IError, IService, JsonObject, Entity, Row } from "../base/core.js";

class ReplicationError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export interface IReplicableService extends IService {
    get replicable(): boolean;
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

