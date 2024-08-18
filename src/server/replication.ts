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
    _IError, IService, JsonObject, Entity, Row, Cfg, TypeCfg, IConfiguration,
    Persona, State, IContext, Logger
} from "../base/core.js";

import { NOCONTEXT } from "../base/configuration.js";

import { SessionContext } from "../base/session.js";

import {
    AdapterError, getHeader, SessionAwareAdapter, SessionAwareAdapterSpec
} from "./adapter.js";

import { rzoAuthenticate } from "./authentication.js";


class ReplicationError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export interface IReplicableService extends IService {
    get isReplicable(): boolean;
    createInMemorySession(logger: Logger, userId: string, expiryOverride?: Date,
                          personaOverride?: Persona): Promise<State>;
    getReplicationState(logger: Logger, entity: Entity,
                       id: string): Promise<JsonObject | null>;
    putReplicationState(logger: Logger, entity: Entity, id: string,
                        repState: JsonObject): Promise<ReplicationResponse>;
    getChangesNormal(logger: Logger, entity: Entity,
                     query: ChangesFeedQuery): Promise<NormalChangeFeed>;
    getRevsDiffRequest(logger: Logger, entity: Entity,
                       diffRequest: RevsDiffRequest): Promise<RevsDiffResponse>;
    getAllLeafRevs(logger: Logger, entity: Entity, id: string, query: RevsQuery,
                   multipart: boolean, boundary?: string): Promise<string>;
    postBulkGet(logger: Logger, entity: Entity, request: BulkGetRequest,
                query: RevsQuery): Promise<BulkGetResponse>;
    postBulkDocs(logger: Logger, entity: Entity,
                 docsRequest: BulkDocsRequest): Promise<ReplicationResponse[]>;
}

export type BulkGetRequest = {
    docs: [
        { id: string; rev?: string; }
    ];
}

type BulkGetError = {
    id: string;
    rev: string;
    error: string;
    reason: string;
}

type BulkGetResult = {
    ok?: JsonObject;
    error?: BulkGetError;
}

export type BulkGetResponseObj = {
    id: string;
    docs: BulkGetResult[];
}

export type BulkGetResponse = {
    results: BulkGetResponseObj[];
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
}

export type RevsDiffResponse = {
    [name: string]: {
        missing: string[];
    };
}

export type BulkDocsRequest = {
    docs: JsonObject[];
    new_edits: boolean;
}

export type PurgedDoc = {
    _id: string;
    _rev: string;
    purged_hash: string;
}

type CouchServerVendor = {
    name: string;
    version: string;
}

type CouchServerInfo = {
    couchdb: string;
    uuid: string;
    vendor: CouchServerVendor;
    version: string;
}

export const STATE_TABLE = "local_replication";

export function stateTableDDL(dropFirst?: boolean): string {
    const drop   = `
drop table if exists ${STATE_TABLE};
`   ;
    const create = `
create table ${STATE_TABLE} (
   seq                     bigserial primary key,
   entity                  varchar(46) not null,
   _id                     text not null,
   _rev                    varchar(43) not null,
   contents                jsonb not null
);
create index ${STATE_TABLE}_entity
   on ${STATE_TABLE} (entity, _id);

`   ;
    const fullCreate = dropFirst ? drop + create : create;
    return fullCreate;
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

export class ReplicationAdapter extends SessionAwareAdapter {
    entities: Cfg<Map<string, Entity>>;
    loginEntity: Cfg<Entity>;
    replSource: Cfg<IReplicableService>;

    constructor(config: TypeCfg<SessionAwareAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.entities = new Cfg("entities");
        this.loginEntity = new Cfg("loginentity");
        this.replSource = new Cfg(config.spec.source);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        const service: unknown = this.source.v;
        if (!((<any>service).isReplicable)) {
            throw new ReplicationError(
                `Source ${this.replSource.name} is not a replicable source`);
        }
        this.replSource.v = <IReplicableService>service;
        this.entities.v = configuration.entities;
        this.loginEntity.v = configuration.getEntity("login");
    }

    async handleGetReplicateRevs(entity: Entity, request: IncomingMessage,
                                 response: ServerResponse,
                                 uriElements: string[]): Promise<void> {
        /*
         *     0   1       2          3                4
         * GET r entity    id                     Fetch lastest version
         * GET r entity    id         ?           openrevs=['']
         *                                        Fetch specific versions
         */
        const id = decodeURIComponent(uriElements[2]);
        const multipart =
            getHeader(request.headers, "accept") == "multipart/mixed";
        const revsQuery =
            new RevsQuery(uriElements.length > 4 ? uriElements[4] : "");
        const boundary = multipart ?
            Entity.generateId().replaceAll("-", "") : "";
        const result = await this.replSource.v.getAllLeafRevs(
            this.logger, entity, id, revsQuery, multipart, boundary);
        if (this.logger.willLog("Debug")) {
            if (multipart) {
                this.logger.debug("multipart response requested");
            }
            this.logger.debug(result);
        }
        if (multipart) {
            response.setHeader(
                "Content-Type", `multipart/mixed; boundary="${boundary}"`);
        }
        response.end(result);
    }

    async handleGetReplicateChanges(context: IContext, entity: Entity,
                                    request: IncomingMessage,
                                    response: ServerResponse,
                                    uriElements: string[]): Promise<void> {
        /*
         *        0   1       2          3                4
         * GET    r entity _changes      ?         feed=continues&...
         *                                             Changes feed
         */
        const changesQuery = new ChangesFeedQuery(uriElements[4]);
        if (changesQuery.feed == "normal") {
            const feed = await this.replSource.v.getChangesNormal(
                this.logger, entity, changesQuery);
            response.end(JSON.stringify(feed));
        } else {
            throw new ReplicationError("Not yet implemented");
        }
    }

    async handleGetReplicate(entityName: string, request: IncomingMessage,
                             response: ServerResponse,
                             uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0   1       2          3                4
         *
         * GET         r entity                           Get max(seq) info
         *
         * GET         r entity    id                     Get lastest version
         *
         * GET         r entity    id         ?           openrevs=['']
         *                                                  Fetch specific
         *                                                  versions
         *
         * GET         r entity _local        id          Get replication logs
         *
         * GET         r entity _changes      ?           feed=continues&...
         *                                                  Changes feed
         */
        try {
            const context = await this.authenticate(request);
            const entity = this.entities.v.get(entityName);
            if (!entity) {
                throw new ReplicationError(
                    `Invalid entity: ${entityName}`, 404);
            }
            const resource = `entity/${entity.name}`;
            this.policyConfig.v.guardResource(context, resource, "get");
            if (uriElements.length == 2) {
                const maxseq = await this.replSource.v.getSequenceId(
                    this.logger, context, entity);
                const result = {
                    "instance_start_time": "0",
                    "update_seq": maxseq
                };
                response.end(JSON.stringify(result));
            } else if (uriElements.length == 4 && uriElements[2] == "_local") {
                const state = await this.replSource.v.getReplicationState(
                    this.logger, entity, decodeURIComponent(uriElements[3]))
                if (state) {
                    response.end(JSON.stringify(state));
                } else {
                    response.statusCode = 404;
                    response.end(JSON.stringify({
                        "error": "not_found",
                        "reason": "missing"
                    }));
                }
            } else if (uriElements.length == 5 &&
                       uriElements[2] == "_changes") {
                await this.handleGetReplicateChanges(
                    context, entity, request, response, uriElements);
            } else if (uriElements.length == 3 ||
                       (uriElements.length == 5 && uriElements[3] == "?")) {
                await this.handleGetReplicateRevs(
                    entity, request, response, uriElements);
            } else {
                throw new ReplicationError(
                    `Invalid request: invalid URI components for ` +
                    `Replicate GET`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    async handleBulkGet(entity: Entity, payload: JsonObject,
                        response: ServerResponse,
                        uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0   1       2        3            4
         * POST        r entity _bulk_get   ?   revs=true&latest=true
         *                                        PouchDB calls this instead
         *                                        of the Multipart API, i.e.
         *                                        GetReplicateRevs()
         */
        const revsQuery = new RevsQuery(
            uriElements.length >= 5 ? uriElements[4] : "");
        const bulk = await this.replSource.v.postBulkGet(
            this.logger, entity, payload as BulkGetRequest, revsQuery);
        if (this.logger.willLog("Info")) {
            this.logger.info("Replication _bulk_get outbound:");
            for (const obj of bulk.results) {
                const id = obj.id;
                for (const doc of obj.docs) {
                    if (doc.ok) {
                        this.logger.info(
                            `OK Entity ${entity.name} id = ${id} ; rev = ` +
                            `${doc.ok["_rev"]}`);
                    }
                    if (doc.error) {
                        this.logger.info(
                            `ERROR Entity ${entity.name} id = ${id} ; rev = ` +
                            `${doc.error.rev} ; reason = ${doc.error.reason}`);
                    }
                }
            }
        }
        const result = JSON.stringify(bulk);
        if (this.logger.willLog("Debug")) {
            this.logger.debug(result);
        }
        response.end(result);
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
                                   id?: string): Promise<void> {
        /* https:/host/
         *             0   1       2
         * POST        r entity _revs_diff     Calculate Revision Difference
         * POST        r entity _bulk_docs     Upload Batch of Documents
         *
         *             0   1       2        3            4
         * POST        r entity _bulk_get   ?   revs=true&latest=true
         *                                        PouchDB calls this instead
         *                                        of the Multipart API, i.e.
         *                                        GetReplicateRevs()
         *
         *             0   1       2        3
         * PUT         r entity _local replicationid    Insert replication log
         *                      ^ not passed in
         */
        await this.authenticate(request);
        if (this.logger.willLog("Debug")) {
            this.logger.debug(JSON.stringify(payload));
        }
        if (!resource || !id) {
            throw new ReplicationError(
                "Missing resource and/or id for ReplicationAdapter");
        }
        const entity = this.entities.v.get(resource!);
        if (!entity) {
            throw new ReplicationError(`Invalid entity: ${resource}`, 404);
        }
        if (request.method == "POST") {
            if (id == "_revs_diff") {
                const diffResponse = await this.replSource.v.getRevsDiffRequest(
                    this.logger, entity, payload as RevsDiffRequest);
                response.end(JSON.stringify(diffResponse));
            } else if (id == "_bulk_docs") {
                const bulkResponse = await this.replSource.v.postBulkDocs(
                    this.logger, entity, payload as BulkDocsRequest);
                response.end(JSON.stringify(bulkResponse));
            } else if (id == "_bulk_get") {
                await this.handleBulkGet(
                    entity, payload, response, uriElements);
            } else {
                throw new ReplicationError(
                    "ReplicationAdapter POST must be either _revs_diff or " +
                    "_bulk_docs");
            }
        } else if (request.method == "PUT") {
            const repLogResponse = await this.replSource.v.putReplicationState(
                        this.logger, entity, id, payload);
            response.end(JSON.stringify(repLogResponse));
        } else {
            // This should never happen
            response.end();
        }
    }

    protected async authenticate(
        request: IncomingMessage): Promise<SessionContext> {
        const authHeader = getHeader(request.headers, "Authorization");
        if (!authHeader) {
            console.log(
                `Replication auth failed: no Authorization header present`);
            throw new ReplicationError("Unauthorized", 401);
        }
        if (!authHeader.toUpperCase().startsWith("BASIC ")) {
            console.log(
                `Replication auth failed: Authorization header ` +
                `[${authHeader}] does not start with 'Basic' ` +
                `(case insensitive)`);
            throw new ReplicationError("Unauthorized", 401);
        }
        const authBasic: Cfg<string[]> = new Cfg("basic");
        try {
            const authB64 = authHeader.substring("BASIC ".length);
            authBasic.v = atob(authB64).split(":", 2);
        } catch (error) {
            console.log(
                `Replication auth failed: Authorization header ` +
                `[${authHeader}] holds an invalid Base64 value`);
            console.log(error);
            throw new ReplicationError("Unauthorized", 401);
        }
        const userName = authBasic.v[0];
        // Check the cache for this username
        const cached = this.sessionCache.v.get(userName);
        if (cached) {
            // Refresh the timeout for the cached value
            this.sessionCache.v.set(userName, cached);
            return cached;
        }
        const row = new Row(
            { "username": authBasic.v[0], "password": authBasic.v[1] });

       const userId = await rzoAuthenticate(
           this.logger, this.replSource.v, this.loginEntity.v, row);
        /* The session backend will persist this API session, but since we
         * can not track the sessionId, we force a short expiry on this
         * persistent session and rely on the local cache instead.
         * This means that every (load-balanced) server will create its
         * own session and track it independently.
         */
        const sessionRow = await this.sessionBackend.v.createSession(
            this.logger, userId, new Date(Date.now() + 30000));

        const personaName = sessionRow.get("persona");
        const persona = this.personas.v.get(personaName);
        if (!persona) {
            throw new ReplicationError(
                `Invalid persona: ${personaName}`, 403);
        }

        const sessionContext = new SessionContext(sessionRow, persona);
        this.sessionCache.v.set(userName, sessionContext);
        return sessionContext;
    }

    async handleServerInfo(response: ServerResponse): Promise<void> {
        try {
            const row = await this.replSource.v.getDBInfo(
                this.logger, NOCONTEXT);
            const couchServerInfo: CouchServerInfo = {
                couchdb: "Welcome",
                uuid: row.get("uuid"),
                vendor: { name: "RZO", version: row.get("version") },
                version: row.get("version")
            };
            response.end(JSON.stringify(couchServerInfo));
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    decodeOptionalURIComponent(input?: string) {
        if (input !== undefined) {
            return decodeURIComponent(input);
        } else {
            return input;
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        this.logger.info(`${request.method} - ${request.url}`);
        try {
            if (uriElements.length <= 1) {
                this.handleServerInfo(response);
                return;
            }
            const entityName = uriElements[1];
            if (request.method == "HEAD") {
                const entity = this.entities.v.get(entityName);
                if (!entity) {
                    throw new ReplicationError(
                        `Invalid entity: ${entityName}`, 404);
                }
                this.logger.log(`Verify Peer '${entityName}'`);
                response.end();
            } else {
                switch (request.method) {
                    case "GET":
                        this.handleGetReplicate(
                            entityName, request, response, uriElements);
                        break;
                    case "POST":
                        this.handlePayload(
                            request, response, uriElements, entityName,
                            this.decodeOptionalURIComponent(
                                uriElements.at(2)));
                        break;
                    case "PUT":
                        if (uriElements.length < 4 ||
                            uriElements.at(2) != "_local") {
                            throw new ReplicationError(
                                "Invalid Replication PUT uri");
                        }
                        this.handlePayload(
                            request, response, uriElements, entityName,
                            this.decodeOptionalURIComponent(
                                uriElements.at(3)));
                        break;
                    default:
                        throw new ReplicationError(
                            `Invalid Replicate request: ${request.method}`);
                }
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }
}

