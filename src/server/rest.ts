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

import {
    IncomingMessage, IncomingHttpHeaders, Server, ServerResponse,
    createServer
} from "http";

import {
    ClassSpec, TypeCfg, Entity, Row, Source, IService, IConfiguration,
    Filter, Query, OrderBy, DaemonWorker, IResultSet,
    DeferredToken, Nobody, _IError, Cfg, ICache, IPolicyConfiguration
} from "../base/core.js";

import {
    Destination, ReplicationState, ChangesFeedQuery, RevsDiffRequest,
    RevsDiffResponse, RevsQuery, IReplicableService, BulkDocsRequest,
    ReplicationResponse
} from "./replication.js";

import { SessionContext, ISessionBackendService } from "./session.js";

type ErrorType = {
    type: string;
    message: string;
    causeType?: string;
    cause?: string;
}

class RestServerError extends _IError {

    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }

    static toResponse(exc: unknown, response: ServerResponse): void {
        let msg;
        let statusCode = 500;
        if (exc instanceof _IError) {
            const error = <_IError>exc;
            statusCode = error.code;
            const type = error.name;
            const message = error.message;
            if (error.cause && error.cause instanceof Error) {
                const cause = <Error>error.cause;
                const causeType = cause.name;
                const causeMsg = cause.message;
                msg = RestServer.toRestError(statusCode, type, message,
                                             causeType, causeMsg);
            } else {
                msg = RestServer.toRestError(statusCode, type, message);
            }
        } else if (exc instanceof Error) {
            const error = <Error>exc;
            const type = error.name;
            const message = error.message;
            if (error.cause && error.cause instanceof Error) {
                const cause = <Error>error.cause;
                const causeType = cause.name;
                const causeMsg = cause.message;
                msg = RestServer.toRestError(statusCode, type, message,
                                             causeType, causeMsg);
            } else {
                msg = RestServer.toRestError(statusCode, type, message);
            }
        } else {
            msg = RestServer.toRestError(statusCode, typeof exc, `${exc}`);
        }
        console.log(msg);
        console.log(exc);
        response.statusCode = statusCode;
        response.write(msg);
    }
}

class RestServer {
    httpServer: Server;
    source: IService;
    sessionBackend: ISessionBackendService;
    config: IConfiguration;
    policyConfig: IPolicyConfiguration;
    userEntity: Entity;
    sessionEntity: Entity;
    sessionCache: ICache;

    static DEFAULT_MAX_ROWS = 100;
    static fieldRegEx = /^f=(\*|[A-Za-z0-9_,\*\(\)]+)/;
    static orderRegEx = /\&o=([-+A-Za-z0-9_,]+)/;

    static toRestError(statusCode: number, type: string, message:string,
                       causeType?: string, cause?: string): string {

        const err: ErrorType = {
            type: type,
            message: message,
            causeType: causeType,
            cause: cause
        };
        return JSON.stringify(err);
    }

    static respondWithRestError(response: ServerResponse, statusCode: number,
                                type: string, message:string,
                                causeType?: string, cause?: string): void {
        response.statusCode = statusCode;
        response.end(RestServer.toRestError(statusCode, type, message));
    }

    static stringToQuery(input?: string): Query {
        if (input) {
            /* Since the where clause must always be last, and it would be
             * the only possible legal source of single / double quotes,
             * remove everything following the first quote (if any) from the
             * string to be parsed for fields and order-by.
             */
            const firstQuote = input.search(/['"]/);
            const safeInput = firstQuote != -1 ? input.substring(0, firstQuote)
                                               : input;
            let fields: string[] = [];
            let nextParam = "";
            let match = input.match(RestServer.fieldRegEx);
            if (match) {
                fields = match[1].split(",").filter((field) => {
                    return Query.isValidField(field);
                });
                nextParam = "&";
            }
            let wherePos = safeInput.indexOf(nextParam + Filter.QueryAnd);
            if (wherePos < 0) {
                wherePos = safeInput.indexOf(nextParam + Filter.QueryOr);
            }
            const orderStart = "&o=";
            const orderPos = safeInput.indexOf(orderStart);
            if (orderPos >= 0 && wherePos >= 0 && wherePos < orderPos) {
                throw new RestServerError(
                    `where clause must not precede order-by clause: ${input}`);
            }
            const orderBy: OrderBy[] = [];
            if (orderPos >= 0) {
                match = safeInput.match(RestServer.orderRegEx);
                if (!match) {
                    throw new RestServerError(
                        `Cannot parse order by clause in ${input}`);
                }
                const orderBys = match[1].trim().split(",").filter((field) => {
                    return !!field;
                });
                orderBys.forEach((element) => {
                    if (element.length < 2) {
                        throw new RestServerError(`Invalid order-by element ` +
                                            `in ${input}`);
                    }
                    const orderByField = element.slice(0, -1);
                    if (element.endsWith("+")) {
                        orderBy.push({ field: orderByField, order: "asc" });
                    } else if (element.endsWith("-")) {
                        orderBy.push({ field: orderByField, order: "desc" });
                    } else {
                        throw new RestServerError(`Invalid direction(s) in ` +
                                            `order-by clause: ${input}`);
                    }
                });
            }
            if (wherePos >= 0) {
                const filter = new Filter();
                filter.parseParameters(input.substring(wherePos));
                return new Query(fields, filter, orderBy);
            } else {
                return new Query(fields, undefined, orderBy);
            }
        }
        return new Query();
    }

    constructor(source: IService, sessionBackend: ISessionBackendService,
                config: IConfiguration, sessionCache: ICache) {
        this.source = source;
        this.sessionBackend = sessionBackend;
        this.userEntity = config.getEntity("useraccount");
        this.sessionEntity = config.getEntity("session");
        this.config = config;
        this.policyConfig = config.policyConfig!;
        this.sessionCache = sessionCache;
        this.httpServer = createServer((request, response) => {
            this.handle(request, response);
        });
    }

    get replicationSource(): IReplicableService {
        if ((<any>this.source).replicable) {
            return this.source as IReplicableService;
        } else {
            throw new RestServerError(
                `No replicable source is available for this server`);
        }
    }

    async pullContext(request: IncomingMessage): Promise<SessionContext> {
        const sessionId = this.getHeader(request.headers, "rzo-sessionid");
        if (!sessionId) {
            throw new RestServerError("Missing 'rzo-sessionid'");
        }
        let context: SessionContext = this.sessionCache.get(sessionId);
        // Check the expiry on the cached session
        if (context && context.expiry.getTime() <= Date.now()) {
            console.log(
                `cached sessionContext expired: ${context.expiry}`);
            this.sessionCache.delete(sessionId);
            // Session expired, delete it, no need to 'await' it.
            this.sessionBackend.deleteSession(sessionId);
            throw new RestServerError("Session expired", 401);
        }
        if (!context) {
            const row = await this.sessionBackend.getSessionContext(sessionId);
            const personaName = row.get("persona");
            const persona = this.config.personas.get(personaName);
            if (!persona) {
                throw new RestServerError(
                    `Invalid persona: ${personaName}`, 403);
            }
            return new SessionContext(row, persona);
        }
        return context;
    }

    async delete(request: IncomingMessage, response: ServerResponse,
                 entity: Entity, id: string, version: string) : Promise<void> {
        try {
            const resource = `entity/${entity.name}`;
            const context = await this.pullContext(request);
            this.policyConfig.guardResource(context, resource, "delete");
            const row = await this.source.getOne(entity, id, version);
            if (row && !row.empty) {
                this.policyConfig.guardRow(context, resource, "delete", row);
                await this.source.delete(entity, id, version, context);
                response.end(`{"id": "${id}}"`);
            } else {
                RestServer.respondWithRestError(
                    response, 404, "NotFound", `${entity.name} : ${id}`);
            }
        } catch(error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    post(request: IncomingMessage, response: ServerResponse,
         entity: Entity) : void {
        this.handlePayload(request, response, entity);
    }

    put(request: IncomingMessage, response: ServerResponse, entity: Entity,
           id: string) : void {
        this.handlePayload(request, response, entity, id);
    }

    private async processPayload(request: IncomingMessage,
                                 response: ServerResponse,
                                 payload: string,
                                 entity: Entity, id?: string): Promise<void> {
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${entity.name}`;
            const action = (id !== undefined) ? "put" : "post";
            this.policyConfig.guardResource(context, resource, action);
            const parsed = JSON.parse(payload);
            let json_obj: any;
            if (parsed instanceof Array) {
                json_obj = (<any[]>parsed)[0];
            } else {
                json_obj = parsed;
            }
            const row = Row.dataToRow(json_obj, entity);
            if (row && !row.empty) {
                this.policyConfig.guardRow(context, resource, action, row);
                let output: Row;
                if (id) {
                    output = await this.source.put(entity, id, row, context);
                } else {
                    output = await this.source.post(entity, row, context);
                }
                response.end(JSON.stringify(Row.rowToData(output)));
            } else {
                throw new RestServerError("Cannot parse payload");
            }
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    handlePayload(request: IncomingMessage, response: ServerResponse,
                  entity: Entity, id?: string): void {
        const chunks: Uint8Array[] = [];
        request.on("data", (chunk) => {
            chunks.push(chunk);
        });
        request.on("end", () => {
            const payload = Buffer.concat(chunks).toString();
            this.processPayload(request, response, payload, entity, id);
        });
        request.on("error", (error) => {
            RestServerError.toResponse(error, response);
            response.end();
        });
    }

    async getOne(request: IncomingMessage, response: ServerResponse,
                 entity: Entity, id: string, version?: string) : Promise<void> {
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${entity.name}`;
            this.policyConfig.guardResource(context, resource, "get");
            const row = await this.source.getOne(entity, id, version);
            if (row && !row.empty) {
                this.policyConfig.guardRow(context, resource, "get", row);
                response.end(JSON.stringify(Row.rowToData(row)));
            } else {
                RestServer.respondWithRestError(
                    response, 404, "NotFound", `${entity.name} : ${id}`);
            }
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    async getQuery(request: IncomingMessage, response: ServerResponse,
                   entity: Entity, queryStr: string): Promise<void> {

        console.log(`Entity: ${entity.name}; query: ${queryStr}`);
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${entity.name}`;
            this.policyConfig.guardResource(context, resource, "get");
            const query = RestServer.stringToQuery(queryStr);
            const resultSet = await this.source.getQuery(entity, query);
            this.policyConfig.guardResultSet(context, resource, resultSet);
            resultSet.rewind();
            const result: any[] = [];
            while (resultSet.next()) {
                const row = resultSet.getRow();
                result.push(Row.rowToData(row));
            }
            response.end(JSON.stringify(result));
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    getHeader(headers: IncomingHttpHeaders, key: string): string | undefined {
        const value = headers[key];
        if (value) {
            if (Array.isArray(value)) {
                return value[0];
            }
            return value;
        } else {
            return undefined;
        }
    }

    async handleQueryOne(request: IncomingMessage, response: ServerResponse,
                         uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0   1      2     3
         *             o entity
         *             o entity   ?   filter
         *
         */
        if (request.method != "GET") {
            throw new RestServerError(`Invalid request method ` +
                                      `'${request.method}' for QueryOne`);
        }
        if (uriElements.length != 4) {
            throw new RestServerError("Malformed QueryOne request");
        }
        const entity = this.config.entities.get(uriElements[1]);
        if (!entity) {
            throw new RestServerError(`Invalid entity: ${uriElements[1]}`);
        }
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${entity.name}`;
            this.policyConfig.guardResource(context, resource, "get");
            const filter = new Filter();
            filter.parseParameters(decodeURIComponent(uriElements[3]));
            const row = await this.source.getQueryOne(entity, filter);
            if (row && !row.empty) {
                this.policyConfig.guardRow(context, resource, "get", row);
                response.end(JSON.stringify(Row.rowToData(row)));
            } else {
                RestServer.respondWithRestError(
                    response, 404, "NotFound", `${entity.name}`);
            }
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    async handleCollection(request: IncomingMessage, response: ServerResponse,
                           uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0     1       2       3
         *             c collection
         *             c collection  ?     q=filter
         *             c collection  ?     f=fields&q=filter
         */
        if (request.method != "GET") {
            throw new RestServerError(
                `Invalid request method '${request.method}' for collection ` +
                `queries`);
        }
        if (uriElements.length <= 1) {
            throw new RestServerError("Missing collection in request");
        }
        const collectionName = uriElements[1];
        const collection = this.config.collections.get(collectionName);
        if (!collection) {
            throw new RestServerError(`Invalid collection: ${collectionName}`);
        }
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${collection.entity.v.name}`;
            this.policyConfig.guardResource(context, resource, "get");
            let resultSet: IResultSet;
            if (uriElements.length > 3) {
                const queryStr = decodeURIComponent(uriElements[3]);
                console.log(`Collection: ${collectionName}; query: ${queryStr}`);
                const query = RestServer.stringToQuery(queryStr);
                resultSet = await collection.query(context, query);
            } else {
                resultSet = await collection.query(context);
            }
            this.policyConfig.guardResultSet(context, resource, resultSet);
            resultSet.rewind();
            const result: any[] = [];
            while (resultSet.next()) {
                const row = resultSet.getRow();
                result.push(Row.rowToData(row));
            }
            response.end(JSON.stringify(result));
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    async handleGenerator(request: IncomingMessage, response: ServerResponse,
                          uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0   1
         *             g sequence
         */
        if (uriElements.length != 2) {
            throw new RestServerError("Invalid generator request");
        }
        try {
            await this.pullContext(request);
            const nextval = await this.source.getGeneratorNext(uriElements[1]);
            response.end(JSON.stringify({ "nextval": nextval }));
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    async handleGetEntity(entity: Entity, request: IncomingMessage,
                          response: ServerResponse,
                          uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0   1     2     3         4
         * GET         e entity                         Get _vc max(seq) info
         * GET         e entity uuid                    Get winning version
         * GET         e entity uuid   ?     rev=1-xxx  Get specific version
         * GET         e entity  ?   filter             Query
         */
        if (uriElements.length == 2) {
            try {
                const context = await this.pullContext(request);
                const resource = `entity/${entity.name}`;
                this.policyConfig.guardResource(context, resource, "get");
                const maxseq = await this.source.getSequenceId(entity);
                const result = {
                    "instance_start_time": "0",
                    "update_seq": maxseq
                };
                response.end(JSON.stringify(result));
            } catch (error) {
                RestServerError.toResponse(error, response);
                response.end();
            }
        } else if (uriElements.length == 3) {
            this.getOne(request, response, entity, uriElements[2]);
        } else if (uriElements.length == 4 && uriElements[2] == "?") {
            const query = decodeURIComponent(uriElements[3]);
            this.getQuery(request, response, entity, query);
        } else if (uriElements.length == 5 && uriElements[3] == "?" &&
                  uriElements[4].startsWith("rev=")) {
            const version = uriElements[4].substring("rev=".length);
            this.getOne(request, response, entity, uriElements[2],
                               version);
        } else {
            throw new RestServerError(`Invalid request: invalid URI ` +
                                      `components for GET`);
        }
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
            this.getHeader(request.headers, "accept") == "multipart/mixed";
        const revsQuery = new RevsQuery(uriElements[4]);
        const boundary = Entity.generateId().replaceAll("-", "");
        this.replicationSource.getAllLeafRevs(entity, id, revsQuery, multipart,
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
            RestServerError.toResponse(error, response);
            response.end();
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
            this.replicationSource.getChangesNormal(entity, changesQuery)
            .then((feed) => {
                return JSON.stringify(feed);
            })
            .then((msg) => {
                response.end(msg);
            })
            .catch((error) => {
                RestServerError.toResponse(error, response);
                response.end();
            });
        } else {
            throw new RestServerError("Not yet implemented");
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
            this.source.getSequenceId(entity)
            .then((maxseq) => {
                const result = {
                    "instance_start_time": "0",
                    "update_seq": maxseq
                };
                response.end(JSON.stringify(result));
            })
            .catch((error) => {
                RestServerError.toResponse(error, response);
                response.end();
            });
        } else if (uriElements.length == 4 && uriElements[2] == "_local") {
            this.replicationSource.getReplicationLogs(entity,
                                                      uriElements[3])
            .then((state) => {
                response.end(JSON.stringify(state));
            })
            .catch((error) => {
                RestServerError.toResponse(error, response);
                response.end();
            });
        } else if (uriElements.length == 5 && uriElements[2] == "_changes") {
            this.handleGetReplicateChanges(entity, request, response,
                                           uriElements);
        } else if (uriElements.length == 5 && uriElements[3] == "?") {
            this.handleGetReplicateRevs(entity, request, response, uriElements);
        } else {
            throw new RestServerError(`Invalid request: invalid URI ` +
                                      `components for Replicate GET`);
        }
    }

    handleRevsDiff(entity: Entity, payload: string): Promise<RevsDiffResponse> {
        const diffRequest = JSON.parse(payload) as RevsDiffRequest;
        return this.replicationSource.getRevsDiffRequest(entity, diffRequest);
    }

    handleBulkDocs(entity: Entity,
                   payload: string): Promise<ReplicationResponse[]> {
        const docs = JSON.parse(payload) as BulkDocsRequest;
        return this.replicationSource.postBulkDocs(entity, docs);
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
            throw new RestServerError(`Invalid request: invalid URI ` +
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
                    RestServerError.toResponse(error, response);
                    response.end();
                });
            } catch (error) {
                RestServerError.toResponse(error, response);
                response.end();
            }
        });
        request.on("error", (error) => {
            RestServerError.toResponse(error, response);
            response.end();
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
                    this.replicationSource.putReplicationState(
                        destination, repState)
                    .then((response) => {
                        return JSON.stringify(response);
                    })
                    .then((msg) => {
                        response.end(msg);
                    })
                    .catch((error) => {
                        RestServerError.toResponse(error, response);
                        response.end();
                    });
                } catch (error) {
                    RestServerError.toResponse(error, response);
                    response.end();
                }
            });
            request.on("error", (error) => {
                RestServerError.toResponse(error, response);
                response.end();
            });
        } else {
            throw new RestServerError(`Invalid request: invalid URI ` +
                                      `components for replicate PUT`);
        }
    }

    handleDeleteEntity(entity: Entity, request: IncomingMessage,
                    response: ServerResponse, uriElements: string[]): void {
        /* https:/host/
         *             0   1     2     3         4
         * DELETE      e entity uuid   ?     rev=1-xxx  Delete
         */
        if (uriElements.length == 5 && uriElements[3] == "?" &&
                  uriElements[4].startsWith("rev=")) {
            const version = uriElements[4].substring("rev=".length);
            this.delete(request, response, entity, uriElements[2], version);
        } else {
            throw new RestServerError(`Invalid request: invalid URI ` +
                                      `components for DELETE`);
        }
    }

    handlePutEntity(entity: Entity, request: IncomingMessage,
                    response: ServerResponse, uriElements: string[]): void {
        /* https:/host/
         *             0   1     2
         * PUT         e entity uuid      Update: version must be part of body
         */
        if (uriElements.length == 3) {
            this.put(request, response, entity, uriElements[2]);
        } else {
            throw new RestServerError(`Invalid request: invalid URI ` +
                                      `components for PUT`);
        }
    }

    protected async createSession(userId: string,
                                  response: ServerResponse): Promise<void> {
        const row =
            await this.sessionBackend.createSessionContext(userId);

        const personaName = row.get("persona");
        const persona = this.config.personas.get(personaName);
        if (!persona) {
            throw new RestServerError(
                `Invalid persona: ${personaName}`, 403);
        }

        const sessionContext = new SessionContext(row, persona);
        this.sessionCache.set(sessionContext.sessionId, sessionContext);
        response.end(JSON.stringify(Row.rowToData(row)));
    }

    handleCreateSession(request: IncomingMessage,
                        response: ServerResponse): void {
        const chunks: Uint8Array[] = [];
        request.on("data", (chunk) => {
            chunks.push(chunk);
        });
        request.on("end", () => {
            try {
                const payload = Buffer.concat(chunks).toString();
                const parsed = JSON.parse(payload);
                let json_obj: any;
                if (parsed instanceof Array) {
                    json_obj = (<any[]>parsed)[0];
                } else {
                    json_obj = parsed;
                }
                const row = Row.dataToRow(json_obj);
                if (row.has("sub")) {
                    const userId = row.getString("sub");
                    if (userId) {
                        this.createSession(userId, response)
                        .catch((error) => {
                            RestServerError.toResponse(error, response);
                            response.end();
                        });
                    } else {
                        throw new RestServerError("Cannot parse sub");
                    }
                } else {
                    throw new RestServerError("Cannot parse payload");
                }
            } catch (error) {
                RestServerError.toResponse(error, response);
                response.end();
            }
        });
        request.on("error", (error) => {
            RestServerError.toResponse(error, response);
            response.end();
        });
    }

    handleCORS(request: IncomingMessage, response: ServerResponse): void {
        const origin = this.getHeader(request.headers, "origin");
        if (origin) {
            const requestMethod = this.getHeader(
                request.headers, "access-control-request-method");
            const requestHeaders = this.getHeader(
                request.headers, "access-control-request-headers");
            console.log(`CORS Origin: ${origin}`);
            console.log(`CORS Access-Control-Request-Method: ` +
                        `${requestMethod}`);
            console.log(`CORS Access-Control-Request-Headers: ` +
                        `${requestHeaders}`);
            response.setHeader("Access-Control-Allow-Origin", origin);
            response.setHeader("Access-Control-Allow-Methods",
                               "GET, POST, PUT, DELETE, HEAD, OPTIONS");
            response.setHeader("Access-Control-Allow-Headers",
                               "rzo-sessionid, Content-Type");
            response.setHeader("Access-Control-Max-Age", "86400");
            response.statusCode = 204;
            response.end();
        } else {
            throw new RestServerError("Invalid request: missing 'Origin'");
        }
    }

    handleEntity(request: IncomingMessage, response: ServerResponse,
                 uriElements: string[]): void {
        try {
            if (uriElements.length <= 1) {
                throw new RestServerError("Missing entity in request", 400);
            }
            const entityName = uriElements[1];
            const entity = this.config.entities.get(entityName);
            if (!entity) {
                throw new RestServerError(`Invalid entity: ${entityName}`, 404);
            }
            switch (request.method) {
                case "HEAD":
                    response.end();
                    break;
                case "GET":
                    this.handleGetEntity(
                        entity, request, response, uriElements);
                    break;
                case "POST":
                    this.post(request, response, entity);
                    break;
                case "DELETE":
                    this.handleDeleteEntity(
                        entity, request, response, uriElements);
                    break;
                case "PUT":
                    this.handlePutEntity(
                        entity, request, response, uriElements);
                    break;
                default:
                    throw new RestServerError(
                        `Invalid Entity request: ${request.method}`, 400);
            }
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    handleSession(request: IncomingMessage, response: ServerResponse,
                  uriElements: string[]): void {
        try {
            switch (request.method) {
                case "POST":
                    this.handleCreateSession(request, response);
                    break;
                default:
                    throw new RestServerError(
                        `Invalid Login request method: ${request.method}`);
            }
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    handleReplicate(request: IncomingMessage, response: ServerResponse,
                 uriElements: string[]): void {
        if (uriElements.length <= 1) {
            throw new RestServerError("Missing entity in request", 400);
        }
        const entityName = uriElements[1];
        const entity = this.config.entities.get(entityName);
        if (!entity) {
            throw new RestServerError(`Invalid entity: ${entityName}`, 404);
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
                throw new RestServerError(
                    `Invalid Replicate request: ${request.method}`);
        }
    }

    handleQueryToken(uriElements: string[], response: ServerResponse): void {
        const query = uriElements[2];
        const queryElements = query.split("&");
        if (queryElements.length != 5) {
            throw new RestServerError("Invalid Token GET query request", 400);
        }
        this.source.queryDeferredToken(queryElements[0], queryElements[1],
                                       queryElements[2], queryElements[3],
                                       queryElements[4])
        .then((token) => {
            if (token) {
                return JSON.stringify(token);
            } else {
                const statusCode = 404;
                const msg = RestServer.toRestError(
                    statusCode, "NotFound",
                    `${queryElements[1]}.${queryElements[2]}: ` +
                    `${queryElements[5]}`);
                response.statusCode = statusCode;
                return msg;
            }
        })
        .then((msg) => {
            if (msg) {
                response.end(msg);
            }
        })
        .catch((error) => {
            RestServerError.toResponse(error, response);
            response.end();
        });
    }

    handleGetToken(tokenUuid: string, response: ServerResponse): void {
        this.source.getDeferredToken(tokenUuid)
        .then((token) => {
            if (token) {
                return JSON.stringify(token);
            } else {
                const statusCode = 404;
                const msg = RestServer.toRestError(
                    statusCode, "NotFound", tokenUuid);
                response.statusCode = statusCode;
                return msg;
            }
        })
        .then((msg) => {
            if (msg) {
                response.end(msg);
            }
        })
        .catch((error) => {
            RestServerError.toResponse(error, response);
            response.end();
        });
    }

    private async putToken(tokenUuid: string, payload: string,
                           request: IncomingMessage,
                           response: ServerResponse): Promise<void> {
        try {
            const context = await this.pullContext(request);
            const token = JSON.parse(payload) as DeferredToken;
            token.token = tokenUuid;
            if (!token.token || !token.updatedby || !token.updated) {
                throw new RestServerError("Invalid Token", 400);
            }
            const result = await this.source.putDeferredToken(token, context);
            response.end(JSON.stringify({ wait: result }));
        } catch (error) {
            RestServerError.toResponse(error, response);
            response.end();
        }
    }

    handlePutToken(tokenUuid: string, request: IncomingMessage,
                   response: ServerResponse): void {
        const chunks: Uint8Array[] = [];
        request.on("data", (chunk) => {
            chunks.push(chunk);
        });
        request.on("end", () => {
            const payload = Buffer.concat(chunks).toString();
            this.putToken(tokenUuid, payload, request, response);
        });
        request.on("error", (error) => {
            RestServerError.toResponse(error, response);
            response.end();
        });
    }

    handleToken(request: IncomingMessage, response: ServerResponse,
                 uriElements: string[]): void {
        /* https:/host/
         *             0   1                          2
         *
         * GET         t   ?     parent&contained&parentField&containedField&id
         *
         * GET         t  token
         *
         * PUT         t  token
         */
        switch (request.method) {
            case "GET":
                if (uriElements.length == 2) {
                    return this.handleGetToken(uriElements[1], response);
                } else if (uriElements.length == 3 && uriElements[1] == "?") {
                    return this.handleQueryToken(uriElements, response);
                } else {
                    throw new RestServerError("Invalid Token GET request", 400);
                }
            case "PUT":
                if (uriElements.length == 2) {
                    return this.handlePutToken(uriElements[1], request,
                                               response);
                } else {
                    throw new RestServerError("Invalid Token PUT request", 400);
                }
            default:
                throw new RestServerError(
                    `Invalid Token request: ${request.method}`);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse): void {
        console.log(`${request.method} - ${request.url}`);
        try {
            // Handle CORS
            if (request.method == "OPTIONS") {
                this.handleCORS(request, response);
                return;
            }
            response.setHeader('Content-Type', 'application/json');
            /* If the request includes an "Origin" header, we respond
             * with the matching Access-Control-Allow-Origin.
             */
            const origin = this.getHeader(request.headers, "origin");
            if (origin) {
                response.setHeader("Access-Control-Allow-Origin", origin);
            }
            let url = request.url || "/";
            if (url == "/") {
                const welcome = { msg: "Welcome to RZO" };
                response.end(JSON.stringify(welcome));
            } else {
                /* split the url by '/', '?' and remove all empty
                 * and '/' elements:
                 */
                const regexp = /([\/\?])/;
                const uriElements = url.split(regexp).filter((element) => {
                    return !!element && element.length > 0 && element != "/";
                });
                if (!uriElements.length || uriElements[0] == "?") {
                    throw new RestServerError(
                        "Request must have a context specified");
                }
                switch (uriElements[0]) {
                    case "e":
                        this.handleEntity(request, response, uriElements);
                        break;
                    case "c":
                        this.handleCollection(request, response, uriElements);
                        break;
                    case "g":
                        this.handleGenerator(request, response, uriElements);
                        break;
                    case "o":
                        this.handleQueryOne(request, response, uriElements);
                        break;
                    case "r":
                        this.handleReplicate(request, response, uriElements);
                        break;
                    case "s":
                        this.handleSession(request, response, uriElements);
                        break;
                    case "t":
                        this.handleToken(request, response, uriElements);
                        break;
                    default:
                        throw new RestServerError(
                            `Unknown context: '${uriElements[0]}'`, 400);
                }
            }
        } catch (error) {
            console.log("Caught at handle()");
            RestServerError.toResponse(error, response);
            response.end();
        }
    }
}

class BootstrapRestServer extends RestServer {

    protected async createSession(userId: string,
                                  response: ServerResponse): Promise<void> {
        if (userId != Nobody.ID) {
            throw new RestServerError(
                "Only the built-in Nobody user can use this server", 403);
        }
        const session = new SessionContext();
        session.sessionId = Entity.generateId();
        session.persona = this.config.getPersona("admins");
        session.userAccount = Nobody.NUM;
        session.userAccountId = Nobody.ID;
        session.expiry =
            new Date(Date.now() + SessionContext.DEFAULT_TIMEOUT);

        this.sessionCache.set(session.sessionId, session);
        const row = session.toRow();
        const sessionJson = JSON.stringify(Row.rowToData(row));
        console.log(
            `Bootstrap session: ${sessionJson}`);
        response.end(sessionJson);
    }
}

type RestServerWorkerSpec = ClassSpec & {
    source: string;
    cache: string;
    sessionBackendSource: string;
    ports: number[];
}

export class RestServerWorker extends DaemonWorker {
    readonly name: string;
    server: Cfg<RestServer>;
    source: Cfg<Source>;
    cache: Cfg<ICache>;
    sessionBackend: Cfg<ISessionBackendService>;
    ports: number[];

    constructor(config: TypeCfg<RestServerWorkerSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.ports = config.spec.ports;
        this.server = new Cfg("server");
        this.source = new Cfg(config.spec.source);
        this.cache = new Cfg(config.spec.cache);
        this.sessionBackend = new Cfg(config.spec.sessionBackendSource);
    }

    protected createRestServer(configuration: IConfiguration): void {
        this.server.v = new RestServer(
            this.source.v.service!,
            this.sessionBackend.v,
            configuration,
            this.cache.v);
    }

    configure(configuration: IConfiguration): void {
        this.source.setIf(
            `Invalid RestServerWorker: ${this.name}: source `,
            configuration.sources.get(this.source.name)
        );
        const worker: unknown = configuration.workers.get(this.cache.name);
        if (!worker || !((<any>worker).isCache)) {
            throw new RestServerError(
                `Invalid RestServerWorker: ${this.name}: worker ` +
                `${this.cache.name} does not exist or it is not a cache`);
        }
        this.cache.v = <ICache>worker;
        if (!configuration.policyConfig) {
            throw new RestServerError(
                `Cannot start RestServerWorker '${this.name}' because no ` +
                `policy configuration was defined`);
        }
        const sessionBackendService: unknown =
            configuration.getSource(this.sessionBackend.name).service;
        if (!((<any>sessionBackendService).isSessionBackendService)) {
            throw new RestServerError(
                `Invalid RestServerWorker: ${this.name}: ` +
                `sessionBackendSource ${this.sessionBackend.name} is not an ` +
                `ISessionBackendService`);
        }
        this.sessionBackend.v = <ISessionBackendService>sessionBackendService;
        this.createRestServer(configuration);
        configuration.registerAsyncTask(this);
    }

    private listenPort(portIndex: number, reject: (error: any) => void): void {
        if (portIndex < this.ports.length &&
            !this.server.v.httpServer.listening) {
            const port = this.ports[portIndex];
            this.server.v.httpServer.listen(port);
        } else {
            reject(new RestServerError(
                `Server '${this.name}' has exhausted all configured ports ` +
                `or it is already listening on another port`));
        }
    }

    start(): Promise<any> {
        console.log("Starting REST server...");
        return new Promise<void>((resolve, reject) => {
            if (!this.server.isSet()) {
                reject(new RestServerError(
                    `Cannot start RestServerWorker '${this.name}' because ` +
                    `its configuration is invalid`));
            } else {
                let portIndex = 0;
                this.server.v.httpServer.on("listening", () => {
                    console.log(
                        `Server '${this.name}' listening on port ` +
                        `${this.ports.at(portIndex) || "????"}`);
                    resolve();
                });
                this.server.v.httpServer.on("error", (error) => {
                    if ((<any>error).code == "EADDRINUSE") {
                        portIndex++;
                        if (portIndex < this.ports.length) {
                            setTimeout(() => {
                                this.server.v.httpServer.close();
                                this.listenPort(portIndex, reject);
                            }, 1000);
                        } else {
                            reject(new RestServerError(
                                `Server '${this.name}' has exhausted all ` +
                                `configured ports and could not find an ` +
                                `available one`));
                        }
                    } else {
                        console.log(`Server '${this.name}' error: ${error}`);
                        reject(error);
                    }
                });
                this.listenPort(portIndex, reject);
            }
        });
    }

    stop(): Promise<any> {
        return new Promise<void>((resolve, reject) => {
            if (this.server.isSet()) {
                this.server.v.httpServer.close((error) => {
                    if (error) {
                        console.log(
                            `Server '${this.name}' cannot close due to: ` +
                            `${error}`);
                        reject(error);
                    } else {
                        console.log(`Server '${this.name}' closed`);
                        resolve();
                    }
                });
            } else {
                resolve();
            }
        });
    }
}

export class BootstrapRestServerWorker extends RestServerWorker {

    protected createRestServer(configuration: IConfiguration): void {
        this.server.v = new BootstrapRestServer(
            this.source.v.service!,
            this.sessionBackend.v,
            configuration,
            this.cache.v);
    }
}

