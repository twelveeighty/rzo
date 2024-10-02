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

import { IncomingMessage, IncomingHttpHeaders, ServerResponse } from "http";

import {
    _IError, Entity, Cfg, DaemonWorker, IService, IPolicyConfiguration,
    TypeCfg, ClassSpec, IConfiguration, Persona, Row, Query, Filter,
    OrderBy, Collection, IResultSet, DeferredToken, JsonObject, Logger,
    IContext, ServiceSource
} from "../base/core.js";

import { ICache } from "./cache.js";

import { SessionContext, ISessionBackendService } from "../base/session.js";

type ErrorType = {
    type: string;
    message: string;
    causeType?: string;
    cause?: string;
}

function toRestError(statusCode: number, type: string, message:string,
                     causeType?: string, cause?: string): string {
    const err: ErrorType = {
        type: type,
        message: message,
        causeType: causeType,
        cause: cause
    };
    return JSON.stringify(err);
}

export function getHeader(headers: IncomingHttpHeaders,
                          key: string): string | undefined {
    const value = headers[key.toLowerCase()];
    if (value) {
        if (Array.isArray(value)) {
            return value[0];
        }
        return value;
    } else {
        return undefined;
    }
}

function respondWithRestError(response: ServerResponse, statusCode: number,
                              type: string, message:string): void {
    response.statusCode = statusCode;
    response.end(toRestError(statusCode, type, message));
}

function stringToQuery(input?: string): Query {
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
        let match = input.match(/^f=(\*|[A-Za-z0-9_,\*\(\)]+)/);
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
            throw new AdapterError(
                `where clause must not precede order-by clause: ${input}`);
        }
        const orderBy: OrderBy[] = [];
        if (orderPos >= 0) {
            match = safeInput.match(/\&o=([-+A-Za-z0-9_,]+)/);
            if (!match) {
                throw new AdapterError(
                    `Cannot parse order by clause in ${input}`);
            }
            const orderBys = match[1].trim().split(",").filter((field) => {
                return !!field;
            });
            orderBys.forEach((element) => {
                if (element.length < 2) {
                    throw new AdapterError(`Invalid order-by element ` +
                                        `in ${input}`);
                }
                const orderByField = element.slice(0, -1);
                if (element.endsWith("+")) {
                    orderBy.push({ field: orderByField, order: "asc" });
                } else if (element.endsWith("-")) {
                    orderBy.push({ field: orderByField, order: "desc" });
                } else {
                    throw new AdapterError(`Invalid direction(s) in ` +
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

export class AdapterError extends _IError {

    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }

    static toResponse(logger: Logger, exc: unknown,
                      response: ServerResponse): void {
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
                msg = toRestError(
                    statusCode, type, message, causeType, causeMsg);
                logger.exc(cause);
            } else {
                msg = toRestError(statusCode, type, message);
                logger.exc(<Error>error);
            }
        } else if (exc instanceof Error) {
            const error = <Error>exc;
            const type = error.name;
            const message = error.message;
            if (error.cause && error.cause instanceof Error) {
                const cause = <Error>error.cause;
                const causeType = cause.name;
                const causeMsg = cause.message;
                msg = toRestError(
                    statusCode, type, message, causeType, causeMsg);
                logger.exc(cause);
            } else {
                logger.exc(<Error>error);
                msg = toRestError(statusCode, type, message);
            }
        } else {
            msg = toRestError(statusCode, typeof exc, `${exc}`);
            logger.error(msg);
        }
        response.statusCode = statusCode;
        response.end(msg);
    }
}

export interface IAdapter {
    get isAdapter(): boolean;
    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void;
}

export type AdapterSpec = ClassSpec & {
    source: string;
}

export type SessionAwareAdapterSpec = AdapterSpec & {
    cache: string;
    sessionBackendSource: string;
}

export class BaseAdapter extends DaemonWorker implements IAdapter {
    readonly name: string;
    source: Cfg<IService>;
    logger: Logger;

    constructor(config: TypeCfg<AdapterSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.source = new Cfg(config.spec.source);
        this.logger = new Logger(`adapter/${this.name}`);
    }

    get isAdapter(): boolean {
        return true;
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.source.v = (<ServiceSource>configuration.getSource(
            this.source.name).ensure(ServiceSource)).service;
        this.logger.configure(configuration);
    }

    protected decodeOptionalURIComponent(input?: string) {
        if (input !== undefined) {
            return decodeURIComponent(input);
        } else {
            return input;
        }
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
                                   id?: string): Promise<void> {
        response.end();
    }

    protected handlePayload(request: IncomingMessage, response: ServerResponse,
                            uriElements: string[], resource?: string,
                            id?: string): void {
        const chunks: Uint8Array[] = [];
        request.on("data", (chunk) => {
            chunks.push(chunk);
        });
        request.on("end", () => {
            try {
                const payload = Buffer.concat(chunks).toString();
                const parsed = payload ? JSON.parse(payload) : {};
                let json_obj: any;
                if (parsed instanceof Array) {
                    json_obj = (<any[]>parsed)[0];
                } else {
                    json_obj = parsed;
                }
                this.payloadHandler(
                    json_obj, request, response, uriElements, resource, id)
                .catch((error) => {
                    AdapterError.toResponse(this.logger, error, response);
                });
            } catch(error) {
                AdapterError.toResponse(this.logger, error, response);
            }
        });
        request.on("error", (error) => {
            AdapterError.toResponse(this.logger, error, response);
        });
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
    }
}

export class SessionAwareAdapter extends BaseAdapter {
    personas: Cfg<Map<string, Persona>>;
    policyConfig: Cfg<IPolicyConfiguration>;
    sessionBackend: Cfg<ISessionBackendService>;
    sessionCache: Cfg<ICache>;

    constructor(config: TypeCfg<SessionAwareAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.personas = new Cfg("personas");
        this.policyConfig = new Cfg("policyconfig");
        this.sessionCache = new Cfg(config.spec.cache);
        this.sessionBackend = new Cfg(config.spec.sessionBackendSource);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);

        this.personas.v = configuration.personas;

        const worker: unknown = configuration.workers.get(
            this.sessionCache.name);
        if (!worker || !((<any>worker).isCache)) {
            throw new AdapterError(
                `Invalid BaseAdapter: ${this.name}: worker ` +
                `${this.sessionCache.name} does not exist or it is not a` +
                `cache`);
        }
        this.sessionCache.v = <ICache>worker;

        if (!configuration.policyConfig) {
            throw new AdapterError(
                `Cannot start BaseAdapter '${this.name}' because no ` +
                `policy configuration was defined`);
        }
        this.policyConfig.v = configuration.policyConfig!;

        const source = configuration.getSource(this.sessionBackend.name).ensure(
            ServiceSource) as ServiceSource;
        const sessionBackendService: unknown = source.service;
        if (!((<any>sessionBackendService).isSessionBackendService)) {
            throw new AdapterError(
                `Invalid BaseAdapter: ${this.name}: ` +
                `sessionBackendSource ${this.sessionBackend.name} is not an ` +
                `ISessionBackendService`);
        }
        this.sessionBackend.v = <ISessionBackendService>sessionBackendService;
    }

    async pullContext(request: IncomingMessage): Promise<SessionContext> {
        const sessionId = getHeader(request.headers, "rzo-sessionid");
        if (!sessionId) {
            throw new AdapterError("Missing 'rzo-sessionid'");
        }
        let context: SessionContext = this.sessionCache.v.get(sessionId);
        // Check the expiry on the cached session
        if (context && context.expiry.getTime() <= Date.now()) {
            console.log(
                `cached sessionContext expired: ${context.expiry}`);
            this.sessionCache.v.delete(sessionId);
            // Session expired, delete it, no need to 'await' it.
            this.sessionBackend.v.deleteSession(this.logger, sessionId);
            throw new AdapterError("Session expired", 401);
        }
        if (!context) {
            const row = await this.sessionBackend.v.getSession(
                this.logger, sessionId);
            const personaName = row.get("persona");
            const persona = this.personas.v.get(personaName);
            if (!persona) {
                throw new AdapterError(
                    `Invalid persona: ${personaName}`, 403);
            }
            return new SessionContext(row, persona);
        }
        return context;
    }
}

export class EntityAdapter extends SessionAwareAdapter {
    entities: Cfg<Map<string, Entity>>;

    constructor(config: TypeCfg<SessionAwareAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.entities = new Cfg("entities");
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.entities.v = configuration.entities;
    }

    async getOne(context: IContext, request: IncomingMessage,
                 response: ServerResponse, entity: Entity, id: string,
                 rev?: string) : Promise<void> {
        const resource = `entity/${entity.name}`;
        this.policyConfig.v.guardResource(context, resource, "get");
        const row = await this.source.v.getOne(
            this.logger, context, entity, id, rev);
        if (row && !row.empty) {
            this.policyConfig.v.guardRow(context, resource, "get", row);
            response.end(JSON.stringify(Row.rowToData(row)));
        } else {
            respondWithRestError(
                response, 404, "NotFound", `${entity.name} : ${id}`);
        }
    }

    async getQuery(context: IContext, request: IncomingMessage,
                   response: ServerResponse, entity: Entity,
                   queryStr: string): Promise<void> {

        this.logger.debug(`Entity: ${entity.name}; query: ${queryStr}`);
        try {
            const resource = `entity/${entity.name}`;
            this.policyConfig.v.guardResource(context, resource, "get");
            const query = stringToQuery(queryStr);
            const resultSet = await this.source.v.getQuery(
                this.logger, context, entity, query);
            this.policyConfig.v.guardResultSet(context, resource, resultSet);
            resultSet.rewind();
            const result: any[] = [];
            while (resultSet.next()) {
                const row = resultSet.getRow();
                result.push(Row.rowToData(row));
            }
            response.end(JSON.stringify(result));
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    async handleGetEntity(entity: Entity, request: IncomingMessage,
                          response: ServerResponse,
                          uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0   1     2     3         4
         * GET         e entity uuid                    Get winning version
         * GET         e entity uuid   ?     rev=1-xxx  Get specific version
         * GET         e entity  ?   filter             Query
         */
        try {
            const context = await this.pullContext(request);
            if (uriElements.length == 3) {
                await this.getOne(
                    context, request, response, entity, uriElements[2]);
            } else if (uriElements.length == 4 && uriElements[2] == "?") {
                const query = decodeURIComponent(uriElements[3]);
                this.getQuery(
                    context, request, response, entity, query);
            } else if (uriElements.length == 5 && uriElements[3] == "?" &&
                      uriElements[4].startsWith("rev=")) {
                const version = uriElements[4].substring("rev=".length);
                await this.getOne(
                    context, request, response, entity, uriElements[2],
                    version);
            } else {
                throw new AdapterError(
                    `Invalid request: invalid URI components for GET`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    async delete(request: IncomingMessage, response: ServerResponse,
                 entity: Entity, id: string, version: string) : Promise<void> {
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${entity.name}`;
            this.policyConfig.v.guardResource(context, resource, "delete");
            const row = await this.source.v.getOne(
                this.logger, context, entity, id, version);
            if (row && !row.empty) {
                this.policyConfig.v.guardRow(context, resource, "delete", row);
                await this.source.v.delete(
                    this.logger, context, entity, id, version);
                response.end(`{"id": "${id}}"`);
            } else {
                respondWithRestError(
                    response, 404, "NotFound", `${entity.name} : ${id}`);
            }
        } catch(error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
                                   id?: string): Promise<void> {
        const entity = this.entities.v.get(resource!);
        if (!entity) {
            throw new AdapterError(`Invalid entity: ${resource}`, 404);
        }
        const context = await this.pullContext(request);
        const policyTarget = `entity/${entity.name}`;
        const action = (id !== undefined) ? "put" : "post";
        this.policyConfig.v.guardResource(context, policyTarget, action);
        const row = Row.dataToRow(payload, entity);
        if (row && !row.empty) {
            this.policyConfig.v.guardRow(context, policyTarget, action, row);
            let output: Row;
            if (id) {
                output = await this.source.v.put(
                    this.logger, context, entity, id, row);
            } else {
                output = await this.source.v.post(
                    this.logger, context, entity, row);
            }
            response.end(JSON.stringify(Row.rowToData(output)));
        } else {
            throw new AdapterError("Cannot parse payload");
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
            throw new AdapterError(
                `Invalid request: invalid URI components for DELETE`);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        try {
            if (uriElements.length <= 1) {
                throw new AdapterError("Missing entity in request", 400);
            }
            const entityName = uriElements[1];
            const entity = this.entities.v.get(entityName);
            if (!entity) {
                throw new AdapterError(`Invalid entity: ${entityName}`, 404);
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
                    this.handlePayload(
                        request, response, uriElements, entityName);
                    break;
                case "DELETE":
                    this.handleDeleteEntity(
                        entity, request, response, uriElements);
                    break;
                case "PUT":
                    if (uriElements.length == 3) {
                        this.handlePayload(
                            request, response, uriElements, entityName,
                            uriElements[2]);
                    } else {
                        throw new AdapterError(
                            `Invalid request: invalid URI components for PUT`);
                    }
                    break;
                default:
                    throw new AdapterError(
                        `Invalid Entity request: ${request.method}`, 400);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }
}

export class CollectionAdapter extends SessionAwareAdapter {
    collections: Cfg<Map<string, Collection>>;

    constructor(config: TypeCfg<SessionAwareAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.collections = new Cfg("collections");
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.collections.v = configuration.collections;
    }

    async handleCollection(collection: Collection, request: IncomingMessage,
                           response: ServerResponse,
                           uriElements: string[]): Promise<void> {
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${collection.entity.v.name}`;
            this.policyConfig.v.guardResource(context, resource, "get");
            let resultSet: IResultSet;
            if (uriElements.length > 3) {
                const queryStr = decodeURIComponent(uriElements[3]);
                this.logger.info(
                    `Collection: ${collection.name}; query: ${queryStr}`);
                const query = stringToQuery(queryStr);
                resultSet = await collection.query(context, query);
            } else {
                resultSet = await collection.query(context);
            }
            this.policyConfig.v.guardResultSet(context, resource, resultSet);
            response.end(JSON.stringify(resultSet.getAll()));
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        /* https:/host/
         *             0     1       2       3
         *             c collection
         *             c collection  ?     q=filter
         *             c collection  ?     f=fields&q=filter
         */
        if (request.method != "GET") {
            throw new AdapterError(
                `Invalid request method '${request.method}' for collection ` +
                `queries`);
        }
        if (uriElements.length <= 1) {
            throw new AdapterError("Missing collection in request");
        }
        const collectionName = uriElements[1];
        const collection = this.collections.v.get(collectionName);
        if (!collection) {
            throw new AdapterError(`Invalid collection: ${collectionName}`);
        }
        this.handleCollection(collection, request, response, uriElements);
    }
}

export class GeneratorAdapter extends SessionAwareAdapter {

    async handleGenerator(request: IncomingMessage, response: ServerResponse,
                          uriElements: string[]): Promise<void> {
        try {
            const context = await this.pullContext(request);
            const nextval = await this.source.v.getGeneratorNext(
                this.logger, context, uriElements[1]);
            response.end(JSON.stringify({ "nextval": nextval }));
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        /* https:/host/
         *             0   1
         *             g sequence
         */
        if (uriElements.length != 2) {
            throw new AdapterError("Invalid generator request");
        }
        this.handleGenerator(request, response, uriElements);
    }
}

export class QueryOneAdapter extends SessionAwareAdapter {
    entities: Cfg<Map<string, Entity>>;

    constructor(config: TypeCfg<SessionAwareAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.entities = new Cfg("entities");
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.entities.v = configuration.entities;
    }

    async handleQueryOne(entity: Entity, request: IncomingMessage,
                         response: ServerResponse,
                         uriElements: string[]): Promise<void> {
        try {
            const context = await this.pullContext(request);
            const resource = `entity/${entity.name}`;
            this.policyConfig.v.guardResource(context, resource, "get");
            const filter = new Filter();
            filter.parseParameters(decodeURIComponent(uriElements[3]));
            const row = await this.source.v.getQueryOne(
                this.logger, context, entity, filter);
            if (row && !row.empty) {
                this.policyConfig.v.guardRow(context, resource, "get", row);
                response.end(JSON.stringify(Row.rowToData(row)));
            } else {
                respondWithRestError(
                    response, 404, "NotFound", `${entity.name}`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        /* https:/host/
         *             0   1      2     3
         *             o entity
         *             o entity   ?   filter
         *
         */
        if (request.method != "GET") {
            throw new AdapterError(
                `Invalid request method '${request.method}' for QueryOne`);
        }
        if (uriElements.length != 4) {
            throw new AdapterError("Malformed QueryOne request");
        }
        const entity = this.entities.v.get(uriElements[1]);
        if (!entity) {
            throw new AdapterError(`Invalid entity: ${uriElements[1]}`);
        }
        this.handleQueryOne(entity, request, response, uriElements);
    }
}

export class TokenAdapter extends SessionAwareAdapter {

    async handleQueryToken(request: IncomingMessage, response: ServerResponse,
                           uriElements: string[]): Promise<void> {
        try {
            const context = await this.pullContext(request);
            const query = uriElements[2];
            const queryElements = query.split("&");
            if (queryElements.length != 5) {
                throw new AdapterError("Invalid Token GET query request", 400);
            }
            const token = await this.source.v.queryDeferredToken(
                this.logger, context,
                queryElements[0], queryElements[1], queryElements[2],
                queryElements[3], queryElements[4]);
            if (token) {
                response.end(JSON.stringify(token));
            } else {
                respondWithRestError(
                    response, 404, "NotFound",
                    `${queryElements[1]}.${queryElements[2]}: ` +
                    `${queryElements[5]}`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    async handleGetToken(request: IncomingMessage, response: ServerResponse,
                         tokenUuid: string): Promise<void> {
        try {
            const context = await this.pullContext(request);
            const token = await this.source.v.getDeferredToken(
                this.logger, context, tokenUuid);
            if (token) {
                response.end(JSON.stringify(token));
            } else {
                respondWithRestError(response, 404, "NotFound", tokenUuid);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
                                   id?: string): Promise<void> {
        const context = await this.pullContext(request);
        const token = payload as DeferredToken;
        token.token = id!;
        if (!token.updatedby || !token.updated) {
            throw new AdapterError("Invalid Token", 400);
        }
        const result = await this.source.v.putDeferredToken(
            this.logger, context, token);
        response.end(JSON.stringify({ wait: result }));
    }

    handle(request: IncomingMessage, response: ServerResponse,
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
                    this.handleGetToken(request, response, uriElements[1]);
                    return;
                } else if (uriElements.length == 3 && uriElements[1] == "?") {
                    this.handleQueryToken(request, response, uriElements);
                    return;
                } else {
                    throw new AdapterError("Invalid Token GET request", 400);
                }
            case "PUT":
                if (uriElements.length == 2) {
                    this.handlePayload(
                        request, response, uriElements, "", uriElements[1]);
                } else {
                    throw new AdapterError("Invalid Token PUT request", 400);
                }
            default:
                throw new AdapterError(
                    `Invalid Token request: ${request.method}`);
        }
    }
}

