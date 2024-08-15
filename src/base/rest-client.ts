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
    Entity, IService, IResultSet, Query, MemResultSet, EmptyResultSet,
    Filter, Collection, IContext, Row, TypeCfg, DeferredToken, Source,
    ClassSpec, IConfiguration, Cfg, Authenticator, IAuthenticator, Persona,
    Logger
} from "./core.js";

import { SessionContext } from "./session.js";

class RestClientError extends Error {

    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }

    static fromResponse(response: Response, body?: string): RestClientError {
        const details = body ? `: ${body}` : "";
        const msg = `HTTP Error: ${response.status} ${response.statusText}` +
            `${details}`;
        return new RestClientError(msg);
    }
}

export class RestClient implements IService, IAuthenticator {

    readonly url: string;
    sessionEntity: Cfg<Entity>;
    personas: Cfg<Map<string, Persona>>;

    constructor(url: string) {
        this.sessionEntity = new Cfg("session");
        this.personas = new Cfg("personas");
        let finalUrl = url.trim();
        while (finalUrl.endsWith("/")) {
            finalUrl = finalUrl.slice(0, -1);
        }
        this.url = finalUrl;
    }

    configure(configuration: IConfiguration) {
        this.sessionEntity.v = configuration.getEntity(this.sessionEntity.name);
        this.personas.v = configuration.personas;
    }

    async getDeferredToken(logger: Logger, context: IContext,
                           tokenUuid: string): Promise<DeferredToken | null> {
        const targetUrl = `${this.url}/t/${tokenUuid}`;
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(targetUrl);
        if (!response.ok) {
            if (response.status == 404) {
                return null;
            }
            throw new RestClientError(
                `fetch returned status code ${response.status}`);
        }
        const token = await response.json() as DeferredToken;
        return token;
    }

    async queryDeferredToken(logger: Logger, context: IContext, parent: string,
                             contained: string, parentField: string,
                             containedField: string,
                             id: string): Promise<DeferredToken | null> {
        const targetUrl =
            `${this.url}/t?${parent}&${contained}&${parentField}&` +
            `${containedField}&${id}`;
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(targetUrl);
        if (!response.ok) {
            if (response.status == 404) {
                return null;
            }
            throw new RestClientError(
                `fetch returned status code ${response.status}`);
        }
        const token = await response.json() as DeferredToken;
        return token;
    }

    async putDeferredToken(logger: Logger, context: IContext,
                     token: DeferredToken): Promise<number> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        const targetUrl = `${this.url}/t/${token.token}`;
        const payload = JSON.stringify(token);
        const headers = new Headers();
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "put",
            body: payload,
            headers: headers
        };
        fetchRequest.headers.set("rzo-sessionid", context.sessionId);

        logger.info(`fetch PUT - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const row = Row.dataToRow(data);
        return row.has("wait") ? row.get("wait") : 0;
    }

    async getDBInfo(logger: Logger, context: IContext): Promise<Row> {
        const targetUrl = this.url;
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(targetUrl);
        if (!response.ok) {
            throw new RestClientError(
                `fetch returned status code ${response.status}`);
        }
        const data = await response.json();
        return Row.dataToRow(data);
    }

    async getGeneratorNext(logger: Logger, context: IContext,
                           generatorName: string): Promise<string> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        const targetUrl = this.url + "/g/" + generatorName;
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(
            targetUrl, { headers: { "rzo-sessionid": context.sessionId } });
        if (!response.ok) {
            throw RestClientError.fromResponse(response);
        }
        const data = await response.json();
        const row = Row.dataToRow(data);
        if (row.empty) {
            throw new RestClientError(`GET call for Generator ` +
                                      `${generatorName} resulted in an ` +
                                      `empty or invalid JSON object`);
        }
        return "" + row.get("nextval");
    }

    async getSequenceId(logger: Logger, context: IContext,
                        entity: Entity): Promise<string> {
        const targetUrl = `${this.url}/e/${entity.name}`;
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(targetUrl);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        return <string>(data["update_seq"]);
    }

    async getQueryOne(logger: Logger, context: IContext, entity: Entity,
                      filter: Filter): Promise<Row> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        if (filter.sealed) {
            throw new RestClientError(
                "Cannot use RestClient with sealed filters");
        }
        if (filter.empty) {
            throw new RestClientError(
                "Must specify a filter for getQueryOne()");
        }
        const targetUrl = `${this.url}/o/${entity.name}` +
            `${filter.toParameters(true)}`;

        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(
            targetUrl, { headers: { "rzo-sessionid": context!.sessionId } });
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        return Row.dataToRow(data, entity);
    }

    async getOne(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev?: string): Promise<Row> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        let targetUrl;
        if (rev) {
            targetUrl = `${this.url}/e/${entity.name}/${id}?rev=${rev}`;
        } else {
            targetUrl = `${this.url}/e/${entity.name}/${id}`;
        }
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(
            targetUrl, { headers: { "rzo-sessionid": context!.sessionId } });
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const row = Row.dataToRow(data, entity);
        if (row.empty) {
            throw new RestClientError(
                `GET call for Entity ${entity.name}, id = ${id} resulted in ` +
                `an empty or invalid JSON object`);
        }
        return row;
    }

    queryParams(query: Query): string {
        let result = "f=*";
        if (query) {
            let firstTerm = true;
            if (query.fields != null && query.fields.length > 0) {
                result = "f=" + query.fields.join();
                firstTerm = false;
            }
            if (query.hasOrderBy) {
                result += query.orderToParameters(firstTerm);
                firstTerm = false;
            }
            if (query.filter && query.filter.notEmpty) {
                if (query.filter.sealed) {
                    throw new RestClientError(
                        "Cannot use RestClient with sealed filters");
                }
                result += query.filter.toParameters(firstTerm);
            }
        }
        return result;
    }

    async queryCollection(logger: Logger, context: IContext,
                          collection: Collection,
                          query?: Query): Promise<IResultSet> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        let targetUrl = this.url + "/c/" + collection.name;
        let firstTerm = true;
        if (query && query.fields.length > 0) {
            targetUrl += "?f=" + query.fields.join();
            firstTerm = false;
        }
        if (query?.filter && query.filter.notEmpty) {
            if (query.filter.sealed) {
                throw new RestClientError(
                    "Cannot use RestClient with sealed filters");
            }
            targetUrl += query.filter.toParameters(firstTerm);
            firstTerm = false;
        }
        if (query && query.orderBy.length > 0) {
            targetUrl += query.orderToParameters(firstTerm);
        }
        const headers = new Headers();
        const fetchRequest = {
            method: "get",
            headers: headers
        };
        fetchRequest.headers.set("rzo-sessionid", context.sessionId);

        logger.info(`fetch collection GET - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            if (response.status == 404) {
                return new EmptyResultSet();
            }
            throw new RestClientError(
                `fetch returned status code ${response.status}`);
        }
        const responseData = await response.json();
        // return empty if the result is not an array
        if (!Array.isArray(responseData)) {
            return new EmptyResultSet();
        }
        const jsonArray = <Object[]>responseData;
        return new MemResultSet(jsonArray);
    }

    async getQuery(logger: Logger, context: IContext, entity: Entity,
             query: Query): Promise<IResultSet> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        const targetUrl = this.url + "/e/" + entity.name +
            "?" + this.queryParams(query);
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(
            targetUrl, { headers: { "rzo-sessionid": context!.sessionId } });
        if (!response.ok) {
            if (response.status == 404) {
                return new EmptyResultSet();
            }
            throw new RestClientError(
                `fetch returned status code ${response.status}`);
        }
        const responseData = await response.json();
        // return empty if the result is not an array
        if (!Array.isArray(responseData)) {
            return new EmptyResultSet();
        }
        const jsonArray = <Object[]>responseData;
        return new MemResultSet(jsonArray);
    }

    async put(logger: Logger, context: IContext, entity: Entity, id: string,
              row: Row): Promise<Row> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        if (entity.immutable) {
            throw new RestClientError(`Entity '${entity.name}' is immutable`);
        }
        const jsonData = Row.rowToData(row);
        const payload = JSON.stringify(jsonData);

        const headers = new Headers();
        headers.set("rzo-sessionid", context.sessionId);
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "put",
            body: payload,
            headers: headers
        };
        const targetUrl = this.url + "/e/" + entity.name + "/" + id;
        logger.info(`fetch PUT - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const result = Row.dataToRow(data, entity);
        if (result.empty) {
            throw new RestClientError(
                `PUT call for Entity ${entity.name}, id = ${id} resulted in ` +
                `an empty or invalid JSON object`);
        }
        return result;
    }

    async post(logger: Logger, context: IContext, entity: Entity,
         row: Row): Promise<Row> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        const jsonData = Row.rowToData(row);
        const payload = JSON.stringify(jsonData);

        const headers = new Headers();
        headers.set("rzo-sessionid", context.sessionId);
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "post",
            body: payload,
            headers: headers
        };
        const targetUrl = this.url + "/e/" + entity.name;
        logger.info(`fetch POST - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const result = Row.dataToRow(data, entity);
        if (result.empty) {
            throw new RestClientError(
                `POST call for Entity ${entity.name} resulted in an empty ` +
                `or invalid JSON object`);
        }
        return result;
    }

    async deleteImmutable(logger: Logger, context: IContext, entity: Entity,
                          id: string): Promise<void> {
        throw new RestClientError("deleteImmutable() is forbidden");
    }

    async delete(logger: Logger, context: IContext, entity: Entity, id: string,
                 rev: string): Promise<void> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        const headers = new Headers();
        headers.set("rzo-sessionid", context.sessionId);
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "delete",
            headers: headers
        };
        const targetUrl = `${this.url}/e/${entity.name}/${id}?rev=${rev}`;
        logger.info(`fetch DELETE - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
    }

    get isAuthenticator(): boolean {
        return true;
    }

    async resetAuthentication(logger: Logger, row: Row): Promise<Row> {
        /* This is the client call to:
         * server.authentication.RZOOneTimeAdapter.createOneTimeLogin()
         * GET https:/host/otl?user=WILSONB
         *
         * Returns: { email: "ma..@d.com", expiry: "<date>" }
         */
        if (!row.has("user")) {
            throw new RestClientError("Missing 'user' in passed row");
        }
        const targetUrl = `${this.url}/otl?user=${row.getString("user")}`;
        logger.info(`fetch GET - ${targetUrl}`);
        const response = await fetch(targetUrl);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const result = Row.dataToRow(data);
        if (result.empty) {
            throw new RestClientError(
                `GET call for resetAuthentication resulted in an empty or ` +
                `invalid JSON object`);
        }
        return result;
    }

    async oneTimeLogin(logger: Logger, row: Row): Promise<IContext> {
        /* This is the client call to:
         * server.authentication.RZOOneTimeAdapter.authenticate()
         * POST https:/host/otl
         *    { username: "WILSONB", code: "1234567" }
         *
         * Returns SessionContext {}
         */
        const jsonData = Row.rowToData(row);
        const payload = JSON.stringify(jsonData);

        const headers = new Headers();
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "post",
            body: payload,
            headers: headers
        };
        const targetUrl = this.url + "/otl";
        logger.info(`fetch POST - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const result = Row.dataToRow(data);
        if (result.empty) {
            throw new RestClientError(
                `One time login does not return anything`);
        }
        const persona = this.personas.v.get(result.get("persona"));
        if (!persona) {
            throw new RestClientError(
                `OneTimeLogin returns unknown persona: ` +
                `${result.get("persona")}`);
        }
        return new SessionContext(result, persona);
    }

    async createLogin(logger: Logger, context: IContext,
                      row: Row): Promise<Row> {
        /* This is the client call to:
         * server.authentication.RZOOneTimeAdapter.createLogin()
         * PUT https:/host/otl
         *    { password: "clear_text_password" }
         *
         * Returns Row, a Login without password
         */
        if (!context.sessionId) {
            throw new RestClientError(
                "One time Session ID missing");
        }
        const jsonData = Row.rowToData(row);
        const payload = JSON.stringify(jsonData);

        const headers = new Headers();
        headers.set("rzo-sessionid", context.sessionId);
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "put",
            body: payload,
            headers: headers
        };
        const targetUrl = this.url + "/otl";
        logger.info(`fetch PUT - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const result = Row.dataToRow(data);
        if (result.empty) {
            throw new RestClientError(`create login returns empty`);
        }
        return result;
    }

    async login(logger: Logger, row: Row): Promise<IContext> {
        const payload = JSON.stringify(Row.rowToData(row));
        const fetchRequest = {
            method: "post",
            body: payload
        };
        const targetUrl = this.url + "/s";
        logger.info(`fetch POST - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        const result = Row.dataToRow(data);
        if (result.empty) {
            throw new RestClientError(`Login does not return anything`);
        }
        const persona = this.personas.v.get(result.get("persona"));
        if (!persona) {
            throw new RestClientError(
                `Login returns unknown persona: ${result.get("persona")}`);
        }
        return new SessionContext(result, persona);
    }

    async logout(logger: Logger, context: IContext): Promise<void> {
        if (!context.sessionId) {
            throw new RestClientError("Session ID missing");
        }
        const headers = new Headers();
        headers.set("rzo-sessionid", context.sessionId);
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "delete",
            headers: headers
        };
        const targetUrl = this.url + "/s";
        logger.info(`fetch DELETE - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw RestClientError.fromResponse(response, body);
        }
    }
}

type RestClientSourceSpec = ClassSpec & {
    url: string;
}

export class RestClientSource extends Source {
    url: string;
    private _service: RestClient;

    constructor(config: TypeCfg<RestClientSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.url = config.spec.url;
        this._service = new RestClient(this.url);
    }

    configure(configuration: IConfiguration) {
        this._service.configure(configuration);
    }

    get service(): IService {
        return this._service;
    }
}

type RestClientAuthenticatorSpec = ClassSpec & {
    source: string;
}

export class RestClientAuthenticator extends Authenticator {
    source: Cfg<IAuthenticator>;

    constructor(config: TypeCfg<RestClientAuthenticatorSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.source = new Cfg(config.spec.source);
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        const target = configuration.getSource(this.source.name).service;
        if (!((<any>target).isAuthenticator)) {
            throw new RestClientError(
                `Invalid Authenticator ${this.name}, source ` +
                `'${this.source.name}' is not an IAuthenticator`);
        }
        this.source.v = target as unknown as IAuthenticator;
    }

    get service(): IAuthenticator {
        return this.source.v;
    }
}

