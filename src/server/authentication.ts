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
    _IError, Row, Filter, Cfg, Entity, TypeCfg, IConfiguration, Nobody,
    JsonObject, Persona, SideEffects
} from "../base/core.js";

import { SessionContext } from "./session.js";

import {
    SessionAwareAdapter, SessionAwareAdapterSpec, AdapterError
} from "./adapter.js";

import { PasswordField } from "./crypto.js";

class AuthenticationError extends _IError {

    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

type OneTimeAdapterSpec = SessionAwareAdapterSpec & {
    persona: string;
}

export class RZOOneTimeAdapter extends SessionAwareAdapter {
    onetimeEntity: Cfg<Entity>;
    loginEntity: Cfg<Entity>;
    persona: Cfg<Persona>;

    constructor(config: TypeCfg<OneTimeAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.onetimeEntity = new Cfg("onetimelogin");
        this.loginEntity = new Cfg("loginentity");
        this.persona = new Cfg(config.spec.persona);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.onetimeEntity.v = configuration.getEntity("onetimelogin");
        this.loginEntity.v = configuration.getEntity("login");
        this.persona.v = configuration.getPersona(this.persona.name);
    }

    private async authenticate(row: Row,
                               response: ServerResponse): Promise<void> {
        if (row.has("username") && row.has("code")) {
            const useraccountnum = row.getString("username");
            const inputCode = row.getString("code");
            if (useraccountnum && inputCode) {
                const filter = new Filter()
                    .op("useraccountnum", "=", useraccountnum);
                const authRow = await this.source.v.getQueryOne(
                    this.onetimeEntity.v, filter);
                if (row.empty) {
                    console.log(
                        `OTL Login attempt blocked due no OTL found for ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                const code = authRow.get("code");
                if (code != inputCode) {
                    console.log(
                        `OTL Login attempt blocked due to code mismatch for ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                const expiry = authRow.get("expiry");
                if (Date.now() > expiry) {
                    console.log(
                        `OTL Login expired: ${expiry}, ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                console.log(
                    `OTL Authentication successful for username: ` +
                    `${useraccountnum}`);
                const userId = authRow.getString("useraccountnum_id");
                const sessionRow =
                    await this.sessionBackend.v.createSessionContext(userId);

                const sessionContext = new SessionContext(
                    sessionRow, this.persona.v);
                this.sessionCache.v.set(
                    sessionContext.sessionId, sessionContext);

                // Delete the login record for this user, if it exists
                this.policyConfig.v.guardResource(
                    sessionContext, "entity/login", "delete");
                const loginRow = await this.source.v.getQueryOne(
                    this.loginEntity.v, filter);
                if (!loginRow.empty) {
                    this.policyConfig.v.guardRow(
                        sessionContext, "entity/login", "delete", loginRow);
                    await this.source.v.deleteImmutable(
                        this.loginEntity.v, loginRow.get("_id"));
                }
                response.end(JSON.stringify(Row.rowToData(sessionRow)));
            } else {
                throw new AuthenticationError("Cannot parse authentication");
            }
        } else {
            throw new AuthenticationError("Cannot parse payload");
        }
    }

    private async createLogin(row: Row, request: IncomingMessage,
                              response: ServerResponse): Promise<void> {
        if (!row.has("password")) {
            console.log("Missing 'password'");
            throw new AuthenticationError("Authentication error", 403);
        }
        const passwd = row.get("password");
        const sessionContext = await this.pullContext(request);
        this.policyConfig.v.guardResource(
            sessionContext, "entity/login", "post");
        /* validate the password. This means the client must pass it in
         * clear text.
         */
        const state = await this.loginEntity.v.create(
            this.source.v, sessionContext);
        const validations: Promise<SideEffects>[] = [];
        validations.push(
            this.loginEntity.v.setValue(
                state, "useraccountnum", sessionContext.userAccount));
        validations.push(
            this.loginEntity.v.setValue(state, "password", passwd));
        await Promise.all(validations);
        await this.loginEntity.v.post(this.source.v, state, sessionContext);
        // Delete the One Time session
        this.sessionCache.v.delete(sessionContext.sessionId);
        // Session expired, delete it, no need to 'await' it.
        this.sessionBackend.v.deleteSession(sessionContext.sessionId);
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse, resource?: string,
                                   id?: string): Promise<void> {
        const row = Row.dataToRow(payload);
        if (request.method == "POST") {
            // Create One Time Login session, await it to handle errors upstream
            await this.authenticate(row, response);
        } else if (request.method == "PUT") {
            // Validate One Time Login session and create a Login record
            await this.createLogin(row, request, response);
        } else {
            console.log(`Invalid request method: ${request.method}`);
            throw new AuthenticationError("Authentication error", 500);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        if (request.method == "POST" || request.method == "PUT") {
            this.handlePayload(request, response);
        } else {
            console.log(`Invalid request method: ${request.method}`);
            throw new AuthenticationError("Authentication error", 500);
        }
    }
}

export class RZOAuthAdapter extends SessionAwareAdapter {
    loginEntity: Cfg<Entity>;

    constructor(config: TypeCfg<SessionAwareAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.loginEntity = new Cfg("loginentity");
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.loginEntity.v = configuration.getEntity("login");
    }

    protected async authenticate(row: Row): Promise<string> {
        if (row.has("username") && row.has("password")) {
            const useraccountnum = row.getString("username");
            const passwd = row.getString("password");
            if (useraccountnum && passwd) {
                const filter = new Filter()
                    .op("useraccountnum", "=", useraccountnum);
                const authRow = await this.source.v.getQueryOne(
                    this.loginEntity.v, filter);
                if (row.empty) {
                    console.log(
                        `Login attempt blocked due no login found for ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                const passwordHash = authRow.get("password");
                const components = passwordHash.split("/");
                if (components.length != 3) {
                    console.log(
                        `Login attempt blocked due to hashed value found ` +
                        `stored for username: ${useraccountnum} does not ` +
                        `have three components, separated by /`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                // [0:algorithm]/[1:salt]/[2:digest]
                const digest = await PasswordField.digest(
                    components[0], passwd, components[1]);
                if (components[2] != digest) {
                    console.log(
                        `Login attempt blocked due to password mismatch for ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                console.log(
                    `Authentication successful for username: ` +
                    `${useraccountnum}`);
                return authRow.getString("useraccountnum_id");
            } else {
                throw new AuthenticationError("Cannot parse authentication");
            }
        } else {
            throw new AuthenticationError("Cannot parse payload");
        }
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse, resource?: string,
                                   id?: string): Promise<void> {
        const row = Row.dataToRow(payload);
        const userId = await this.authenticate(row);
        const sessionRow =
            await this.sessionBackend.v.createSessionContext(userId);

        const personaName = sessionRow.get("persona");
        const persona = this.personas.v.get(personaName);
        if (!persona) {
            throw new AuthenticationError(
                `Invalid persona: ${personaName}`, 403);
        }

        const sessionContext = new SessionContext(sessionRow, persona);
        this.sessionCache.v.set(sessionContext.sessionId, sessionContext);
        response.end(JSON.stringify(Row.rowToData(sessionRow)));
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        try {
            switch (request.method) {
                case "POST":
                    this.handlePayload(request, response);
                    break;
                default:
                    throw new AdapterError(
                        `Invalid Login request method: ${request.method}`);
            }
        } catch (error) {
            AdapterError.toResponse(error, response);
        }
    }
}

export class BootstrapSessionAdapter extends RZOAuthAdapter {

    protected async authenticate(row: Row): Promise<string> {
        if (row.has("username")) {
            return row.getString("username");
        } else {
            throw new AuthenticationError("Cannot parse payload");
        }
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse, resource?: string,
                                   id?: string): Promise<void> {
        const userId = await this.authenticate(Row.dataToRow(payload));
        if (userId != Nobody.ID) {
            throw new AdapterError(
                "Only the built-in Nobody user can use this server", 403);
        }
        const session = new SessionContext();
        session.sessionId = Entity.generateId();
        const persona = this.personas.v.get("admins");
        if (!persona) {
            throw new AdapterError(
                "Cannot find the required 'admins' persona", 403);
        }
        session.persona = persona;
        session.userAccount = Nobody.NUM;
        session.userAccountId = Nobody.ID;
        session.expiry =
            new Date(Date.now() + SessionContext.DEFAULT_TIMEOUT);

        this.sessionCache.v.set(session.sessionId, session);
        const sessionRow = session.toRow();
        const sessionJson = JSON.stringify(Row.rowToData(sessionRow));
        console.log(`Bootstrap session: ${sessionJson}`);
        response.end(sessionJson);
    }
}

