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

import { randomInt } from "node:crypto";

import {
    _IError, Row, Filter, Cfg, Entity, TypeCfg, IConfiguration, Nobody,
    JsonObject, Persona, SideEffects, Logger, IContext
} from "../base/core.js";

import { NOCONTEXT } from "../base/configuration.js";

import { SessionContext } from "../base/session.js";

import {
    SessionAwareAdapter, SessionAwareAdapterSpec, AdapterError, getHeader
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

interface IRZOAuthService {
    getQueryOne(logger: Logger, context: IContext, entity: Entity,
                filter: Filter): Promise<Row>;
}

export async function rzoAuthenticate(logger: Logger,
                                      service: IRZOAuthService,
                                      loginEntity: Entity,
                                      row: Row): Promise<string> {
    if (row.has("username") && row.has("password")) {
        const useraccountnum = row.getString("username");
        const passwd = row.getString("password");
        if (useraccountnum && passwd) {
            const filter = new Filter()
                .op("useraccountnum", "=", useraccountnum);
            const authRow = await service.getQueryOne(
                logger, NOCONTEXT, loginEntity, filter);
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

export class RZOOneTimeAdapter extends SessionAwareAdapter {
    oneTimeEntity: Cfg<Entity>;
    loginEntity: Cfg<Entity>;
    userAccountEntity: Cfg<Entity>;
    persona: Cfg<Persona>;

    static FIVEMINUTES = 1000*5*60;

    constructor(config: TypeCfg<OneTimeAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.oneTimeEntity = new Cfg("onetimelogin");
        this.loginEntity = new Cfg("loginentity");
        this.userAccountEntity = new Cfg("useraccount");
        this.persona = new Cfg(config.spec.persona);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.oneTimeEntity.v = configuration.getEntity("onetimelogin");
        this.loginEntity.v = configuration.getEntity("login");
        this.userAccountEntity.v = configuration.getEntity("useraccount");
        this.persona.v = configuration.getPersona(this.persona.name);
    }

    private async createOneTimeLogin(userNum: string,
                                     response: ServerResponse): Promise<void> {
        const filter = new Filter()
            .op("useraccountnum", "=", userNum);
        const userRow = await this.source.v.getQueryOne(
            this.logger, NOCONTEXT, this.userAccountEntity.v, filter);
        if (userRow.empty) {
            console.log(
                `Cannot create One Time Login because the requested usernum ` +
                `${userNum} does not exist`);
            throw new AuthenticationError("Authentication error", 403);
        }
        // Delete any existing onetimelogin, if present
        const existingRow = await this.source.v.getQueryOne(
            this.logger, NOCONTEXT, this.oneTimeEntity.v, filter);
        if (!existingRow.empty) {
            await this.source.v.deleteImmutable(
                this.logger, NOCONTEXT, this.oneTimeEntity.v,
                existingRow.get("_id"));
        }

        // Create a random number between 111000 and 999900 (as a string)
        const code = "" + randomInt(111000, 999900);
        // Censor the email address
        const split = userRow.getString("email").split("@");
        const right = split.length > 1 ? split[1].slice(-5) : "";
        const left = split[0].slice(2);
        const email = `${left}..@..${right}`;
        // Set the expiry to be 5 minutes from now
        const expiry = new Date(Date.now() + RZOOneTimeAdapter.FIVEMINUTES);
        // Create the onetimelogin entity
        const state = await this.oneTimeEntity.v.create(
            NOCONTEXT, this.source.v);
        const validations: Promise<SideEffects>[] = [];
        validations.push(
            this.oneTimeEntity.v.setValue(
                state, "useraccountnum", userNum, NOCONTEXT));
        validations.push(
            this.oneTimeEntity.v.setValue(state, "code", code, NOCONTEXT));
        validations.push(
            this.oneTimeEntity.v.setValue(state, "expiry", expiry, NOCONTEXT));
        await Promise.all(validations);
        await this.oneTimeEntity.v.post(this.source.v, state, NOCONTEXT);
        this.logger.log(
            `One Time Login created for User: ${userNum}, Email: ${email}, ` +
            `Code: ${code}, Expiry: ${expiry}`);
        response.end(JSON.stringify(
            { "user": userNum, "email": email, "expiry": expiry }));
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
                    this.logger, NOCONTEXT, this.oneTimeEntity.v, filter);
                if (row.empty) {
                    this.logger.error(
                        `OTL Login attempt blocked due no OTL found for ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                const code = authRow.get("code");
                if (code != inputCode) {
                    this.logger.error(
                        `OTL Login attempt blocked due to code mismatch for ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                const expiry = authRow.get("expiry");
                if (Date.now() > expiry) {
                    this.logger.error(
                        `OTL Login expired: ${expiry}, ` +
                        `username: ${useraccountnum}`);
                    throw new AuthenticationError("Authentication error", 403);
                }
                this.logger.info(
                    `OTL Authentication successful for username: ` +
                    `${useraccountnum}`);
                const userId = authRow.getString("useraccountnum_id");
                const sessionRow =
                    await this.sessionBackend.v.createSession(
                        this.logger,
                        userId,
                        new Date(Date.now() + RZOOneTimeAdapter.FIVEMINUTES),
                        this.persona.v);

                const sessionContext = new SessionContext(
                    sessionRow, this.persona.v);
                this.sessionCache.v.set(
                    sessionContext.sessionId, sessionContext);

                // Delete the login record for this user, if it exists
                this.policyConfig.v.guardResource(
                    sessionContext, "entity/login", "delete");
                const loginRow = await this.source.v.getQueryOne(
                    this.logger, sessionContext, this.loginEntity.v, filter);
                if (!loginRow.empty) {
                    this.policyConfig.v.guardRow(
                        sessionContext, "entity/login", "delete", loginRow);
                    await this.source.v.deleteImmutable(
                        this.logger, sessionContext, this.loginEntity.v,
                        loginRow.get("_id"));
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
            sessionContext, this.source.v);
        const validations: Promise<SideEffects>[] = [];
        validations.push(
            this.loginEntity.v.setValue(
                state, "useraccountnum", sessionContext.userAccount,
                sessionContext));
        validations.push(
            this.loginEntity.v.setValue(
                state, "password", passwd, sessionContext));
        await Promise.all(validations);
        const resultRow = await this.loginEntity.v.post(
            this.source.v, state, sessionContext);
        // Delete the One Time session
        this.sessionCache.v.delete(sessionContext.sessionId);
        // Session expired, delete it, no need to 'await' it.
        this.sessionBackend.v.deleteSession(
            this.logger, sessionContext.sessionId);
        resultRow.delete("password");
        response.end(JSON.stringify(Row.rowToData(resultRow)));
    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
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
        if (request.method == "GET") {
            /* https:/host/
             *             0     1         2
             * GET        otl    ?    user=WILSONB
             */
            if (uriElements.length == 3 && uriElements[1] == "?" &&
                uriElements[2].startsWith("user=")) {
                const userNum = uriElements[2].substring("user=".length);
                if (userNum) {
                    this.createOneTimeLogin(userNum, response)
                    .catch((error) => {
                        AdapterError.toResponse(this.logger, error, response);
                    });
                } else {
                    console.log(`Missing user in uriElements: ${uriElements}`);
                    throw new AuthenticationError("Authentication error", 500);
                }
            } else {
                console.log(`Invalid uriElements: ${uriElements}`);
                throw new AuthenticationError("Authentication error", 500);
            }
        } else if (request.method == "POST" || request.method == "PUT") {
            this.handlePayload(request, response, uriElements);
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
                    this.logger, NOCONTEXT, this.loginEntity.v, filter);
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
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
                                   id?: string): Promise<void> {
        const row = Row.dataToRow(payload);
        const userId = await rzoAuthenticate(
            this.logger, this.source.v, this.loginEntity.v, row);
        const sessionRow = await this.sessionBackend.v.createSession(
            this.logger, userId);

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

    async handleDelete(request: IncomingMessage,
                       response: ServerResponse): Promise<void> {
        const sessionId = getHeader(request.headers, "rzo-sessionid");
        if (sessionId) {
            try {
                const sessionContext = await this.pullContext(request);
                this.sessionCache.v.delete(sessionContext.sessionId);
                /* For an explicit logout, we wait for the session
                 * to be deleted.
                 */
                await this.sessionBackend.v.deleteSession(
                    this.logger, sessionContext.sessionId);
                console.log(
                    `Logged out user: ${sessionContext.userAccount}`);
            } catch (error) {
                console.log(
                    `Logout action fails silently for session ID ` +
                    `${sessionId}`);
                console.log(error);
            }
        } else {
            console.log(
                "logout action fails silently because there is no " +
                "'rzo-sessionid' present");
        }
        response.end();
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        try {
            switch (request.method) {
                case "POST":
                    this.handlePayload(request, response, uriElements);
                    break;
                case "DELETE":
                    this.handleDelete(request, response);
                    break;
                default:
                    throw new AdapterError(
                        `Invalid Login request method: ${request.method}`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
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
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
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

