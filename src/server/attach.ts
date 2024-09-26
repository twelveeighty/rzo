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
    _IError, Entity, Cfg, TypeCfg, IConfiguration, Row, IContext, Attachment,
    Attachments, Logger, Source, ClassSpec
} from "../base/core.js";

import {
    AdapterError, SessionAwareAdapter, SessionAwareAdapterSpec, getHeader
} from "./adapter.js";

class AttachError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export const ATTACH_TABLE = "local_attachments";

export function attachTableDDL(dropFirst?: boolean): string {
    const drop   = `
drop table if exists ${ATTACH_TABLE};
`   ;
    const create = `
create table ${ATTACH_TABLE} (
   hash                    char(64) primary key,
   name                    text not null,
   data                    bytea not null
);

`   ;
    const fullCreate = dropFirst ? drop + create : create;
    return fullCreate;
}

export interface IAttachService {
    get isAttachService(): boolean;
    put(logger: Logger, context: IContext, entity: Entity, id: string,
        rev: string, name: string, data: Buffer,
        mimeType?: string): Promise<Row>;
    get(logger: Logger, context: IContext,
        attObject: Attachment): Promise<Buffer>;
}

export class AttachSource extends Source {

    constructor(config: TypeCfg<ClassSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
    }

    configure(configuration: IConfiguration): void {
    }

    get service(): IAttachService {
        throw new AttachError(
            `AttachSource ${this.name} has an undefined service`);
    }
}

type AttachAdapterSpec = SessionAwareAdapterSpec & {
    attachSource: string;
}

export class AttachAdapter extends SessionAwareAdapter {
    entities: Cfg<Map<string, Entity>>;
    attachSource: Cfg<IAttachService>;

    constructor(config: TypeCfg<AttachAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.entities = new Cfg("entities");
        this.attachSource = new Cfg(config.spec.attachSource);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.entities.v = configuration.entities;
        this.attachSource.v = (<AttachSource>configuration.getSource(
            this.attachSource.name).ensure(AttachSource)).service;

    }

    protected handleBinPayload(request: IncomingMessage,
                               response: ServerResponse,
                               uriElements: string[],
                               entityName: string): void {
        const chunks: Uint8Array[] = [];
        request.on("data", (chunk) => {
            chunks.push(chunk);
        });
        request.on("end", () => {
            try {
                const payload = Buffer.concat(chunks);
                this.binPayloadHandler(
                    payload, request, response, uriElements, entityName)
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

    protected async binPayloadHandler(payload: Buffer,
                                      request: IncomingMessage,
                                      response: ServerResponse,
                                      uriElements: string[],
                                      entityName: string): Promise<void> {
        /* https:/host/
         *             0   1       2     3      4    5
         * PUT         a entity  uuid name.ext  ?   rev=1-xxx  Add attachment
         */
        if (uriElements.length != 6 || uriElements[4] != "?") {
            throw new AttachError("Invalid Attach PUT uri");
        }
        if (!(uriElements[5].startsWith("rev="))) {
            throw new AttachError("Invalid Attach PUT uri - rev");
        }
        const context = await this.pullContext(request);
        const id = uriElements[2];
        const attachName = decodeURIComponent(uriElements[3]);
        const rev = uriElements[5].substring("rev=".length);
        const entity = this.entities.v.get(entityName);
        if (!entity) {
            throw new AttachError(`Invalid entity: ${entityName}`, 404);
        }
        const policyTarget = `entity/${entityName}`;
        if (request.method == "PUT") {
            this.policyConfig.v.guardResource(context, policyTarget, "put");
            const mimeType = getHeader(request.headers, "content-type");
            const row = await this.attachSource.v.put(
                this.logger, context, entity, id, rev, attachName, payload,
                mimeType);
            if (!row.empty) {
                response.end(JSON.stringify(row.raw()));
            } else {
                throw new AttachError(
                    `NotFound, NotLeaf, or InConflict entity: ` +
                    `${entityName}, id: ${id}, rev: ${rev}`, 404);
            }
        } else {
            throw new AttachError(`Invalid request method: ${request.method}`);
        }
    }

    async getOne(context: IContext, entity: Entity, id: string,
                 rev?: string) : Promise<Row> {
        const resource = `entity/${entity.name}`;
        this.policyConfig.v.guardResource(context, resource, "get");
        const row = await this.source.v.getOne(
            this.logger, context, entity, id, rev);
        if (!row.empty) {
            this.policyConfig.v.guardRow(context, resource, "get", row);
            return row;
        } else {
            return row;
        }
    }

    async handleGet(entityName: string, request: IncomingMessage,
                    response: ServerResponse,
                    uriElements: string[]): Promise<void> {
        /* https:/host/
         *             0   1       2     3      4    5
         * GET         a entity  uuid name.ext                 Get attachment
         * GET         a entity  uuid name.ext  ?   rev=1-xxx  Get attachment
         */
        try {
            const entity = this.entities.v.get(entityName);
            if (!entity) {
                throw new AttachError(`Invalid entity: ${entityName}`, 404);
            }
            const context = await this.pullContext(request);
            const len = uriElements.length;
            if (len == 4 || (len == 6 && uriElements.at(4) == "?" &&
                 uriElements[5].startsWith("rev="))) {
                const id = uriElements[2];
                const rev = len == 6 ? uriElements[5].substring("rev=".length)
                    : undefined;
                const row = await this.getOne(context, entity, id, rev);
                if (!row.empty) {
                    const attname = uriElements[3];
                    const atts = row.get("_att") as Attachments;
                    if (atts) {
                        const attObj = atts.att.find((att) => att.n == attname);
                        if (attObj) {
                            const buf = await this.attachSource.v.get(
                                this.logger, context, attObj);
                            if (buf.length) {
                                response.setHeader("Content-Type", attObj.m);
                                response.end(buf);
                            } else {
                                throw new AttachError(
                                    `NotFound digest: ${attObj.d}`, 404);
                            }
                        } else {
                            throw new AttachError(
                                `NotFound ${entity.name} : ${id} : ${attname}`,
                                404);
                        }
                    } else {
                        throw new AttachError(
                            `NotFound ${entity.name} : ${id} : ${attname}`,
                            404);
                    }
                } else {
                    throw new AttachError(
                        `NotFound ${entity.name} : ${id}`, 404);
                }
            } else {
                throw new AttachError(
                    `Invalid request: invalid URI components for GET`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        this.logger.info(`${request.method} - ${request.url}`);
        try {
            if (uriElements.length < 4) {
                throw new AdapterError("Invalid Attachment request");
            }
            const entityName = uriElements[1];
            switch (request.method) {
                case "GET":
                    this.handleGet(entityName, request, response, uriElements);
                    break;
                case "PUT":
                    this.handleBinPayload(
                        request, response, uriElements, entityName);
                    break;
                default:
                    throw new AttachError(
                        `Invalid Replicate request: ${request.method}`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }
}

