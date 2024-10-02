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
    Entity, IConfiguration, IContext, Row, TypeCfg, ClassSpec, _IError, Logger,
    Attachment, Attachments
} from "../base/core.js";

import { MvccController } from "./mvcc.js";
import { PgBaseClient } from "./pg-client.js";

import { ATTACH_TABLE, IAttachService, AttachSource } from "./attach.js";


type PgAttachSourceSpec = ClassSpec & {
    pool: string;
}

export class PgAttach extends PgBaseClient implements IAttachService {
    private mvccLogger: Logger;
    private mvccController: MvccController;

    constructor(spec: PgAttachSourceSpec) {
        super(spec.pool);
        this.mvccLogger = new Logger("server/mvcc");
        this.mvccController = new MvccController(this.mvccLogger);
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
    }

    get isAttachService(): boolean {
        return true;
    }

    async put(logger: Logger, context: IContext, entity: Entity, id: string,
              rev: string, name: string, data: Buffer,
              mimeType?: string): Promise<Row> {
        const row = await this.getOne(logger, context, entity, id, rev);
        if (row.empty) {
            return row;
        }
        if (row.has("_conflict") || row.has("_notleaf") ||
            row.has("_deleted")) {
            return new Row();
        }
        const hashArrayBuffer = await crypto.subtle.digest("SHA-256", data);
        const hash = Buffer.from(hashArrayBuffer).toString("hex");
        const attObj = {
            n: name,
            m: mimeType || "application/octet-stream",
            d: hash,
            l: data.length,
            r: this.mvccController.versionDepth(row.get("_rev")) + 1
        };
        const atts: Attachments = row.get("_att") || { att: [] };
        // Filter out existing attachment(s) with the same name.
        const newAtt = atts.att.filter((rec) => rec.n != name);
        newAtt.push(attObj);
        atts.att = newAtt;
        row.put("_att", atts);
        /* Check the attachment table by hash, since this attachment could
         * already exist. In that case, we do not add another with the same
         * hash.
         */
        let statement = `select hash from ${ATTACH_TABLE} where hash = \$1`;
        let parameters: any[] = [hash];
        this.log(logger, statement, parameters);
        let result = await this.pool.query(statement, parameters);
        const addAttachment = result.rows.length == 0;
        const versions = await this.pullVcTable(logger, entity, id);
        const mvccResult = this.mvccController.putMvcc(
            row, versions, false, context);
        const client = await this.pool.connect();
        try {
            statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);

            if (addAttachment) {
                statement =
                    `insert into ${ATTACH_TABLE} (hash, name, data) ` +
                    `values (\$1, \$2, decode(\$3, 'base64'))`;
                parameters = [hash, name, data.toString("base64")];
                this.log(logger, statement);
                await client.query(statement, parameters);
            }

            await this.applyMvccResults(logger, client, entity, mvccResult);

            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);

            return Row.must(mvccResult.leafTable.leafActionPut?.payload);
        } catch (err: any) {
            statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

    async get(logger: Logger, context: IContext,
              attObject: Attachment): Promise<Buffer> {
        const statement =
            `select encode(data, 'base64') "data" from ${ATTACH_TABLE} ` +
            `where hash = \$1`;
        const parameters = [attObject.d];
        this.log(logger, statement, parameters);
        const result = await this.pool.query(statement, parameters);
        if (result.rows.length) {
            return Buffer.from(result.rows[0].data, "base64");
        } else {
            return Buffer.from([]);
        }
    }
}

export class PgAttachSource extends AttachSource {
    _service: PgAttach;

    constructor(config: TypeCfg<PgAttachSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new PgAttach(config.spec);
    }

    configure(configuration: IConfiguration) {
        this._service.configure(configuration);
    }

    get service(): IAttachService {
        return this._service;
    }
}

