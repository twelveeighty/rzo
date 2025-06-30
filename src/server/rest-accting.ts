/*
    RZO - A Business Application Framework

    Copyright (C) 2025 Frank Vanderham

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
    _IError, Cfg, TypeCfg, IConfiguration, JsonObject
} from "../base/core.js";

import {
    AdapterError, SessionAwareAdapter, SessionAwareAdapterSpec
} from "./adapter.js";

import {
    IAcctingService, AcctingServiceSource, AccTrans, TxnRaw
} from "../accting/acc-core.js";

class RestAcctingError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

type AcctingAdapterSpec = SessionAwareAdapterSpec & {
    acctingSource: string;
}

export class AcctingAdapter extends SessionAwareAdapter {
    acctransEntity: Cfg<AccTrans>;
    acctingSource: Cfg<IAcctingService>;

    constructor(config: TypeCfg<AcctingAdapterSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.acctransEntity = new Cfg("acctrans");
        this.acctingSource = new Cfg(config.spec.acctingSource);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.acctransEntity.setIfCast(
            `${this.name}: configuration error: 'acctrans' `,
            configuration.entities.get(this.acctransEntity.name),
            AccTrans);
        this.acctingSource.v = (<AcctingServiceSource>configuration.getSource(
            this.acctingSource.name).ensure(AcctingServiceSource)).service;

    }

    protected async payloadHandler(payload: JsonObject,
                                   request: IncomingMessage,
                                   response: ServerResponse,
                                   uriElements: string[],
                                   resource?: string,
                                   id?: string): Promise<void> {
        const context = await this.pullContext(request);
        this.policyConfig.v.guardResource(context, "entity/acctrans", "post");
        this.policyConfig.v.guardResource(context, "entity/accsplit", "post");
        const txn = this.acctransEntity.v.rawToTxn(payload);
        if (!txn.transaction || txn.transaction.empty) {
            throw new RestAcctingError("Missing 'transaction' in payload");
        }
        if (!txn.splits || !txn.splits.rowCount) {
            throw new RestAcctingError("Missing 'splits' in payload");
        }
        this.policyConfig.v.guardRow(
            context, "entity/acctrans", "post", txn.transaction);
        txn.splits.rewind();
        while (txn.splits.next()) {
            this.policyConfig.v.guardRow(
                context, "entity/accsplit", "post", txn.splits.getRow());
        }
        const resultTxn = await this.acctingSource.v.postTxn(
            this.logger, context, txn);
        const txnRaw: TxnRaw = {
            transaction: resultTxn.transaction.raw(),
            splits: resultTxn.splits.getAll()
        };
        response.end(JSON.stringify(txnRaw));
    }

    handle(request: IncomingMessage, response: ServerResponse,
           uriElements: string[]): void {
        this.logger.info(`${request.method} - ${request.url}`);
        try {
            if (uriElements.length < 1) {
                throw new AdapterError("Invalid Accting request uriElements");
            }
            const account = uriElements[1];
            switch (request.method) {
                case "POST":
                    this.handlePayload(
                        request, response, uriElements, account);
                    break;
                default:
                    throw new RestAcctingError(
                        `Invalid Accting request: ${request.method}`);
            }
        } catch (error) {
            AdapterError.toResponse(this.logger, error, response);
        }
    }
}

