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

import {
    Logger, IContext, IConfiguration, ClassSpec, TypeCfg, Cfg, Entity
} from "../base/core.js";

import {
    Txn, TxnRaw, IAcctingService, AcctingServiceSource
} from "./acc-core.js";


class AcctingRestClientError extends Error {

    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }

    static fromResponse(response: Response,
                        body?: string): AcctingRestClientError {
        const details = body ? `: ${body}` : "";
        const msg = `HTTP Error: ${response.status} ${response.statusText}` +
            `${details}`;
        return new AcctingRestClientError(msg);
    }
}

export class AcctingRestClient implements IAcctingService {
    readonly url: string;
    accountEntity: Cfg<Entity>;

    constructor(url: string) {
        let finalUrl = url.trim();
        while (finalUrl.endsWith("/")) {
            finalUrl = finalUrl.slice(0, -1);
        }
        this.url = finalUrl;
        this.accountEntity = new Cfg("account");
    }

    configure(configuration: IConfiguration) {
        this.accountEntity.v = configuration.getEntity(this.accountEntity.name);
    }

    async postTxn(logger: Logger, context: IContext, txn: Txn): Promise<Txn> {
        if (!context.sessionId) {
            throw new AcctingRestClientError("Session ID missing");
        }
        const txnAccount = txn.transaction.getString("account");
        const txnRaw: TxnRaw = {
            "transaction": txn.transaction.raw(),
            "splits": txn.splits.getAll()
        }
        const payload = JSON.stringify(txnRaw);
        const headers = new Headers();
        headers.set("rzo-sessionid", context.sessionId);
        headers.set("Content-Type", "application/json");
        const fetchRequest = {
            method: "post",
            body: payload,
            headers: headers
        };
        const targetUrl = this.url + "/f/" + txnAccount;
        logger.info(`fetch POST - ${targetUrl}`);
        const response = await fetch(targetUrl, fetchRequest);
        if (!response.ok) {
            const body = await response.text();
            throw AcctingRestClientError.fromResponse(response, body);
        }
        const data = await response.json();
        return Txn.rawToTxnUnparsed(data);
    }
}

type AcctingRestClientSourceSpec = ClassSpec & {
    url: string;
}

export class AcctingRestClientSource extends AcctingServiceSource {
    url: string;
    _service: AcctingRestClient;

    constructor(config: TypeCfg<AcctingRestClientSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.url = config.spec.url;
        this._service = new AcctingRestClient(this.url);
    }

    configure(configuration: IConfiguration) {
        this._service.configure(configuration);
    }

    get service(): IAcctingService {
        return this._service;
    }
}

