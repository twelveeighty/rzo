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
    Logger, IContext, TypeCfg, ClassSpec, IConfiguration, Cfg, Entity,
    _IError, IService, ServiceSource
} from "../base/core.js";

import {
    AcctingServiceSource, IAcctingService, AccTrans, Txn
} from "../accting/acc-core.js";

import { PgBaseClient } from "./pg-client.js";


type PgAcctingSourceSpec = ClassSpec & {
    source: string;
    pool: string;
}

/*
class PgAcctingError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}
*/

export class PgAccting extends PgBaseClient implements IAcctingService {
    acctransEntity: Cfg<AccTrans>;
    source: Cfg<IService>;
    accountEntity: Cfg<Entity>;

    constructor(spec: PgAcctingSourceSpec) {
        super(spec.pool);
        this.acctransEntity = new Cfg("acctrans");
        this.source = new Cfg(spec.source);
        this.accountEntity = new Cfg("account");
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.accountEntity.v =
            configuration.getEntity(this.accountEntity.name);
        this.acctransEntity.setIfCast(
            `Configuration error: 'acctrans' `,
            configuration.entities.get(this.acctransEntity.name),
            AccTrans);
        this.source.v =
            (<ServiceSource>configuration.getSource(
                this.source.name).ensure(ServiceSource)).service;
    }

    private setCreated(txn: Txn, now: Date): void {
        txn.transaction.updateOrAdd("created", now);
        txn.splits.rewind();
        while (txn.splits.next()) {
            txn.splits.getRow().updateOrAdd("created", now);
        }
    }

    async postTxn(logger: Logger, context: IContext, txn: Txn): Promise<Txn> {
        this.acctransEntity.v.checkBalancedSplits(txn.splits);
        this.acctransEntity.v.checkOrSetIds(txn);
        if (!this.acctransEntity.v.checkNums(txn)) {
            await this.acctransEntity.v.createNums(txn, context, this.source.v);
        }
        this.acctransEntity.v.processPostedDT(txn);
        this.setCreated(txn, new Date());
        const client = await this.pool.connect();
        try {
            let statement = "BEGIN";
            this.log(logger, statement);
            await client.query(statement);
            await this.postEntity(
                logger, context.userAccountId, client, this.acctransEntity.v,
                txn.transaction, context);
            const splitEntity = this.acctransEntity.v.accsplitEntity.v;
            txn.splits.rewind();
            while (txn.splits.next()) {
                await this.postEntity(
                    logger, context.userAccountId, client, splitEntity,
                    txn.splits.getRow(), context);
            }
            statement = "COMMIT";
            this.log(logger, statement);
            await client.query(statement);
            return txn;
        } catch (err: any) {
            const statement = "ROLLBACK";
            this.log(logger, statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

}

export class PgAcctingSource extends AcctingServiceSource {
    _service: PgAccting;

    constructor(config: TypeCfg<PgAcctingSourceSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new PgAccting(config.spec);
    }

    configure(configuration: IConfiguration) {
        this._service.configure(configuration);
    }

    get service(): IAcctingService {
        return this._service;
    }
}


