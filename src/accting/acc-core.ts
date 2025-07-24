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
    Logger, IContext, Row, _IError, Source, TypeCfg, ClassSpec, IConfiguration,
    BigDecimal, JsonObject, Entity, ImmutableEntity, Cfg, EntitySpec,
    IResultSet, MemResultSet, Nobody, Epilogue, EpilogueColumn,
    EpilogueColumnOperator, GeneratorField, IService, Phase, State, Filter
} from "../base/core.js";

class AcctingError extends _IError {
    constructor(message: string, options?: ErrorOptions) {
        super(500, message, options);
    }
}

export type ChangeType = "Dr" | "Cr";

export type Txn = {
    transaction: Row;
    splits: IResultSet;
}

export type TxnRaw = {
    transaction: JsonObject;
    splits: JsonObject[];
}

export type TxnState = {
    transaction: State;
    splits: State[];
}

export interface IAcctingService {
    postTxn(logger: Logger, context: IContext, txn: Txn): Promise<Txn>;
}

export function rawToTxnUnparsed(raw: any): Txn {
    const asRow = Row.dataToRow(raw);
    if (asRow.hasAll(["transaction", "splits"])) {
        const transaction = Row.dataToRow(asRow.get("transaction"));
        const splits = asRow.get("splits");
        if (!transaction.empty && Array.isArray(splits)) {
            const result: Txn = {
                "transaction": transaction,
                "splits": new MemResultSet(splits)
            };
            return result;
        } else {
            throw new AcctingError(
                "Txn alike object cannot be parsed to a Txn");
        }
    } else {
        throw new AcctingError(
            `Object cannot be parsed to a Txn: ${JSON.stringify(raw)}`);
    }
}

export class AcctingServiceSource extends Source {

    constructor(config: TypeCfg<ClassSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
    }

    configure(configuration: IConfiguration): void {
    }

    get service(): IAcctingService {
        throw new AcctingError(
            `AcctingServiceSource ${this.name} has an undefined service`);
    }
}

export class AccTrans extends ImmutableEntity {
    accsplitEntity: Cfg<Entity>;
    transnumField: Cfg<GeneratorField>;
    splitnumField: Cfg<GeneratorField>;

    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.accsplitEntity = new Cfg("accsplit");
        this.transnumField = new Cfg("acctrans.transnum");
        this.splitnumField = new Cfg("accsplit.splitnum");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.accsplitEntity.v =
            configuration.getEntity(this.accsplitEntity.name);
        this.transnumField.setIfCast(
            `${this.name}: configuration error: 'transnum' `,
            configuration.getField(this.transnumField.name),
            GeneratorField);
        this.splitnumField.setIfCast(
            `${this.name}: configuration error: 'splitnum' `,
            configuration.getField(this.splitnumField.name),
            GeneratorField);
    }

    rawToTxn(raw: any): Txn {
        const txn = rawToTxnUnparsed(raw);
        txn.splits.rewind();
        while (txn.splits.next()) {
            this.accsplitEntity.v.transformDataForRow(
                txn.splits.getRow().raw());
        }
        return txn;
    }

    txnToTxnState(txn: Txn): TxnState {
        const splits: State[] = [];
        txn.splits.rewind();
        while (txn.splits.next()) {
            splits.push(this.accsplitEntity.v.rowToState(txn.splits.getRow()));
        }
        const result: TxnState = {
            transaction: this.rowToState(txn.transaction),
            splits: splits
        };
        return result;
    }

    total(splits: IResultSet, change: ChangeType): BigDecimal {
        let total = new BigDecimal("0");
        splits.rewind();
        while (splits.next()) {
            if (splits.get("change") == change) {
                total = total.add(BigDecimal.ensure(splits.get("amount")));
            }
        }
        return total;
    }

    async createNums(txn: Txn, context: IContext,
                     service: IService): Promise<void> {
        const transnum = await this.transnumField.v.generate(service, context);
        txn.transaction.updateOrAdd("transnum", transnum);
        txn.splits.rewind();
        while (txn.splits.next()) {
            const row = txn.splits.getRow();
            row.updateOrAdd("acctrans", transnum);
            const splitnum = await this.splitnumField.v.generate(
                service, context);
            row.updateOrAdd("splitnum", splitnum);
        }
    }

    checkNums(txn: Txn): boolean {
        /* Check if the passed Txn has transnum specified. If so, all splits'
         * corresponding acctrans, as well as all splitnum's must also be
         * specified.
         */
        if (txn.transaction.has("transnum") &&
                txn.transaction.isNotNullish("transnum")) {
            const transnum = txn.transaction.getString("transnum");
            txn.splits.rewind();
            while (txn.splits.next()) {
                const row = txn.splits.getRow();
                if (!row.has("acctrans") || !row.has("splitnum") ||
                        row.isNullish("acctrans") ||
                        row.get("acctrans") != transnum) {
                    throw new AcctingError(
                        "Invalid Txn split: missing or mismatched splitnum " +
                        "and/or transnum");
                }
            }
            return true;
        } else {
            return false;
        }
    }

    checkOrSetIds(txn: Txn): void {
        /* Check if the passed Txn has _id specified. If so, the
         * corresponding acctrans_id, as well as all accsplit's _id
         * must also be specified.
         * If _id is not specified, all id's will get created.
         */
        if (txn.transaction.has("_id") && txn.transaction.get("_id")) {
            const acctrans_id = txn.transaction.getString("_id");
            txn.splits.rewind();
            while (txn.splits.next()) {
                const row = txn.splits.getRow();
                if (!row.has("_id") || !row.has("acctrans_id") ||
                    row.isNullish("_id") ||
                    row.get("acctrans_id") != acctrans_id) {
                    throw new AcctingError(
                        "Invalid Txn split: missing or mismatched _id " +
                        "and/or acctrans_id");
                }
            }
        } else {
            const acctrans_id = Entity.generateId();
            txn.transaction.updateOrAdd("_id", acctrans_id);
            txn.splits.rewind();
            while (txn.splits.next()) {
                const row = txn.splits.getRow();
                row.updateOrAdd("_id", Entity.generateId());
                row.updateOrAdd("acctrans_id", acctrans_id);
            }
        }
    }

    processPostedDT(txn: Txn): void {
        if (!txn.transaction.has("posted")) {
            throw new AcctingError("Transaction is missing 'posted' date");
        }
        const posted = txn.transaction.get("posted");
        txn.splits.rewind();
        while (txn.splits.next()) {
            txn.splits.getRow().updateOrAdd("posted", posted);
        }
    }

    balanceSplits(txn: Txn): void {
        const drTotal = this.total(txn.splits, "Dr");
        const crTotal = this.total(txn.splits, "Cr");
        if (!drTotal.equals(crTotal)) {
            throw new AcctingError(
                `Splits total debits ${drTotal.toString()} does not equal ` +
                `credits ${crTotal.toString()}`);
        }
        txn.transaction.updateOrAdd("amount", drTotal);
    }
}

export class AccSplit extends ImmutableEntity {
    accountBalEntity: Cfg<Entity>;
    holdingEntity: Cfg<Entity>;

    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.accountBalEntity = new Cfg("accountbalance");
        this.holdingEntity = new Cfg("holding");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.accountBalEntity.v =
            configuration.getEntity(this.accountBalEntity.name);
        this.holdingEntity.v =
            configuration.getEntity(this.holdingEntity.name);
    }

    async validate(phase: Phase, state: State,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, context);
        const quantState = state.field("quantity");
        const priceState = state.field("price");
        const amountState = state.field("amount");
        if (quantState.isNotNull || priceState.isNotNull) {
            if (quantState.isNull || priceState.isNull) {
                throw new AcctingError(
                    "Both 'quantity' and 'price' must be specified, or " +
                    "otherwise none of them");
            }
            const result = BigDecimal.ensure(quantState.value).multiply(
                BigDecimal.ensure(priceState.value));
            if (!result.equals(BigDecimal.ensure(amountState.value))) {
                throw new AcctingError(
                    `amount ${amountState.value} does not match ` +
                    `quantity * price = ${result}`);
            }
        }
    }

    balanceOperator(row: Row): EpilogueColumnOperator {
        const change = row.get("change");
        const elementtype = row.get("elementtype");
        if (change != "Dr" && change != "Cr") {
            throw new AcctingError(`Invalid change: ${change}`);
        }
        switch(elementtype) {
            case "ASSET":
            case "EXPENSE":
                return change == "Dr" ? "+=" : "-=";
            case "LIABILITY":
            case "EQUITY":
            case "INCOME":
                return change == "Dr" ? "-=" : "+=";
            default:
                throw new AcctingError(`Invalid elementtype: ${elementtype}`);
        }
    }

    hasEpilogue(row?: Row): boolean {
        return true;
    }

    epilogue(row: Row, context?: IContext): Epilogue[] {
        /* Inbound: row => accsplit,
         * Outbound:
         *           accountbalance update (always)
         *           holding update (if 'price' was specified in the accsplit)
         */
        const updatedby = context ? context.userAccountId : (
            row.has("updatedby") ? row.get("updatedby") : Nobody.ID);
        const updated = row.has("updated") ? row.get("updated") : new Date();
        const cols: EpilogueColumn[] = [];
        const operator = this.balanceOperator(row);
        const amount = row.get("amount");
        cols.push({
            column: "presentvalue",
            operator: operator,
            value: amount
        });
        if (row.has("quantity") && row.isNotNull("quantity")) {
            cols.push({
                column: "balance",
                operator: operator,
                value: row.get("quantity")
            });
        } else {
            cols.push({
                column: "balance",
                operator: operator,
                value: amount
            });
        }
        cols.push({ column: "updatedby", value: updatedby });
        cols.push({ column: "updated", value: updated });
        const balanceUpdate: Epilogue = {
            entity: this.accountBalEntity.v,
            action: "put",
            key: {
                keyColumn: "account_id",
                keyValue: row.getString("account_id")
            },
            columns: cols
        };
        const result: Epilogue[] = [balanceUpdate];
        if (row.has("price") && row.isNotNull("price")) {
            const price = row.get("price");
            const holdingUpdate: Epilogue = {
                entity: this.holdingEntity.v,
                action: "put",
                key: {
                    keyColumn: "_id",
                    keyValue: row.getString("holding_id")
                },
                filter: new Filter().op("price", "!=", `${price}`, true),
                columns: [
                    { column: "price", value: price }
                ]
            };
            result.push(holdingUpdate);

        }
        return result;
    }
}

