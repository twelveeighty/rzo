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
    IContext, Row, _IError, TypeCfg, IConfiguration, BigDecimal, Entity,
    ImmutableEntity, Cfg, EntitySpec, Nobody, Epilogue, EpilogueColumn,
    EpilogueColumnOperator, GeneratorField, IService, Phase, State, Filter,
    BizTrans, SideEffects, IReadOnlyService, BooleanField,
    ManualGeneratorField, Field, LocalDay
} from "../base/core.js";

class AcctingError extends _IError {
    constructor(message: string, options?: ErrorOptions) {
        super(500, message, options);
    }
}

export type ChangeType = "Dr" | "Cr";

export class DocumentBuilder {
    finDocEntity: FinDoc;
    context: IContext;
    service: IService;
    bt: BizTrans;
    doc: State | null;
    splits: State[];
    docnumField: ManualGeneratorField;
    splitnumField: ManualGeneratorField;

    constructor(finDocEntity: FinDoc, bt: BizTrans, context: IContext,
                service: IService) {
        this.finDocEntity = finDocEntity;
        this.docnumField = this.castGeneratorField(
            finDocEntity.getField("docnum"));
        this.splitnumField = this.castGeneratorField(
            finDocEntity.splitEntity.v.getField("splitnum"));
        this.context = context;
        this.service = service;
        this.bt = bt;
        this.splits = [];
        this.doc = null;
    }

    private castGeneratorField(field: Field) : ManualGeneratorField {
        if (!(field instanceof ManualGeneratorField)) {
            throw new AcctingError(
                `Field ${field.fqName} is not a ManualGeneratorField instance`);
        }
        return <ManualGeneratorField>field;
    }

    reset(bt: BizTrans): void {
        this.doc = null;
        this.bt = bt;
        this.splits = [];
    }

    async document(scaffold: Row): Promise<State> {
        const entity = this.finDocEntity;
        const state = await entity.create(this.context, this.service);
        const se: Promise<SideEffects>[] = [];
        entity.cpSetValue(state, "account", scaffold.get("account"),
                          this.context, se);
        entity.cpSetValue(state, "entityid",
                          scaffold.has("entityid") ?
                              scaffold.get("entityid") : null,
                          this.context, se);
        entity.cpSetValue(state, "memo", scaffold.get("memo"),
                          this.context, se);
        entity.cpSetValue(state, "posted", scaffold.get("posted"),
                          this.context, se);
        entity.cpSetValue(state, "created",
                          scaffold.has("created") ?
                              scaffold.get("created") : null,
                          this.context, se);
        await Promise.all(se);
        this.doc = state;
        return state;
    }

    async split(scaffold: Row): Promise<State> {
        const entity = this.finDocEntity.splitEntity.v;
        const state = await entity.create(this.context, this.service);
        state.bizTrans = this.bt;
        const se: Promise<SideEffects>[] = [];
        entity.cpSetValue(state, "account", scaffold.get("account"),
                          this.context, se);
        entity.cpSetValue(state, "change", scaffold.get("change"),
                          this.context, se);
        entity.cpSetValue(state, "quantity",
                          scaffold.has("quantity") || scaffold.has("price") ?
                              scaffold.get("quantity") : null,
                          this.context, se);
        entity.cpSetValue(state, "price",
                          scaffold.has("quantity") || scaffold.has("price") ?
                              scaffold.get("price") : null,
                          this.context, se);
        entity.cpSetValue(state, "amount", scaffold.get("amount"),
                          this.context, se);
        entity.cpSetValue(state, "memo", scaffold.get("memo"),
                          this.context, se);
        await Promise.all(se);
        this.splits.push(state);
        return state;
    }

    private total(change: ChangeType): BigDecimal {
        let total = new BigDecimal("0");
        for (const split of this.splits) {
            if (split.field("change").value == change) {
                total = total.add(
                    BigDecimal.ensure(split.field("amount").value));
            }
        }
        return total;
    }

    private async balanceSplits(): Promise<void> {
        const drTotal = this.total("Dr");
        const crTotal = this.total("Cr");
        if (!drTotal.equals(crTotal)) {
            throw new AcctingError(
                `Splits total debits ${drTotal.toString()} does not equal ` +
                `credits ${crTotal.toString()}`);
        }
        await this.finDocEntity.setValue(
            State.must(this.doc), "amount", drTotal, this.context);
    }

    async postBizTrans(): Promise<BizTrans> {
        await this.balanceSplits();
        const findoc = State.must(this.doc);
        const docnum = await this.docnumField.createValue(
            findoc, this.context, this.service);
        const be = await this.finDocEntity.postBizTrans(
            this.bt, this.service, findoc, this.context);
        if (!be.row.has("_id")) {
            be.row.add("_id", Entity.generateId());
        }
        const splitEntity = this.finDocEntity.splitEntity.v;
        for (const split of this.splits) {
            await this.splitnumField.createValue(
                split, this.context, this.service);
            await splitEntity.setValue(
                split, "findoc", docnum, this.context);
            await splitEntity.postBizTrans(
                this.bt, this.service, split, this.context);
        }
        return this.bt;
    }
}


export class FinDoc extends ImmutableEntity {
    splitEntity: Cfg<Entity>;
    docnumField: Cfg<GeneratorField>;
    splitnumField: Cfg<GeneratorField>;

    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.splitEntity = new Cfg("split");
        this.docnumField = new Cfg("findoc.docnum");
        this.splitnumField = new Cfg("split.splitnum");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.splitEntity.v =
            configuration.getEntity(this.splitEntity.name);
        this.docnumField.setIfCast(
            `${this.name}: configuration error: 'docnum' `,
            configuration.getField(this.docnumField.name),
            GeneratorField);
        this.splitnumField.setIfCast(
            `${this.name}: configuration error: 'splitnum' `,
            configuration.getField(this.splitnumField.name),
            GeneratorField);
    }
}

export class Split extends ImmutableEntity {
    accountBalEntity: Cfg<Entity>;
    holdingEntity: Cfg<Entity>;
    balanceLogEntity: Cfg<Entity>;
    lastBalanceLogEntity: Cfg<Entity>;

    static balanceOperator(row: Row): EpilogueColumnOperator {
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

    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.accountBalEntity = new Cfg("accountbalance");
        this.holdingEntity = new Cfg("holding");
        this.balanceLogEntity = new Cfg("balancelog");
        this.lastBalanceLogEntity = new Cfg("lastbalancelog");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.accountBalEntity.v =
            configuration.getEntity(this.accountBalEntity.name);
        this.holdingEntity.v =
            configuration.getEntity(this.holdingEntity.name);
        this.balanceLogEntity.v =
            configuration.getEntity(this.balanceLogEntity.name);
        this.lastBalanceLogEntity.v =
            configuration.getEntity(this.lastBalanceLogEntity.name);
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

    hasEpilogue(row?: Row): boolean {
        return true;
    }

    epilogue(row: Row, service?: IReadOnlyService,
             context?: IContext): Epilogue[] {
        /* Inbound: row => split,
         * Outbound:
         *           balancelog delete (if account is 'islogged' and 'posted'
         *                              before the accounting day period's
         *                              midnight)
         *           lastbalancelog update (if account is 'islogged' and 'posted'
         *                                  before the accounting day period's
         *                                  midnight)
         *           accountbalance update (always)
         *           balancelog post (always)
         *           holding update (if 'price' was specified in the split)
         */
        const updatedby = context ? context.userAccountId : (
            row.has("updatedby") ? row.get("updatedby") : Nobody.ID);
        const updated = row.has("updated") ? row.get("updated") : new Date();
        const cols: EpilogueColumn[] = [];
        const operator = Split.balanceOperator(row);
        const amount = Row.transport(row.get("amount"));
        cols.push({
            column: "presentvalue",
            operator: operator,
            value: amount
        });
        if (row.has("quantity") && row.isNotNull("quantity")) {
            cols.push({
                column: "balance",
                operator: operator,
                value: Row.transport(row.get("quantity"))
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
        if (BooleanField.toBoolean(row.get("islogged"))) {
            /* If this transaction is posted before the current local day's
             * midnight we invalidate the balancelogs.
             */
            const todayPeriod = LocalDay.fromDate(new Date()).getPeriod();
            const postedPeriod = LocalDay.fromDate(
                row.get("posted")).getPeriod();
            if (postedPeriod < todayPeriod) {
                if (this.logger.willLog("Debug")) {
                    this.logger.debug(
                        `Acct ${row.get("account")} split posted ` +
                        `${row.get("posted")} before ${todayPeriod} ` +
                        `invalidates balancelog`);
                }
                const deleteFilter = new Filter().
                    op("type", "=", "D").
                    op("period", ">=", `${postedPeriod}`, true);
                const logDelete: Epilogue = {
                    entity: this.balanceLogEntity.v,
                    action: "delete",
                    key: {
                        keyColumn: "account_id",
                        keyValue: row.getString("account_id")
                    },
                    filter: deleteFilter,
                    columns: []
                };
                result.push(logDelete);
                const updateFilter = new Filter().
                    op("type", "=", "D").
                    op("invalidated", "?>", `${postedPeriod}`, true);
                const logUpdate: Epilogue = {
                    entity: this.lastBalanceLogEntity.v,
                    action: "put",
                    key: {
                        keyColumn: "account_id",
                        keyValue: row.getString("account_id")
                    },
                    filter: updateFilter,
                    columns: [
                        { column: "invalidated", value: postedPeriod }
                    ]
                };
                result.push(logUpdate);
            }
        }
        return result;
    }
}

