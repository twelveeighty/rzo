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
    Logger, IContext, Row, _IError, TypeCfg, IConfiguration,
    BigDecimal, JsonObject, Entity, ImmutableEntity, Cfg, EntitySpec,
    Nobody, Epilogue, EpilogueColumn, EpilogueColumnOperator,
    GeneratorField, IService, Phase, State, Filter, BizTrans, SideEffects,
    CoreColumns, IReadOnlyService, BooleanField
} from "../base/core.js";

class AcctingError extends _IError {
    constructor(message: string, options?: ErrorOptions) {
        super(500, message, options);
    }
}

export type ChangeType = "Dr" | "Cr";

/* LocalDay is used to capture a *local* day, which can be 23, 24 or 25 hours,
 * depending on Daylight Savings.
 */
export class LocalDay {
    // The local day's midnight in utc.
    utc: Date;

    static fromPeriodString(period: string): LocalDay {
        return new LocalDay(
            Number.parseInt(period.slice(0, 4)),
            Number.parseInt(period.slice(4, -2)),
            Number.parseInt(period.slice(-2))
        );
    }

    static fromPeriod(period: number): LocalDay {
        return LocalDay.fromPeriodString(`${period}`);
    }

    static toPostgresDate(period: number): string {
        const asStr = "" + period;
        if (asStr.length != "YYYYMMDD".length) {
            throw new AcctingError(
                `Invalid period number: ${period}`);
        }
        return `${asStr.slice(0, 4)}-${asStr.slice(4, 6)}-${asStr.slice(6)}`;
    }

    static fromDate(ts: Date): LocalDay {
        const cpy = new Date(ts);
        cpy.setHours(0, 0, 0, 0);
        return new LocalDay(cpy.getFullYear(), cpy.getMonth()+1, cpy.getDate());
    }

    /* This converts from the confusing <input type="date" ../> valueAsDate()
     * return value. That element's valueAsDate() returns the selected date to
     * midnight *in UTC* for the selected month/day/year.
     */
    static fromUtc(ts: Date): LocalDay {
        const cpy = new Date(ts);
        cpy.setUTCHours(0, 0, 0, 0);
        return new LocalDay(
            cpy.getUTCFullYear(), cpy.getUTCMonth()+1, cpy.getUTCDate());
    }

    constructor(year: number, month12: number, day: number) {
        if (!Number.isSafeInteger(year) || year > 2100 || year < 1971) {
            throw new AcctingError(`'${year}' is not a valid year`);
        }
        if (!Number.isSafeInteger(month12) || month12 > 12 || month12 < 1) {
            throw new AcctingError(`'${month12}' is not a valid month`);
        }
        if (!Number.isSafeInteger(day) || day > 31 || day < 1) {
            throw new AcctingError(`'${day}' is not a valid day`);
        }
        this.utc = new Date(year, month12-1, day);
    }

    increment(by: number): LocalDay {
        const inUtc = new Date(this.utc);
        inUtc.setDate(this.utc.getDate() + by);
        return new LocalDay(
            inUtc.getFullYear(), inUtc.getMonth()+1, inUtc.getDate());
    }

    previous(): LocalDay {
        return this.increment(-1);
    }

    next(): LocalDay {
        return this.increment(1);
    }

    getPeriod(): number {
        return this.utc.getFullYear()*10000 + (this.utc.getMonth()+1)*100 +
            this.utc.getDate();
    }

    getPeriodString(): string {
        return `${this.getPeriod()}`;
    }
}

export class Txn {
    acctrans: AccTrans;
    transaction: State;
    splits: State[];

    constructor(entity: AccTrans, state: State) {
        this.acctrans = entity;
        this.transaction = state;
        this.splits = [];
    }

    total(change: ChangeType): BigDecimal {
        let total = new BigDecimal("0");
        for (const split of this.splits) {
            if (split.field("change").value == change) {
                total = total.add(
                    BigDecimal.ensure(split.field("amount").value));
            }
        }
        return total;
    }

    private setCreated(now: Date): void {
        this.transaction.field("created").value = now;
        for (const split of this.splits) {
            split.field("created").value = now;
        }
    }

    private async validate(context: IContext): Promise<void> {
        const accsplit = this.acctrans.accsplitEntity.v;
        const validations: Promise<void>[] = [];
        validations.push(this.acctrans.validate(
            "create", this.transaction, context));
        for (const split of this.splits) {
            validations.push(accsplit.validate("create", split, context));
        }
        await Promise.all(validations);
    }

    private async activate(context: IContext): Promise<void> {
        const accsplit = this.acctrans.accsplitEntity.v;
        const activations: Promise<SideEffects[]>[] = [];
        activations.push(this.acctrans.activate(
            "create", this.transaction, context));
        for (const split of this.splits) {
            activations.push(accsplit.activate("create", split, context));
        }
        await Promise.all(activations);
    }

    balanceSplits(): void {
        const drTotal = this.total("Dr");
        const crTotal = this.total("Cr");
        if (!drTotal.equals(crTotal)) {
            throw new AcctingError(
                `Splits total debits ${drTotal.toString()} does not equal ` +
                `credits ${crTotal.toString()}`);
        }
        this.transaction.field("amount").value = drTotal;
    }

    checkOrSetIds(): void {
        /* Check _id is specified. If so, the
         * corresponding acctrans_id, as well as all accsplit's _id
         * must also be specified.
         * If _id is not specified, all id's will get created.
         */
        if (this.transaction.hasId()) {
            const acctransId = this.transaction.id;
            for (const split of this.splits) {
                if (!split.hasId() ||
                    split.field("acctrans_id").value != acctransId) {
                } else {
                    throw new AcctingError(
                        "Invalid Txn split: missing or mismatched _id " +
                        "and/or acctrans_id");
                }
            }
        } else {
            const acctransId = Entity.generateId();
            this.transaction.core = new CoreColumns(acctransId, null);
            for (const split of this.splits) {
                split.core = new CoreColumns(Entity.generateId(), null);
                split.field("acctrans_id").value = acctransId;
            }
        }
    }

    checkNums(): boolean {
        /* Check if transaction has transnum specified. If so, all splits'
         * corresponding acctrans, as well as all splitnum's must also be
         * specified.
         */
        const transnumField = this.transaction.field("transnum");
        if (transnumField.isNotNull) {
            const transnum = transnumField.value;
            for (const split of this.splits) {
                const acctransField = split.field("acctrans");
                if (acctransField.isNull || split.field("splitnum").isNull ||
                        acctransField.value != transnum) {
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

    async createNums(context: IContext, service: IService): Promise<void> {
        const transnum = await this.acctrans.transnumField.v.generate(
            service, context);
        this.transaction.field("transnum").value = transnum;
        const splitnumField = this.acctrans.splitnumField.v;
        for (const split of this.splits) {
            split.field("acctrans").value = transnum;
            const splitnum = await splitnumField.generate(service, context);
            split.field("splitnum").value = splitnum;
        }
    }

    processPostedDT(): void {
        const posted = this.transaction.field("posted").value;
        if (!posted) {
            throw new AcctingError("Transaction is missing 'posted' date");
        }
        for (const split of this.splits) {
            split.field("posted").value = posted;
        }
    }

    async toBizTrans(bizTrans: BizTrans, logger: Logger, context: IContext,
                     service: IService): Promise<void> {
        this.balanceSplits();
        this.checkOrSetIds();
        if (!this.checkNums()) {
            await this.createNums(context, service);
        }
        this.processPostedDT();
        this.setCreated(new Date());
        await this.validate(context);
        await this.activate(context);
        bizTrans.post(
            logger, context,
            this.acctrans, this.acctrans.stateToRow(this.transaction));
        const accsplit = this.acctrans.accsplitEntity.v;
        for (const split of this.splits) {
            bizTrans.post(
                logger, context,
                accsplit, accsplit.stateToRow(split));
        }
    }
}

export type TxnRaw = {
    transaction: JsonObject;
    splits: JsonObject[];
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
}

export class AccSplit extends ImmutableEntity {
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
        /* Inbound: row => accsplit,
         * Outbound:
         *           balancelog delete (if account is 'islogged' and 'posted'
         *                              before the accounting day period's
         *                              midnight)
         *           lastbalancelog update (if account is 'islogged' and 'posted'
         *                                  before the accounting day period's
         *                                  midnight)
         *           accountbalance update (always)
         *           balancelog post (always)
         *           holding update (if 'price' was specified in the accsplit)
         */
        const updatedby = context ? context.userAccountId : (
            row.has("updatedby") ? row.get("updatedby") : Nobody.ID);
        const updated = row.has("updated") ? row.get("updated") : new Date();
        const cols: EpilogueColumn[] = [];
        const operator = AccSplit.balanceOperator(row);
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

