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
         Entity, Phase, State, FieldState, IContext, ForeignKey, _IError,
         SideEffects, AliasValueList, DateTimeField, IConfiguration, IService,
         Query, Filter, ReplicationFilter, CrossoverForeignKey,
         CrossoverForeignKeyCfg, Cfg, Field, Row, EntitySpec, BigDecimal,
         TypeCfg, BizTrans
} from "../base/core.js";
import { Txn, FinDoc } from "../accting/acc-core.js";

export const ACC_TICKET_PRICE = new BigDecimal("10.00");

class TripError extends _IError {
    constructor(message: string, options?: ErrorOptions) {
        super(500, message, options);
    }
}

export class TripReplicationFilter extends ReplicationFilter {

    hasSourceFilter(): boolean {
        return true;
    }

    getSourceFilter(arg?: string): string {
        // Default to midnight in current timezone
        let offset = new Date().getTimezoneOffset();
        if (arg) {
            /* Arg is in the form of:
             *    M+480  - Midnight in UTC-8 timezone
             *    M-180  - Midnight in UTC+3 timezone
             */
            const midnightRegex = /M([+-])(\d+)/;
            const result = arg.match(midnightRegex);
            if (result) {
                const value = Number(result[2]);
                if (result[1] == "-") {
                    offset = -1 * value;
                } else {
                    offset = value;
                }
            } else {
                throw new TripError(
                    `Cannot match replication filter ${this.name} argument ` +
                    `'${arg}' to a timezoned midnight`);
            }
        }
        // Create a date object at today's UTC midnight
        const midnight = new Date();
        midnight.setUTCHours(0, 0, 0, 0);
        /* Now add/subtract timezone offset to adjust to the current timezone
         * midnight.
         */
        const midnightLocal =
            new Date(midnight.valueOf() + (offset * 60 * 1000));
        return `v.appointmentts >= '${midnightLocal.toISOString()}'`;
    }
}

export class SubjectEntity extends Entity {

    configure(configuration: IConfiguration) {
        super.configure(configuration);

        const useraccountnum = this.getField("useraccountnum");
        if (!(useraccountnum instanceof ForeignKey) ||
           (<ForeignKey>useraccountnum).targetEntity.name != "useraccount") {
            throw new TripError(
                `${this.name} requires a ForeignKey field called ` +
                `'useraccountnum' that targets the 'useraccount' entity`);
        }
    }

    hasMembership(through: string): boolean {
        if (through == "subject") {
            return true;
        }
        return false;
    }

    async getMembers(service: IService, context: IContext, person: string,
                     through: string): Promise<string[]> {
        if (through == "subject") {
            const query = new Query(
                ["_id"],
                new Filter().op("useraccountnum_id", "=", person));
            const resultSet = await service.getQuery(
                this.logger, context, this, query);
            const result: string[] = [];
            if (resultSet.next()) {
                result.push("" + resultSet.get("_id"));
            }
            return result;
        } else {
            return super.getMembers(service, context, person, through);
        }
    }
}

type AccountEnabledSpec = EntitySpec & {
    ledger: string;
}

export class AccountEnabledSubjectEntity extends SubjectEntity {
    ledger: string;
    accountEntity: Cfg<Entity>;
    accountBalanceEntity: Cfg<Entity>;

    constructor(config: TypeCfg<AccountEnabledSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.ledger = config.spec.ledger || "";
        this.accountEntity = new Cfg("account");
        this.accountBalanceEntity = new Cfg("accountbalance");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.accountEntity.v = configuration.getEntity(this.accountEntity.name);
        this.accountBalanceEntity.v = configuration.getEntity(
            this.accountBalanceEntity.name);
    }

    protected async createAccount(bizTrans: BizTrans, service: IService,
                                  context: IContext, entityId: string,
                                  name: string, tag: string,
                                  elementType: string, description: string,
                                  holding: string) : Promise<Row> {
        const entity = this.accountEntity.v;
        const state = await entity.create(context, service);
        const se: Promise<SideEffects>[] = [];
        entity.cpSetValue(state, "entityid", entityId, context, se);
        entity.cpSetValue(state, "entitytag", tag, context, se);
        entity.cpSetValue(state, "ledger", this.ledger, context, se);
        entity.cpSetValue(state, "name", name, context, se);
        entity.cpSetValue(state, "elementtype", elementType, context, se);
        entity.cpSetValue(state, "description", description, context, se);
        entity.cpSetValue(state, "holding", holding, context, se);
        entity.cpSetValue(state, "status", "ACTIVE", context, se);
        await Promise.all(se);
        const be = await entity.postBizTrans(bizTrans, service, state, context);
        return be.row;
    }

    protected async createAccountBalance(bizTrans: BizTrans, service: IService,
                                         context: IContext, account: Row,
                                         name: string): Promise<Row> {
        const entity = this.accountBalanceEntity.v;
        const state = await entity.create(context, service);
        state.bizTrans = bizTrans;
        await entity.setValue(state, "account", name, context);
        const be = await entity.postBizTrans(bizTrans, service, state, context);
        return be.row;
    }

    protected async createAccountWithBalance(
           bizTrans: BizTrans, service: IService, context: IContext,
           entityId: string, name: string, tag: string, elementType: string,
           description: string, holding: string) : Promise<void> {
        const account = await this.createAccount(
            bizTrans, service, context,
            entityId, name, tag, elementType, description, holding);
        if (!account.has("_id")) {
            account.add("_id", Entity.generateId());
        }
        await this.createAccountBalance(
            bizTrans, service, context, account, name);
    }
}

type RiderSpec = AccountEnabledSpec & {
    currencyHolding: string;
    ticketHolding: string;
    accountsReceivablePrefix: string;
    ticketPrefix: string;
    reservedPrefix: string;
    revenueAccount: string;
}

export class Rider extends AccountEnabledSubjectEntity {
    currencyHolding: string;
    ticketHolding: string;
    accountsReceivablePrefix: string;
    ticketPrefix: string;
    reservedPrefix: string;
    revenueAccount: string;

    constructor(config: TypeCfg<RiderSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.currencyHolding = config.spec.currencyHolding || "";
        this.ticketHolding = config.spec.ticketHolding || "";
        this.accountsReceivablePrefix =
            config.spec.accountsReceivablePrefix || "";
        this.ticketPrefix = config.spec.ticketPrefix || "";
        this.reservedPrefix = config.spec.reservedPrefix || "";
        this.revenueAccount = config.spec.revenueAccount || "";
    }

    async createAccounts(bizTrans: BizTrans, service: IService,
                         context: IContext, rider: Row): Promise<void> {
        if (!rider.has("_id")) {
            rider.add("_id", Entity.generateId());
        }
        const entityId = rider.get("_id");
        const riderNum = rider.getString("ridernum");
        const suffix = riderNum.replaceAll(".", "_");
        await this.createAccountWithBalance(
            bizTrans, service, context,
            entityId, this.accountsReceivablePrefix + suffix, "AR", "ASSET",
            `${riderNum} Accounts Receivable`, this.currencyHolding);
        await this.createAccountWithBalance(
            bizTrans, service, context,
            entityId, this.ticketPrefix + suffix, "AVAILABLE", "LIABILITY",
            `${riderNum} Available Tickets`, this.ticketHolding);
        await this.createAccountWithBalance(
            bizTrans, service, context,
            entityId, this.reservedPrefix + suffix, "RESERVED", "LIABILITY",
            `${riderNum} Reserved Tickets`, this.ticketHolding);
    }
}

type DriverSpec = AccountEnabledSpec & {
    currencyHolding: string;
    ticketHolding: string;
    mileageOwedPrefix: string;
    mileageExpensedPrefix: string;
}

export class Driver extends AccountEnabledSubjectEntity {
    ticketHolding: string;
    mileageOwedPrefix: string;
    mileageExpensedPrefix: string;

    constructor(config: TypeCfg<DriverSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.ticketHolding = config.spec.ticketHolding || "";
        this.mileageOwedPrefix = config.spec.mileageOwedPrefix || "";
        this.mileageExpensedPrefix = config.spec.mileageExpensedPrefix || "";
    }

    async createAccounts(bizTrans: BizTrans, service: IService,
                         context: IContext, driver: Row): Promise<void> {
        if (!driver.has("_id")) {
            driver.add("_id", Entity.generateId());
        }
        const entityId = driver.get("_id");
        const driverNum = driver.getString("drivernum");
        const suffix = driverNum.replaceAll(".", "_");
        await this.createAccountWithBalance(
            bizTrans, service, context,
            entityId, this.mileageOwedPrefix + suffix, "OWED", "LIABILITY",
            `${driverNum} Mileage Tickets Owed`, this.ticketHolding);
        await this.createAccountWithBalance(
            bizTrans, service, context,
            entityId, this.mileageExpensedPrefix + suffix, "EXPENSED",
            "EXPENSE", `${driverNum} Expensed Tickets`, this.ticketHolding);
    }
}

export class AppointmentTSField extends DateTimeField {

    static THIRTYMINS = 1800000;

    static autoFillReturnTS(state: State): boolean {
        const appointmentts = state.field("appointmentts");
        const returnts = state.field("returnts");
        if (appointmentts.isNotNull && (returnts.isNull ||
                returnts.value <= appointmentts.value)) {
            returnts.value = new Date(appointmentts.value.valueOf() +
                    AppointmentTSField.THIRTYMINS);
            return true;
        }
        return false;
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {
        await super.activate(phase, state, fieldState, context);
        if (phase == "set") {
            const triptype = state.field("triptype");
            if (fieldState.dirtyNotNull && triptype.isNotNull &&
                   triptype.value == "RETURN") {
                if (AppointmentTSField.autoFillReturnTS(state)) {
                    return ["returnts"];
                }
            }
        }
        return null;
    }
}

export class ReturnTSField extends DateTimeField {

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {

        await super.validate(phase, state, fieldState, context);
        if (phase == "set" && fieldState.dirtyNotNull) {
            const appointmentts = state.field("appointmentts");
            if (appointmentts.isNotNull &&
                    fieldState.value <= appointmentts.value) {
                throw new TripError(
                    "Return date/time must be after the appointment date/time");
            }
        }
    }
}

export class TripDriverNumField extends CrossoverForeignKey {
    statusField: Cfg<Field>;

    constructor(entity: Entity, config: CrossoverForeignKeyCfg) {
        super(entity, config);
        this.statusField = new Cfg("status");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.statusField.setIf(
            `Invalid TripDriverNum field: ${this.fqName}: ` +
               `field ${this.statusField.name}.`,
            this.entity.findField(this.statusField.name)
        );
        if (this.sideEffects) {
            if (!this.sideEffects.includes("status")) {
                this.sideEffects.push("status");
            }
        } else {
            this.sideEffects = ["status"];
        }
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {
        const se = await super.activate(phase, state, fieldState, context);
        if (phase == "set" && fieldState.dirty) {
            state.field("status").value =
                fieldState.isNull ? "SCHED" : "ACCEPT";
        }
        return se;
    }
}

export class StatusField extends AliasValueList {
    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        if (phase == "set") {
            if (fieldState.changed &&
                this.getInternalValue(fieldState.oldValue) == "CANCEL") {
                throw new TripError("Cannot change status if current status is CANCEL");
            }
            if (state.field("drivernum").isNotNull &&
                fieldState.asString == "CANCEL") {
                throw new TripError(
                    `Cannot CANCEL a trip while a driver is still assigned`);
            }
        }
    }
}

export class TripTypeField extends AliasValueList {

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {
        await super.activate(phase, state, fieldState, context);
        if (phase == "set" && fieldState.dirtyNotNull) {
            const returnts = state.field("returnts");
            if (fieldState.value == "ONEWAY") {
                if (returnts.isNotNull) {
                    returnts.value = null;
                    return ["returnts"];
                }
            } else {
                if (AppointmentTSField.autoFillReturnTS(state)) {
                    return ["returnts"];
                }
            }
        }
        return null;
    }
}

export class Trip extends Entity {
    findocEntity: Cfg<FinDoc>;
    riderEntity: Cfg<Rider>;
    driverEntity: Cfg<Driver>;
    statusField: Cfg<AliasValueList>;

    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.findocEntity = new Cfg("acctrans");
        this.driverEntity = new Cfg("driver");
        this.riderEntity = new Cfg("rider");
        this.statusField = new Cfg("status");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.findocEntity.setIfCast(
            `'${this.name}' configuration error: `,
            configuration.entities.get(this.findocEntity.name),
            FinDoc);
        this.riderEntity.setIfCast(
            `'${this.name}' configuration error: `,
            configuration.entities.get(this.riderEntity.name),
            Rider);
        this.driverEntity.setIfCast(
            `'${this.name}' configuration error: `,
            configuration.entities.get(this.driverEntity.name),
            Driver);
        this.statusField.setIfCast(
            `'${this.name}' configuration error: `,
            this.getField(this.statusField.name),
            AliasValueList);
    }

    private async book(bizTrans: BizTrans, service: IService, context: IContext,
                       trip: Row, now: Date, quantity: BigDecimal,
                       account: string, dr: string, cr: string, reason: string,
                       id?: string): Promise<void> {
        // Do not book anything if the quantity == 0
        if (quantity.equals(BigDecimal.ZERO)) {
            return;
        }
        const amount = quantity.multiply(ACC_TICKET_PRICE);
        const memo =
            `Ticket ${reason} for ${trip.get("ridernum")} on ` +
            `trip ${trip.get("tripnum")}`;
        const findoc = await this.createFinDoc(
            context, service, account, memo, now, id || trip.get("_id"));
        const transnum = findoc.field("transnum").value;
        const txn = new Txn(this.findocEntity.v, findoc);
        const drSplit = await this.createSplit(
            context, service, "Dr", transnum, dr, quantity, amount, now,
            memo);
        txn.splits.push(drSplit);
        const crSplit = await this.createSplit(
            context, service, "Cr", transnum, cr, quantity, amount, now,
            memo);
        txn.splits.push(crSplit);
        await txn.toBizTrans(bizTrans, this.logger, context, service);
    }

    private async createFinDoc(context: IContext, service: IService,
                               account: string, memo: string, now: Date,
                               entityId: string): Promise<State> {
        const entity = this.findocEntity.v;
        const state = await entity.create(context, service);
        const se: Promise<SideEffects>[] = [];
        entity.cpSetValue(state, "account", account, context, se);
        entity.cpSetValue(state, "entityid", entityId, context, se);
        entity.cpSetValue(state, "memo", memo, context, se);
        entity.cpSetValue(state, "posted", now, context, se);
        entity.cpSetValue(state, "created", now, context, se);
        await Promise.all(se);
        return state;
    }

    private async createSplit(context: IContext, service: IService,
                              change: string, transnum: string,
                              account: string, quantity: BigDecimal,
                              amount: BigDecimal, now: Date,
                              memo: string): Promise<State> {
        const entity = this.findocEntity.v.splitEntity.v;
        const split = await entity.create(context, service);
        const se: Promise<SideEffects>[] = [];
        // Set acctrans without validation, since it doesn't exist yet.
        split.field("acctrans").value = transnum;
        entity.cpSetValue(split, "account", account, context, se);
        entity.cpSetValue(split, "change", change, context, se);
        entity.cpSetValue(split, "quantity", quantity, context, se);
        entity.cpSetValue(split, "price", ACC_TICKET_PRICE, context, se);
        entity.cpSetValue(split, "amount", amount, context, se);
        entity.cpSetValue(split, "created", now, context, se);
        entity.cpSetValue(split, "posted", now, context, se);
        entity.cpSetValue(split, "memo", memo, context, se);
        await Promise.all(se);
        return split;
    }

    async createReservation(bizTrans: BizTrans, service: IService,
                            context: IContext, trip: Row, now: Date,
                            quantity: BigDecimal, id?: string): Promise<void> {
        const riderSuffix = trip.getString("ridernum").replaceAll(".", "_");
        const dr = this.riderEntity.v.ticketPrefix + riderSuffix;
        const cr = this.riderEntity.v.reservedPrefix + riderSuffix;
        const reason = "reservation";
        return this.book(bizTrans, service, context, trip, now, quantity,
                        dr, dr, cr, reason, id);
    }

    async reverseReservation(bizTrans: BizTrans, service: IService,
                             context: IContext, trip: Row, now: Date,
                             quantity: BigDecimal, id?: string): Promise<void> {
        const riderSuffix = trip.getString("ridernum").replaceAll(".", "_");
        const dr = this.riderEntity.v.reservedPrefix + riderSuffix;
        const cr = this.riderEntity.v.ticketPrefix + riderSuffix;
        const reason = "cancellation";
        return this.book(bizTrans, service, context, trip, now, quantity,
                        cr, dr, cr, reason, id);
    }

    async completeReservation(bizTrans: BizTrans, service: IService,
                              context: IContext, trip: Row, now: Date,
                              quantity: BigDecimal,
                              id?: string): Promise<void> {
        // RIDER
        const riderSuffix = trip.getString("ridernum").replaceAll(".", "_");
        let dr = this.riderEntity.v.reservedPrefix + riderSuffix;
        let cr = this.riderEntity.v.revenueAccount;
        let reason = "completed";
        await this.book(bizTrans, service, context, trip, now, quantity,
                        cr, dr, cr, reason, id);
        // DRIVER
        if (trip.isNotNullish("drivernum")) {
            const driverSuffix =
                trip.getString("drivernum").replaceAll(".", "_");
            dr = this.driverEntity.v.mileageExpensedPrefix + driverSuffix;
            cr = this.driverEntity.v.mileageOwedPrefix + driverSuffix;
            reason = "driver completed";
            return this.book(bizTrans, service, context, trip, now, quantity,
                             cr, dr, cr, reason, id);
        }
    }

    async reverseBooking(bizTrans: BizTrans, service: IService,
                         context: IContext, trip: Row, now: Date,
                         quantity: BigDecimal, id?: string): Promise<void> {
        // RIDER
        const riderSuffix = trip.getString("ridernum").replaceAll(".", "_");
        let dr = this.riderEntity.v.revenueAccount;
        let cr = this.riderEntity.v.ticketPrefix + riderSuffix;
        let reason = "completion reversal";
        await this.book(bizTrans, service, context, trip, now, quantity,
                        dr, dr, cr, reason, id);
        // DRIVER
        if (trip.isNotNullish("drivernum")) {
            const driverSuffix =
                trip.getString("drivernum").replaceAll(".", "_");
            dr = this.driverEntity.v.mileageOwedPrefix + driverSuffix;
            cr = this.driverEntity.v.mileageExpensedPrefix + driverSuffix;
            reason = "driver completion reversal";
            return this.book(bizTrans, service, context, trip, now, quantity,
                             cr, dr, cr, reason, id);
        }
    }

    async createBooking(bizTrans: BizTrans, service: IService,
                        context: IContext, trip: Row, now: Date,
                        quantity: BigDecimal, id?: string): Promise<void> {
        // RIDER
        const riderSuffix = trip.getString("ridernum").replaceAll(".", "_");
        let dr = this.riderEntity.v.ticketPrefix + riderSuffix;
        let cr = this.riderEntity.v.revenueAccount;
        let reason = "direct charge";
        await this.book(bizTrans, service, context, trip, now, quantity,
                        cr, dr, cr, reason, id);
        // DRIVER
        if (trip.isNotNullish("drivernum")) {
            const driverSuffix =
                trip.getString("drivernum").replaceAll(".", "_");
            dr = this.driverEntity.v.mileageExpensedPrefix + driverSuffix;
            cr = this.driverEntity.v.mileageOwedPrefix + driverSuffix;
            reason = "driver direct charge";
            return this.book(bizTrans, service, context, trip, now, quantity,
                             cr, dr, cr, reason, id);
        }
    }

    async validate(phase: Phase, state: State,
                   context: IContext): Promise<void> {

        await super.validate(phase, state, context);
        if (phase == "create" || phase == "update") {
            const returnts = state.field("returnts");
            if (state.field("triptype").value == "RETURN" && returnts.isNull) {
                throw new TripError(
                    "Return date/time is required when trip is RETURN");
            }
            const appointmentts = state.field("appointmentts");
            if (returnts.isNotNull && appointmentts.isNotNull &&
                    returnts.value <= appointmentts.value) {
                throw new TripError(
                    "Return date/time must be after the appointment date/time");
            }
        }
        if (phase == "delete" && state.hasId()) {
            const status = this.statusField.v.getInternalValue(
                state.value("status"));
            if (!(["CANCEL", "COMP"].includes(status))) {
                throw new TripError(
                    "You must complete or cancel a trip before it can be " +
                    "deleted");
            }
        }
    }

    async cloneTrip(context: IContext,
                    service: IService, trip: Row): Promise<State> {
        const newTrip = await this.create(context, service);
        const se: Promise<SideEffects>[] = [];
        this.cpSetValue(newTrip, "ridernum", trip.get("ridernum"), context, se);
        this.cpSetValue(newTrip, "triptype", trip.get("triptype"), context, se);
        this.cpSetValue(newTrip, "zone", trip.get("zone"), context, se);
        await Promise.all(se);
        newTrip.setAll([
            "odescription", "oaddress1", "oaddress2", "ocity", "ostateprov",
            "opostalcode", "omaplink", "omaplinkmanual", "ophone", "comments",
            "ddescription", "daddress1", "daddress2", "dcity", "dstateprov",
            "dpostalcode", "dmaplink", "dmaplinkmanual", "dphone", "price"
        ], trip);
        return newTrip;
    }

    private flipValue(state: State, from: string, to: string): void {
        const oldFrom = state.value(from);
        state.field(from).value = state.value(to);
        state.field(to).value = oldFrom;
    }

    reverseTrip(state: State): void {
        this.flipValue(state, "oaddress1", "daddress1");
        this.flipValue(state, "oaddress2", "daddress2");
        this.flipValue(state, "ocity", "dcity");
        this.flipValue(state, "ostateprov", "dstateprov");
        this.flipValue(state, "opostalcode", "dpostalcode");
        this.flipValue(state, "omaplink", "dmaplink");
        this.flipValue(state, "omaplinkmanual", "dmaplinkmanual");
        this.flipValue(state, "ophone", "dphone");
    }

    async splitReturnTrip(context: IContext,
                          service: IService, trip: Row): Promise<State> {
        if (trip.get("triptype") != "RETURN") {
            throw new TripError("Trip must be a RETURN type to split");
        }
        if (trip.isNotNullish("drivernum")) {
            throw new TripError("Cannot split a Trip with a driver assigned");
        }
        const awayTrip = this.rowToState(trip);
        const returnTrip = await this.cloneTrip(context, service, trip);
        this.reverseTrip(returnTrip);
        // Set the asynchronous values on both away and return trips
        const se: Promise<SideEffects>[] = [];
        this.cpSetValue(awayTrip, "triptype", "ONEWAY", context, se);
        this.cpSetValue(returnTrip, "triptype", "ONEWAY", context, se);
        this.cpSetValue(
            returnTrip, "appointmentts", trip.get("returnts"), context, se);
        await Promise.all(se);
        const bt = new BizTrans();
        await this.putBizTrans(bt, service, awayTrip, context);
        const returnEntry = await this.postBizTrans(
            bt, service, returnTrip, context);
        if (!returnEntry.row.has("_id")) {
            returnEntry.row.add("_id", Entity.generateId());
        }
        await this.createReservation(
            bt, service, context, returnEntry.row, new Date(),
            BigDecimal.ensure(returnEntry.row.get("price")));
        // Commit all as one transaction.
        const resultBt = await service.processBizTrans(
            this.logger, bt);
        // Fish out the 'POST' return trip
        const returnTripEntry = resultBt.entries.find(
            (entry) => entry.action == "post" &&
                entry.entity.name == this.name);
        if (returnTripEntry) {
            return this.rowToState(returnTripEntry.row);
        } else {
            throw new TripError("BizTrans did not produce a return trip");
        }
    }

    async completeTrip(context: IContext, service: IService,
                       trip: State, reason?: string): Promise<Row> {
        if (!trip.hasId()) {
            throw new TripError(
                "Trip must be saved before it can be completed");
        }
        const status = this.statusField.v.getInternalValue(
            trip.value("status"));
        if (status == "CANCEL") {
            throw new TripError("Cannot complete a cancelled trip");
        }
        if (status == "COMP") {
            throw new TripError("Trip is already completed");
        }
        if (["SCHED", "ACCEPT"].includes(status)) {
            const price = trip.value("price");
            await this.setValue(trip, "status", "COMP", context);
            if (reason) {
                await this.setValue(trip, "statusmemo", reason, context);
            }
            const bt = new BizTrans();
            const tripEntry = await this.putBizTrans(
                bt, service, trip, context);
            const id = tripEntry.id;
            if (status == "ACCEPT") {
                await this.completeReservation(
                    bt, service, context, tripEntry.row, new Date(), price, id);
            } else {
                await this.createBooking(
                    bt, service, context, tripEntry.row, new Date(), price, id);
            }
            const resultBt = await service.processBizTrans(this.logger, bt);
            /* Fish out the Trip from the results. In virtually all cases,
             * this is the same object as 'tripEntry' above, but we'll go
             * through these extra steps in case this changes in the future.
             */
            return resultBt.fish(this.name, id);
        } else {
            throw new TripError(`Unrecognized status: ${status}`);
        }
    }

    async cancelTrip(context: IContext, service: IService,
                     trip: State, reason: string): Promise<Row> {
        if (!trip.hasId()) {
            throw new TripError(
                "Trip must be saved before it can be cancelled");
        }
        const status = this.statusField.v.getInternalValue(
            trip.value("status"));
        if (status == "CANCEL") {
            throw new TripError("Trip is already in cancelled status");
        }
        if (["SCHED", "ACCEPT", "COMP"].includes(status)) {
            const price = trip.value("price");
            await this.setValue(trip, "status", "CANCEL", context);
            await this.setValue(trip, "statusmemo", reason, context);
            const bt = new BizTrans();
            const tripEntry = await this.putBizTrans(
                bt, service, trip, context);
            if (status == "SCHED" || status == "ACCEPT") {
                await this.reverseReservation(
                    bt, service, context, tripEntry.row, new Date(), price,
                    tripEntry.id);
            } else {
                await this.reverseBooking(
                    bt, service, context, tripEntry.row, new Date(), price,
                    tripEntry.id);
            }
            const resultBt = await service.processBizTrans(this.logger, bt);
            /* Fish out the Trip from the results. In virtually all cases,
             * this is the same object as 'tripEntry' above, but we'll go
             * through these extra steps in case this changes in the future.
             */
            return resultBt.fish(this.name, tripEntry.id);
        } else {
            throw new TripError(`Unrecognized status: ${status}`);
        }
    }

    async applyPriceChange(context: IContext, service: IService, trip: State,
                           oldPrice: BigDecimal,
                           newPrice: BigDecimal): Promise<Row> {
        if (oldPrice.equals(newPrice)) {
            throw new TripError(
                "Cannot call applyPriceChange() if the price hasn't changed");
        }
        const status = this.statusField.v.getInternalValue(
            trip.field("status").value);
        if (status == "CANCEL") {
            throw new TripError(
                `Trip cannot change price once status is ${status}`);
        }
        await this.setValue(trip, "price", newPrice, context);
        const bt = new BizTrans();
        const tripEntry = await this.putBizTrans(bt, service, trip, context);
        if (status == "SCHED" || status == "ACCEPT") {
            await this.reverseReservation(
                bt, service, context, tripEntry.row, new Date(), oldPrice,
                tripEntry.id);
            await this.createReservation(
                bt, service, context, tripEntry.row, new Date(), newPrice,
                tripEntry.id);
        } else if (status == "COMP") {
            await this.reverseBooking(
                bt, service, context, tripEntry.row, new Date(), oldPrice,
                tripEntry.id);
            await this.createBooking(
                bt, service, context, tripEntry.row, new Date(), newPrice,
                tripEntry.id);
        } else {
            throw new TripError(`Unrecognized Trip status: ${status}`);
        }
        const resultBt = await service.processBizTrans(this.logger, bt);
        /* Fish out the Trip from the results. In virtually all cases,
         * this is the same object as 'tripEntry' above, but we'll go through
         * these extra steps in case this changes in the future.
         */
        return resultBt.fish(this.name, tripEntry.id);
    }
}

