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
    CrossoverForeignKeyCfg, Cfg, Field, Row, EntitySpec, BigDecimal, TypeCfg,
    BizTrans, BizTransEntry
} from "../base/core.js";

import { Txn, AccTrans } from "../accting/acc-core.js";

const ACC_TICKET_PRICE = new BigDecimal("10.00");

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

        const sideEffects = await super.activate(
            phase, state, fieldState, context);

        if (phase == "set" && (fieldState.dirtyNotNull ||
                               fieldState.dirtyNull)) {
            state.field("status").value = fieldState.isNull ? "SCHED" :
                "ACCEPT";
        }
        return sideEffects;
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

type TripEntitySpec = EntitySpec & {
    ticketPrefix: string;
    reservedPrefix: string;
}

export class Trip extends Entity {
    ticketPrefix: string;
    reservedPrefix: string;
    acctransEntity: Cfg<AccTrans>;

    constructor(config: TypeCfg<TripEntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.ticketPrefix = config.spec.ticketPrefix || "";
        this.reservedPrefix = config.spec.reservedPrefix || "";
        this.acctransEntity = new Cfg("acctrans");
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.acctransEntity.setIfCast(
            `'${this.name}' configuration error: 'acctrans'`,
            configuration.entities.get(this.acctransEntity.name),
            AccTrans);
    }

    private async createTransaction(context: IContext, service: IService,
                                    account: string, memo: string, now: Date,
                                    entityId: string): Promise<State> {
        const state = await this.acctransEntity.v.create(context, service);
        const validations: Promise<SideEffects>[] = [];
        validations.push(this.acctransEntity.v.setValue(
            state, "account", account, context));
        validations.push(this.acctransEntity.v.setValue(
            state, "entityid", entityId, context));
        validations.push(this.acctransEntity.v.setValue(
            state, "memo", memo, context));
        validations.push(this.acctransEntity.v.setValue(
            state, "posted", now, context));
        validations.push(this.acctransEntity.v.setValue(
            state, "created", now, context));
        await Promise.all(validations);
        return state;
    }

    private async createSplit(context: IContext, service: IService,
                              change: string, transnum: string,
                              account: string, quantity: BigDecimal,
                              amount: BigDecimal, now: Date,
                              memo: string): Promise<State> {
        const accsplitEntity = this.acctransEntity.v.accsplitEntity.v;
        const split = await accsplitEntity.create(context, service);
        const validations: Promise<SideEffects>[] = [];
        // Set acctrans without validation, since it doesn't exist yet.
        split.field("acctrans").value = transnum;
        validations.push(accsplitEntity.setValue(
            split, "account", account, context));
        validations.push(accsplitEntity.setValue(
            split, "change", change, context));
        validations.push(accsplitEntity.setValue(
            split, "quantity", quantity, context));
        validations.push(accsplitEntity.setValue(
            split, "price", ACC_TICKET_PRICE, context));
        validations.push(accsplitEntity.setValue(
            split, "amount", amount, context));
        validations.push(accsplitEntity.setValue(
            split, "created", now, context));
        validations.push(accsplitEntity.setValue(
            split, "posted", now, context));
        validations.push(accsplitEntity.setValue(
            split, "memo", memo, context));
        await Promise.all(validations);
        return split;
    }

    async postBizTrans(bizTrans: BizTrans, service: IService, state: State,
                       context: IContext): Promise<BizTransEntry> {
        /* We want to tie this newly created Trip to the Transaction, so we
         * must create and set the Trip _id here.
         */
        const tripId = Entity.generateId();
        const tripEntry = await super.postBizTrans(
            bizTrans, service, state, context);
        tripEntry.row.add("_id", tripId);
        const now = new Date();
        await this.createReservation(
            bizTrans, service, context, tripEntry, now,
            BigDecimal.ensure(tripEntry.row.get("price")), tripId);
        return tripEntry;
    }

    private async createReservation(bizTrans: BizTrans, service: IService,
                                    context: IContext, tripEntry: BizTransEntry,
                                    now: Date, quantity: BigDecimal,
                                    tripId: string): Promise<void> {
        const tripNum = tripEntry.row.getString("tripnum");
        const amount = quantity.multiply(ACC_TICKET_PRICE);
        const name = tripEntry.row.getString("ridernum").replaceAll(".", "_");
        const drName = this.ticketPrefix + name;
        const crName = this.reservedPrefix + name;
        const memo =
            `Ticket reservation for ${name} on trip ${tripNum}`;
        const transaction = await this.createTransaction(
            context, service, drName, memo, now, tripId);
        const transnum = transaction.field("transnum").value;
        const txn = new Txn(this.acctransEntity.v, transaction);
        const drSplit = await this.createSplit(
            context, service, "Dr", transnum, drName, quantity, amount, now,
            memo);
        txn.splits.push(drSplit);
        const crSplit = await this.createSplit(
            context, service, "Cr", transnum, crName, quantity, amount, now,
            memo);
        txn.splits.push(crSplit);
        await txn.toBizTrans(bizTrans, this.logger, context, service);
    }

    private async reverseReservation(bizTrans: BizTrans, service: IService,
                                     context: IContext, tripEntry: BizTransEntry,
                                     now: Date, oldQuantity: BigDecimal,
                                     tripId: string): Promise<void> {
        const tripNum = tripEntry.row.getString("tripnum");
        const amount = oldQuantity.multiply(ACC_TICKET_PRICE);
        const name = tripEntry.row.getString("ridernum").replaceAll(".", "_");
        const drName = this.reservedPrefix + name;
        const crName = this.ticketPrefix + name;
        const memo =
            `Ticket cancellation for ${name} on trip ${tripNum}`;
        const transaction = await this.createTransaction(
            context, service, crName, memo, now, tripId);
        const transnum = transaction.field("transnum").value;
        const txn = new Txn(this.acctransEntity.v, transaction);
        const drSplit = await this.createSplit(
            context, service, "Dr", transnum, drName, oldQuantity, amount, now,
            memo);
        txn.splits.push(drSplit);
        const crSplit = await this.createSplit(
            context, service, "Cr", transnum, crName, oldQuantity, amount, now,
            memo);
        txn.splits.push(crSplit);
        await txn.toBizTrans(bizTrans, this.logger, context, service);
    }

    async putBizTrans(bizTrans: BizTrans, service: IService, state: State,
                      context: IContext): Promise<BizTransEntry> {
        const tripEntry = await super.putBizTrans(
            bizTrans, service, state, context);
        /* If the 'price' has changed, reverse the original reservation and
         * add a new one with the current price.
         */
        const quantityField = state.field("price");
        if (quantityField.changed) {
            const oldQuantity = BigDecimal.ensure(quantityField.oldValue);
            const newQuantity = BigDecimal.ensure(quantityField.value);
            const now = new Date();
            await this.reverseReservation(
                bizTrans, service, context, tripEntry, now, oldQuantity,
                tripEntry.id);
            await this.createReservation(
                bizTrans, service, context, tripEntry, now, newQuantity,
                tripEntry.id);
        }
        return tripEntry;
    }

    async deleteBizTrans(bizTrans: BizTrans, service: IService, state: State,
                         context: IContext): Promise<BizTransEntry | null> {
        const tripEntry = await super.deleteBizTrans(
            bizTrans, service, state, context);
        if (tripEntry) {
            const quantityField = state.findField("quantity");
            if (quantityField && quantityField.isNotNull) {
                const now = new Date();
                const quantity = BigDecimal.ensure(quantityField.value);
                await this.reverseReservation(
                    bizTrans, service, context, tripEntry, now, quantity,
                    tripEntry.id);
            }
        }
        return tripEntry;
    }

    async post(service: IService, state: State,
               context: IContext): Promise<Row> {
        this.logger.error(
            "WARNING - calling post() instead of postBizTrans() can create " +
            "accounting inbalances and inconsistencies");
        return super.post(service, state, context);
    }

    async put(service: IService, state: State,
              context: IContext): Promise<Row> {
        this.logger.error(
            "WARNING - calling put() instead of putBizTrans() can create " +
            "accounting inbalances and inconsistencies");
        return super.put(service, state, context);
    }

    async delete(service: IService, state: State,
              context: IContext): Promise<void> {
        this.logger.error(
            "WARNING - calling delete() instead of deleteBizTrans() can " +
            "create accounting inbalances and inconsistencies");
        super.delete(service, state, context);
    }

    async validate(phase: Phase, state: State,
                   context: IContext): Promise<void> {

        await super.validate(phase, state, context);
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

    async cloneTrip(context: IContext,
                    service: IService, trip: Row): Promise<State> {
        const newTrip = await this.create(context, service);
        const sideEffects: Promise<SideEffects>[] = [];
        sideEffects.push(this.setValue(
            newTrip, "ridernum", trip.get("ridernum"), context));
        sideEffects.push(this.setValue(
            newTrip, "triptype", trip.get("triptype"), context));
        sideEffects.push(this.setValue(
            newTrip, "zone", trip.get("zone"), context));
        await Promise.all(sideEffects);
        newTrip.field("odescription").value = trip.get("odescription");
        newTrip.field("oaddress1").value = trip.get("oaddress1");
        newTrip.field("oaddress2").value = trip.get("oaddress2");
        newTrip.field("ocity").value = trip.get("ocity");
        newTrip.field("ostateprov").value = trip.get("ostateprov");
        newTrip.field("opostalcode").value = trip.get("opostalcode");
        newTrip.field("omaplink").value = trip.get("omaplink");
        newTrip.field("omaplinkmanual").value = trip.get("omaplinkmanual");
        newTrip.field("ophone").value = trip.get("ophone");
        newTrip.field("comments").value = trip.get("comments");
        newTrip.field("ddescription").value = trip.get("ddescription");
        newTrip.field("daddress1").value = trip.get("daddress1");
        newTrip.field("daddress2").value = trip.get("daddress2");
        newTrip.field("dcity").value = trip.get("dcity");
        newTrip.field("dstateprov").value = trip.get("dstateprov");
        newTrip.field("dpostalcode").value = trip.get("dpostalcode");
        newTrip.field("dmaplink").value = trip.get("dmaplink");
        newTrip.field("dmaplinkmanual").value = trip.get("dmaplinkmanual");
        newTrip.field("dphone").value = trip.get("dphone");
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
        if (trip.get("drivername")) {
            throw new TripError("Cannot split a Trip with a driver assigned");
        }
        const awayTrip = this.rowToState(trip);
        const returnTrip = await this.cloneTrip(context, service, trip);
        this.reverseTrip(returnTrip);
        // Set the asynchronous values on both away and return trips
        const sideEffects: Promise<SideEffects>[] = [];
        sideEffects.push(this.setValue(
            awayTrip, "triptype", "ONEWAY", context));
        sideEffects.push(this.setValue(
            returnTrip, "triptype", "ONEWAY", context));
        sideEffects.push(this.setValue(
            returnTrip, "appointmentts", trip.get("returnts"), context));
        await Promise.all(sideEffects);
        const bizTrans = new BizTrans();
        await this.putBizTrans(bizTrans, service, awayTrip, context);
        await this.postBizTrans(bizTrans, service, returnTrip, context);
        const resultTrans = await service.processBizTrans(
            this.logger, bizTrans);
        // Fish out the 'POST' return trip
        const returnTripEntry = resultTrans.entries.find(
            (entry) => entry.action == "post" &&
                entry.entity.name == this.name);
        if (returnTripEntry) {
            return this.rowToState(returnTripEntry.row);
        } else {
            throw new TripError("BizTrans did not produce a return trip");
        }
    }
}

