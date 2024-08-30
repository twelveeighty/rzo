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
    Query, Filter
} from "../base/core.js";

class TripError extends _IError {
    constructor(message: string, options?: ErrorOptions) {
        super(500, message, options);
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
}

