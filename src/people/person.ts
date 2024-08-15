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
    Entity, TypeCfg, EntitySpec, Query, Filter, StringField, Cfg, FieldCfg,
    IConfiguration, IService, Phase, State, FieldState, IContext, _IError,
    SideEffects
} from "../base/core.js";

class PersonError extends _IError {
    constructor(message: string, options?: ErrorOptions) {
        super(500, message, options);
    }
}

export class Crew extends Entity {

    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
    }

    hasMembership(through: string): boolean {
        if (through == "member") {
            return true;
        }
        return false;
    }

    getCrewMemberEntity(): Entity {
        const crewMember = this.contains.find((child) => {
            return child.name == "crewmember";
        });
        if (!crewMember) {
            throw new PersonError(`Entity '${this.name}' is missing a ` +
                                  `mandatory containment of 'crewmember'`);
        }
        return crewMember;
    }

    async getMembers(service: IService, context: IContext, person: string,
                     through: string): Promise<string[]> {
        if (through == "member") {
            const crewMemberEntity = this.getCrewMemberEntity();
            const query = new Query(["crewnum_id"], new Filter().
                                    op("personnum_id", "=", person));
            const resultSet = await service.getQuery(
                this.logger, context, crewMemberEntity, query);
            const result: string[] = [];
            while (resultSet.next()) {
                result.push("" + resultSet.get("crewnum_id"));
            }
            return result;
        } else {
            return super.getMembers(service, context, person, through);
        }
    }
}

type MapLinkFieldCfg = FieldCfg & {
    target: string;
}

export class MapLinkTargetField extends StringField {
    mapLinkSources: MapLinkSrcField[];
    mapLinkManual?: MapLinkManualField;

    constructor(entity: Entity, config: FieldCfg) {
        super(entity, config);
        this.mapLinkSources = [];
    }

    async calculate(phase: Phase, state: State,
                    context: IContext): Promise<void> {
        if (this.mapLinkManual && state.asString(this.mapLinkManual.name)) {
            return;
        }
        let result = "";
        for (const source of this.mapLinkSources) {
            const sourceVal = state.asString(source.name).trim();
            if (sourceVal) {
                if (result) {
                    result += " ";
                }
                result += sourceVal;
            }
        }
        await this.setValue(state, result, context);
    }
}

export class MapLinkSrcField extends StringField {
    mapLinkTarget: Cfg<MapLinkTargetField>;

    constructor(entity: Entity, config: MapLinkFieldCfg) {
        super(entity, config);
        this.mapLinkTarget = new Cfg(config.target);
    }

    configure(configuration: IConfiguration) {
        this.mapLinkTarget.setIfCast(
            `Field '${this.fqName}': `,
            this.entity.coreFields.get(this.mapLinkTarget.name),
            MapLinkTargetField);
        this.mapLinkTarget.v.mapLinkSources.push(this);
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {
        if (phase == "set" && fieldState.dirty) {
            await this.mapLinkTarget.v.calculate(phase, state, context);
            return [this.mapLinkTarget.v.name];
        }
        return null;
    }
}

export class MapLinkManualField extends StringField {
    mapLinkTarget: Cfg<MapLinkTargetField>;

    constructor(entity: Entity, config: MapLinkFieldCfg) {
        super(entity, config);
        this.mapLinkTarget = new Cfg(config.target);
    }

    configure(configuration: IConfiguration) {
        this.mapLinkTarget.setIfCast(
            `Field '${this.fqName}': `,
            this.entity.coreFields.get(this.mapLinkTarget.name),
            MapLinkTargetField);
        this.mapLinkTarget.v.mapLinkManual = this;
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        if (phase == "set" && fieldState.dirtyNotNull) {
             const strValue = fieldState.asString;
             if (strValue && !strValue.trim()) {
                 throw new PersonError(
                     `${this.fqName} must either be empty or have at ` +
                     `least one non-space character`);
             }
        }
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {
        if (phase == "set" && fieldState.dirty) {
            if (fieldState.isNotNull && fieldState.asString) {
                await this.mapLinkTarget.v.setValue(
                    state, fieldState.value, context);
            } else {
                await this.mapLinkTarget.v.calculate(phase, state, context);
            }
            return [this.mapLinkTarget.v.name];
        }
        return null;
    }
}

type PhoneLabelFieldCfg = FieldCfg & {
    default: string;
}

export class PhoneLabelField extends StringField {

    constructor(entity: Entity, config: PhoneLabelFieldCfg) {
        super(entity, config);
        this.default = this.default.trim();
    }
}

