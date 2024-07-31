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
    Entity, Field, ForeignKey, GeneratorField, AmountField, AncestryField,
    StringField, IConfiguration
} from "./core.js";

import { ClassInfo, Reflection } from "./reflect.js";

export interface EntityCreator {
    creationDDL(factory: CreatorFactory, entity: Entity, doVersion: boolean,
                dropFirst?: boolean): string;
    updateDDL(factory: CreatorFactory, from: Entity, into: Entity,
              doVersion: boolean, dropFirst?: boolean): string;
    dropDDL(factory: CreatorFactory, entity: Entity,
            doVersion: boolean): string;
}

export type DataDef = {
    ddl: string[];
    post: string[];
}

export interface FieldCreator {
    creationDDL(factory: CreatorFactory, field: Field, doVersion: boolean,
                dropFirst?: boolean): DataDef;
    updateDDL(factory: CreatorFactory, from: Field, into: Field,
              doVersion: boolean, dropFirst?: boolean): string[];
    dropDDL(factory: CreatorFactory, from: Field, doVersion: boolean): string[];
}

type EntityCreatorClass = { new(): EntityCreator; };
type FieldCreatorClass = { new(): FieldCreator; };

class CoreDDLError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }
}

export class CreatorFactory {
    private reflection: Reflection;
    private entities: Map<string, EntityCreator>;
    private fields: Map<string, FieldCreator>;
    private classes: Map<string, any>;

    constructor() {
        this.entities = new Map();
        this.fields = new Map();
        this.reflection = new Reflection();
        this.classes = new Map();
    }

    private async reflectClasses(names: Set<string>): Promise<void> {
        const classPromises: Promise<ClassInfo>[] = [];
        for (const className of names) {
            classPromises.push(this.reflection.reflect(className));
        }
        try {
            const allClasses = await Promise.all(classPromises);
            for (const resolvedClass of allClasses) {
                const fqn = resolvedClass.name;
                const clazz = resolvedClass.clazz;
                this.classes.set(fqn, clazz);
            }
        } catch (error) {
            throw new CoreDDLError("Reflection error", { cause: error });
        }
    }

    async load(config: IConfiguration): Promise<void> {
        const entityClasses = new Set<string>();
        const fieldClasses = new Set<string>();
        for (const entity of config.entities.values()) {
            let className = entity.ddlCreatorClass;
            if (!this.classes.has(className)) {
                entityClasses.add(className);
            }
            for (const field of entity.allFields) {
                className = field.ddlCreatorClass;
                if (!this.classes.has(className)) {
                    fieldClasses.add(className);
                }
            }
        }
        console.log(`Reflecting ${entityClasses.size} entity DDL classes...`);
        await this.reflectClasses(entityClasses);

        console.log(`Reflecting ${fieldClasses.size} field DDL classes...`);
        await this.reflectClasses(fieldClasses);

        for (const entityClass of entityClasses) {
            const clazz = this.classes.get(entityClass);
            console.log(`instantiating ${entityClass}`);
            const instanceClazz = <EntityCreatorClass>clazz;
            const instance = new instanceClazz();
            this.entities.set(entityClass, instance);
        }

        for (const fieldClass of fieldClasses) {
            const clazz = this.classes.get(fieldClass);
            console.log(`instantiating ${fieldClass}`);
            const instanceClazz = <FieldCreatorClass>clazz;
            const instance = new instanceClazz();
            this.fields.set(fieldClass, instance);
        }
    }

    entityCreator(fqName: string): EntityCreator {
        const result = this.entities.get(fqName);
        if (result) {
            return result;
        } else {
            throw new CoreDDLError(`Invalid creator class: ${fqName}`);
        }
    }

    fieldCreator(fqName: string): FieldCreator {
        const result = this.fields.get(fqName);
        if (result) {
            return result;
        } else {
            throw new CoreDDLError(`Invalid creator class: ${fqName}`);
        }
    }

}

export class FieldDDL implements FieldCreator {

    columnDDLType(field: Field, doVersion: boolean): string {
        throw new CoreDDLError(`No DDL for ${field.fqName}`);
    }

    columnDDL(field: Field, name: string, doVersion: boolean,
              required: boolean): string {
        const notNull = !doVersion && required;
        // Since this can get called by a ForeignKey that has 'field'
        // as its into, we cannot use field.name or field.required.
        return `   ` +
            `${name}  ${this.columnDDLType(field, doVersion)}` +
            `${notNull ? " not null" : ""}`;
    }

    creationDDL(factory: CreatorFactory, field: Field, doVersion: boolean,
                dropFirst?: boolean): DataDef {
        const post: string[] = [];
        if (!doVersion && field.indexed != "none") {
            post.push(this.indexDDL(field));
        }
        const result: DataDef = {
            ddl: [this.columnDDL(field, field.name, doVersion, field.required)],
            post: post
        };
        return result;
    }

    protected isUnchanged(from: Field, into: Field,
                          doVersion: boolean): boolean {
        return from.type == into.type &&
            (doVersion || from.required == into.required) &&
            from.maxlength == into.maxlength;
    }

    protected indexDDL(field: Field): string {
        const direction = field.indexed == "desc" ? " desc" : "";
        return `create index ${field.entity.table}_${field.name} ` +
               ` on ${field.entity.table} (${field.name}${direction})`;
    }

    protected convertField(factory: CreatorFactory, from: Field,
                           into: Field, doVersion: boolean,
                           dropFirst?: boolean): string[] {

        const table = !doVersion ? into.entity.table : `${into.entity.table}_v`;
        const result = [];
        if (!doVersion && from.required != into.required) {
            result.push(
                `alter table ${table} alter ${into.name} ` +
                `${into.required ? "set" : "drop"} not null`);
        }
        if (!doVersion && from.indexed != into.indexed) {
            if (from.indexed != "none") {
                result.push(`drop index if exists ${table}_${from.name}`);
            }
            if (into.indexed != "none") {
                result.push(this.indexDDL(into));
            }
        }
        if (from.type != into.type ||
            from.maxlength != into.maxlength) {

            const newTypeDDL = this.columnDDL(
                into, into.name, doVersion, false);
            result.push(
                `alter table ${table} alter ${newTypeDDL}`);
        }
        if (!result.length) {
            throw new CoreDDLError(
                `Cannot convert field ${from.fqName} from type ` +
                `${from.type} to type ${into.type}`);
        }
        return result;
    }

    updateDDL(factory: CreatorFactory, from: Field, into: Field,
              doVersion: boolean, dropFirst?: boolean): string[] {
        if (this.isUnchanged(from, into, doVersion)) {
            return [];
        }
        return this.convertField(factory, from, into, doVersion,
                                 dropFirst);
    }

    dropDDL(factory: CreatorFactory, from: Field,
            doVersion: boolean): string[] {
        const table = !doVersion ? from.entity.table : `${from.entity.table}_v`;
        return [
            `alter table ${table} drop column if exists ${from.name}`
        ];
    }
}

export class StringFieldDDL extends FieldDDL {
    columnDDLType(field: Field, doVersion: boolean): string {
        if (field.maxlength) {
            return `varchar(${field.maxlength})`;
        } else {
            return "text";
        }
    }
}

export class IntegerFieldDDL extends FieldDDL {
    columnDDLType(field: Field, doVersion: boolean): string {
        return "integer";
    }
}

export class NumberFieldDDL extends FieldDDL {
    columnDDLType(field: Field, doVersion: boolean): string {
        return "double precision";
    }
}

export class BooleanFieldDDL extends FieldDDL {
    columnDDLType(field: Field, doVersion: boolean): string {
        return "boolean";
    }
}

export class DateFieldDDL extends FieldDDL {
    columnDDLType(field: Field, doVersion: boolean): string {
        return "date";
    }
}

export class DateTimeFieldDDL extends FieldDDL {
    columnDDLType(field: Field, doVersion: boolean): string {
        return "timestamptz";
    }
}

export class AmountFieldDDL extends FieldDDL {
    columnDDLType(field: Field, doVersion: boolean): string {
        if (!(field instanceof AmountField)) {
            throw new CoreDDLError(
                `field ${field.fqName} is not an AmountField`);
        }
        const aField = <AmountField>field;
        return `numeric(${aField.precision}, ${aField.scale})`;
    }
}

export class ForeignKeyDDL implements FieldCreator {

    creationDDL(factory: CreatorFactory, field: Field, doVersion: boolean,
                dropFirst?: boolean): DataDef {
        const req = !doVersion && field.required ? " not null" : "";

        if (!(field instanceof ForeignKey)) {
            throw new CoreDDLError(`field ${field.fqName} is not a ForeignKey`);
        }
        const targetField = (<ForeignKey>field).targetField.v;
        const intoDDLCreator = factory.fieldCreator(
            targetField.ddlCreatorClass);

        if (!(intoDDLCreator instanceof FieldDDL)) {
            throw new CoreDDLError(
                `Foreign key ${field.fqName}'s into ` +
                `${targetField.fqName} does not have ` +
                `its ddlCreatorClass property resolve to a FieldDDL instance`);
        }
        const intoDDL = (<FieldDDL>intoDDLCreator).columnDDL(
            targetField, field.name, doVersion, field.required);
        const idDDL = `   ${field.idName} uuid${req}`;
        const postDDL = [];
        if (!doVersion) {
            const indexDDL =
                `create index ${field.entity.name}_${field.name} ` +
                `on ${field.entity.table} (${field.idName})`;
            postDDL.push(indexDDL);
        }
        const result: DataDef = {
            ddl: [intoDDL, idDDL],
            post: postDDL
        };
        return result;
    }

    updateDDL(factory: CreatorFactory, from: Field, into: Field,
              doVersion: boolean, dropFirst?: boolean): string[] {
        if (from.type != into.type) {
            throw new CoreDDLError(
                `Cannot change ${from.fqName} from or into a ForeignKey`);
        }
        if (!(from instanceof ForeignKey)) {
            throw new CoreDDLError(
                `Old field ${from.fqName} is not a ForeignKey`);
        }
        if (!(into instanceof ForeignKey)) {
            throw new CoreDDLError(
                `New field ${into.fqName} is not a ForeignKey`);
        }
        const fromTargetType = (<ForeignKey>from).targetField.v.type;
        const intoTargetType = (<ForeignKey>into).targetField.v.type;
        if (fromTargetType != intoTargetType) {
            throw new CoreDDLError(
                `Foreign key ${into.fqName} cannot be changed from target ` +
                `type ${fromTargetType} to target type ${intoTargetType}`);
        }
        return [];
    }

    dropDDL(factory: CreatorFactory, from: Field,
            doVersion: boolean): string[] {
        if (!(from instanceof ForeignKey)) {
            throw new CoreDDLError(
                `Old field ${from.fqName} is not a ForeignKey`);
        }
        const result: string[] = [];
        const table = !doVersion ? from.entity.table : `${from.entity.table}_v`;
        if (!doVersion) {
            result.push(
                `drop index if exists ${from.entity.name}_${from.name}`);
        }
        result.push(
            `alter table ${table} drop column if exists ${from.name}`);
        result.push(
            `alter table ${table} drop column if exists ` +
            `${(<ForeignKey>from).idName}`);
        return result;
    }
}

export class GeneratorFieldDDL extends StringFieldDDL {

    protected createGenerator(field: GeneratorField, result: string[]) {
        result.push(
            `drop sequence if exists ${field.generatorName}`);
        const maxClause =
            field.generatorSpec.max ?
           ` maxvalue ${field.generatorSpec.max} cycle` : "";
        result.push(
            `create sequence ${field.generatorName} ` +
            `minvalue ${field.generatorSpec.min}${maxClause}`);
    }

    creationDDL(factory: CreatorFactory, field: Field, doVersion: boolean,
                dropFirst?: boolean): DataDef {
        if (!(field instanceof GeneratorField)) {
            throw new CoreDDLError(
                `field ${field.fqName} is not a GeneratorField`);
        }
        const result = super.creationDDL(factory, field, doVersion);
        if (!doVersion) {
            this.createGenerator(<GeneratorField>field, result.post);
        }
        return result;
    }

    updateDDL(factory: CreatorFactory, from: Field, into: Field,
              doVersion: boolean, dropFirst?: boolean): string[] {
        if (!(from instanceof GeneratorField) ||
            !(into instanceof GeneratorField)) {
            throw new CoreDDLError(
                `Cannot change ${from.fqName} from or into a GeneratorField`);
        }
        const result = super.updateDDL(factory, from, into, doVersion,
                                       dropFirst);
        if (!doVersion) {
            if ((<GeneratorField>from).generatorSpec.min !=
                (<GeneratorField>into).generatorSpec.min ||
                (<GeneratorField>from).generatorSpec.max !=
                (<GeneratorField>into).generatorSpec.max) {

                this.createGenerator(<GeneratorField>into, result);
            }
        }
        return result;
    }

    dropDDL(factory: CreatorFactory, from: Field,
            doVersion: boolean): string[] {
        if (!(from instanceof GeneratorField)) {
            throw new CoreDDLError(
                `field ${from.fqName} is not a GeneratorField`);
        }
        const result = super.dropDDL(factory, from, doVersion);
        if (!doVersion) {
            result.push(
                `drop sequence if exists ` +
                `${(<GeneratorField>from).generatorName}`);
        }
        return result;
    }
}

export class AncestryFieldDDL implements FieldCreator {

    creationDDL(factory: CreatorFactory, field: Field, doVersion: boolean,
                dropFirst?: boolean): DataDef {
        const req = !doVersion && field.required ? " not null" : "";
        const colDDL = !doVersion ?
            `   ${field.name} ltree${req}` :
            `   ${field.name} text`;
        const postDDL: string[] = [];
        if (!doVersion) {
            const indexDDL =
                `create index ${field.entity.name}_${field.name} ` +
                `on ${field.entity.table} using GIST (${field.name})`;
            postDDL.push(indexDDL);
        }
        const result: DataDef = {
            ddl: [colDDL],
            post: postDDL
        };
        return result;
    }

    updateDDL(factory: CreatorFactory, from: Field, into: Field,
              doVersion: boolean, dropFirst?: boolean): string[] {
        // We can only convert a 'text' field into a ltree and vice-versa,
        // otherwise bail.
        if (!(from instanceof StringField) &&
            !(from instanceof AncestryField)) {
            throw new CoreDDLError(
                `Cannot change ${from.fqName} from type ${from.type} to ` +
                `${into.type}`);
        }
        const result: string[] = [];
        if (!doVersion && from.type != into.type) {
            result.push(
                `drop index if exists ${into.entity.name}_${into.name}`
            );
            result.push(
                `create index ${into.entity.name}_${into.name} ` +
                `on ${into.entity.table} using GIST (${into.name})`
            );
        }
        return result;
    }

    dropDDL(factory: CreatorFactory, from: Field,
            doVersion: boolean): string[] {

        const result: string[] = [];
        if (!doVersion) {
            result.push(
                `drop index if exists ${from.entity.name}_${from.name}`
            );
        }
        const table = !doVersion ? from.entity.table : `${from.entity.table}_v`;
        result.push(
            `alter table ${table} drop column if exists ${from.name}`
        );
        return result;
    }
}

export class EntityDDL implements EntityCreator {

    updateDDL(factory: CreatorFactory, from: Entity, into: Entity,
              doVersion: boolean, dropFirst?: boolean): string {
        if (from.immutable != into.immutable) {
            throw new CoreDDLError(
                `Cannot change ${from.name} from immutable = ${from.immutable} ` +
                `into ${into.immutable}`);
        }
        if (from.immutable && doVersion) {
            return "";
        }
        const result: string[] = [];
        const table = !doVersion ? into.table : `${into.table}_v`;

        // Added / modified fields
        for (const field of into.allFields) {
            const fieldCreator = factory.fieldCreator(field.ddlCreatorClass);
            const fromField = from.findField(field.name);
            if (fromField) {
                const update =
                    fieldCreator.updateDDL(factory, fromField, field,
                                           doVersion, dropFirst);
                if (update.length) {
                    result.push(...update);
                }
            } else {
                const columnDDL = fieldCreator.creationDDL(factory, field,
                                                           doVersion);
                for (const colStatement of columnDDL.ddl) {
                    result.push(`alter table ${table} add ${colStatement}`);
                }
                if (columnDDL.post.length) {
                    result.push(...columnDDL.post);
                }
            }
        }

        if (!doVersion) {
            const fromKeys = [ ...from.keyFields.keys() ];
            const intoKeys = [ ...into.keyFields.keys() ];
            if (fromKeys.toString() != intoKeys.toString()) {
                result.push(
                    `alter table ${into.table} drop constraint ` +
                    `${into.table}_key`
                );
                result.push(`drop index ${into.table}_key`);
                if (intoKeys.length) {
                    result.push(
                        `create index ${into.table}_key on ${into.table} ` +
                        `(${intoKeys.join(", ")})`
                    );
                }
            }
        }

        // Removed fields
        for (const field of from.allFields) {
            if (!into.findField(field.name)) {
                const fieldCreator = factory.fieldCreator(
                    field.ddlCreatorClass);
                const drop = fieldCreator.dropDDL(factory, field, doVersion);
                if (drop.length) {
                    result.push(...drop);
                }
            }
        }
        if (result.length) {
            return `${result.join(";\n")};\n`;
        } else {
            return "";
        }
    }

    creationDDL(factory: CreatorFactory, entity: Entity, doVersion: boolean,
                dropFirst?: boolean): string {
        if (entity.immutable && doVersion) {
            return "";
        }
        const table = !doVersion ? entity.table : `${entity.table}_v`;

        const drop   = `drop table if exists ${table};\n\n`;
        const create = `create table ${table} (\n`;
        const columns: string[] = [];
        if (!doVersion) {
            columns.push("   _id uuid primary key");
            columns.push("   _rev varchar(43) not null");
            columns.push("   inconflict boolean not null");
        } else {
            columns.push("   _id uuid not null");
            columns.push("   _rev varchar(43) not null");
        }
        columns.push("   updated timestamptz not null");
        columns.push("   updatedby uuid not null");

        const clse   = "\n);\n";
        const postTable: string[] = [];

        const fullCreate = dropFirst ? drop + create : create;

        for (const field of entity.allFields) {
            const fieldCreator = factory.fieldCreator(field.ddlCreatorClass);
            const columnDDL = fieldCreator.creationDDL(factory, field,
                                                       doVersion);
            columns.push(...columnDDL.ddl);
            postTable.push(...columnDDL.post);
        }

        if (!doVersion) {
            const entityKeys = [ ...entity.keyFields.keys() ];
            if (entityKeys.length) {
                postTable.push(
                    `create index ${entity.table}_key on ${entity.table} ` +
                    `(${entityKeys.join(", ")})`
                );
            }
        }

        if (doVersion) {
            columns.push("   primary key (_id, _rev)");
        }

        const allColumns = columns.join(",\n");
        const allPost = postTable.join(";\n");
        const postClose = postTable.length > 0 ? ";\n\n" : "\n";
        let tableDDL = fullCreate + allColumns + clse + allPost + postClose;

        // the _vc table
        if (doVersion) {
            const vcTable = `
drop table if exists ${entity.table}_vc;
create table ${entity.table}_vc (
   seq bigserial primary key,
   _id uuid not null,
   updated timestamptz not null,
   updatedby uuid not null,
   versiondepth integer not null,
   ancestry text not null,
   _rev varchar(43) not null,
   isleaf boolean not null,
   isdeleted boolean not null,
   isstub boolean not null,
   constraint ${entity.table}_vc_version unique (_id, _rev)
);

create index ${entity.table}_vc_versiondepth on ${entity.table}_vc (_id, versiondepth desc);

`           ;
            tableDDL += vcTable;
        }

        return tableDDL;
    }

    dropDDL(factory: CreatorFactory, entity: Entity,
            doVersion: boolean): string {
        if (entity.immutable && doVersion) {
            return "";
        }
        const table = !doVersion ? entity.table : `${entity.table}_v`;
        return `drop table if exists ${table};\n`;
    }
}

export class FieldChangeLogEntityDDL extends EntityDDL {

    creationDDL(factory: CreatorFactory, entity: Entity, doVersion: boolean,
                dropFirst?: boolean): string {
        if (doVersion) {
            return "";
        }
        const drop   = `drop table if exists ${entity.table};\n\n`;
        const create = `create table ${entity.table} (\n`;
        const columns: string[] = [];

        columns.push("   _id uuid not null");
        columns.push("   updated timestamptz not null");
        columns.push("   updatedby uuid not null");

        const clse   = "\n);\n";
        const postTable: string[] = [];

        const fullCreate = dropFirst ? drop + create : create;

        for (const field of entity.allFields) {
            const fieldCreator = factory.fieldCreator(field.ddlCreatorClass);
            const columnDDL = fieldCreator.creationDDL(factory, field, false);
            columns.push(...columnDDL.ddl);
            postTable.push(...columnDDL.post);
        }

        columns.push("   primary key (_id, updated)");

        // To facilitate easier searches and reporting, add a regular index
        // (not unique!) for the key fields + updated (desc).
        const keys: string[] = [];
        for (const field of entity.keyFields.values()) {
            keys.push(field.name);
        }
        if (keys.length > 0) {
            keys.push("updated desc");
            postTable.push(`create index ${entity.table}_keys on ` +
                           `${entity.table} (${keys.join(", ")})`);
        }

        const allColumns = columns.join(",\n");
        const allPost = postTable.join(";\n");
        const postClose = postTable.length > 0 ? ";\n\n" : "\n";
        const tableDDL = fullCreate + allColumns + clse + allPost + postClose;

        return tableDDL;
    }

    updateDDL(factory: CreatorFactory, from: Entity, into: Entity,
              doVersion: boolean, dropFirst?: boolean): string {
        if (doVersion) {
            return "";
        }
        return super.updateDDL(factory, from, into, false, dropFirst);
    }

    dropDDL(factory: CreatorFactory, entity: Entity,
            doVersion: boolean): string {

        if (doVersion) {
            return "";
        }
        return super.dropDDL(factory, entity, false);
    }
}

