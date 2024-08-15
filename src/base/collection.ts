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
    IConfiguration, Cfg, IContext, TypeCfg, Collection, CollectionSpec, Filter,
    Entity, ContainedEntity, Query
} from "./core.js";

class CollectionError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }
}

type PersonaCollectionSpec = CollectionSpec & {
    filter: string;
}

export class PersonaCollection extends Collection {
    cfgFilter: string;
    matches: RegExpMatchArray[];

    static personaRegex = /\$of\s+\$\{my\.(\w+)}/g;

    static replaceMatch(source: string, start: number, len: number,
                       replacement: string): string {
        const newStr = source.slice(0, start) + replacement +
            source.slice(start + len);
        return newStr;
    }

    constructor(config: TypeCfg<CollectionSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        const personaConfig = <PersonaCollectionSpec>config.spec;
        if (!personaConfig.filter) {
            throw new CollectionError(`PersonaCollection ${this.name} must ` +
                                      `have a 'filter' defined`);
        }
        this.cfgFilter = personaConfig.filter;
        try {
            this.matches =
                [ ...this.cfgFilter.matchAll(PersonaCollection.personaRegex) ];
        } catch (error) {
            throw new CollectionError(`Regular expression parsing error for ` +
                                      `PersonaCollection '${this.name}', ` +
                                      `trying to parse '${this.cfgFilter}'`,
                { cause: error });
        }
        // we'll replace matches in reverse order to keep the original
        // positions.
        this.matches.reverse();
    }

    async createFilter(context: IContext, filter?: Filter): Promise<Filter> {
        if (!context) {
            throw new CollectionError(`Collection '${this.name}' requires ` +
                                      `a context `);
        }
        const persona = context.persona;
        const userAccountId = context.userAccountId;
        let colWhere = this.cfgFilter;
        for (const match of this.matches) {
            const matchPosition = match["index"];
            if (matchPosition === undefined) {
                continue;
            }
            const matchLen = match[0].length;
            const membershipName = match[1];
            const membership = persona.memberships.get(membershipName);
            if (!membership) {
                throw new CollectionError(`Collection '${this.name}' cannot ` +
                                          `parse '${match[0]}' clause because` +
                                          ` membership '${membershipName}' ` +
                                          `does not exist for persona ` +
                                          `'${persona.name}'`);
            }
            const entity = membership.entity;
            const members = await entity.getMembers(
                membership.source.service!, context, userAccountId,
                membership.through);
            if (members.length == 0) {
                // no members found, so we purposely return a comparison
                // with null, which always evaluates to false
                colWhere = PersonaCollection.replaceMatch(colWhere,
                                        matchPosition, matchLen,
                                        "= null");
            } else if (members.length == 1) {
                // single match found, use 'equals'
                colWhere = PersonaCollection.replaceMatch(colWhere,
                                        matchPosition, matchLen,
                                        `= '${members[0]}'`);
            } else {
                // multiple matches found, use 'in'
                const quoted: string[] = [];
                members.forEach((member) => quoted.push(`'${member}'`));
                const inClause = `in (${quoted.join()})`;
                colWhere = PersonaCollection.replaceMatch(colWhere,
                                        matchPosition, matchLen,
                                        inClause);
            }
        }
        const finalFilter = new Filter();
        if (filter && filter.notEmpty) {
            finalFilter.seal(`(${colWhere}) and (${filter.where})`);
        } else {
            finalFilter.seal(colWhere);
        }
        return finalFilter;
    }
}

type ContainedCollectionSpec = CollectionSpec & {
    for: string;
}

export class ContainedCollection extends Collection {
    for: Cfg<Collection>;

    constructor(config: TypeCfg<ContainedCollectionSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.for = new Cfg(config.spec.for);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        if (!(this.entity.v instanceof ContainedEntity)) {
            throw new CollectionError(
                `ContainedCollection '${this.name}' entity ` +
                    `'${this.entity.name}' is not a ContainedEntity`);
        }
        this.for.setIf(
            `ContainedCollection ${this.name} has invalid 'for' config: `,
            configuration.collections.get(this.for.name)
        );
        const ourEntity = <ContainedEntity>this.entity.v;
        if (!ourEntity.parentNames.includes(this.for.v.entity.name)) {
            throw new CollectionError(
                `ContainedCollection '${this.name}' references a 'for' ` +
                `collection '${this.for.name}' that does not hold ` +
                `a parent entity for '${this.entity.name}'`);
        }
    }

    async createFilter(context: IContext, filter?: Filter): Promise<Filter> {
        const forCollection = this.for.v;
        const ourTable = this.entity.v.table;
        const pTable = forCollection.entity.v.table;
        const id = forCollection.entity.name + "_id";
        const innerFilter = await forCollection.createFilter(context);
        const existsWhere = `exists (select 1 from ${pTable} where ` +
                            `(${innerFilter.where}) ` +
                            `and (_id = ${ourTable}.${id}) )`;

        const finalFilter = new Filter();
        if (filter && filter.notEmpty) {
            finalFilter.seal(`( ${existsWhere} ) and ( ${filter.where} )`);
        } else {
            finalFilter.seal(existsWhere);
        }
        return finalFilter;
    }
}

type JoinedTable = {
    entity: string;
    join: string;
    alias: string;
    on: string;
    fields?: string[];
}

type JoinedCollectionSpec = CollectionSpec & {
    with: JoinedTable[];
}

export class JoinedCollection extends Collection {
    joinedWith: JoinedTable[];
    joinedEntities: Map<string, Entity>;

    constructor(config: TypeCfg<JoinedCollectionSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.joinedWith = config.spec.with;
        this.joinedEntities = new Map();
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        for (const withCfg of this.joinedWith) {
            this.joinedEntities.set(
                withCfg.entity, configuration.getEntity(withCfg.entity));
        }
        // Alias the main table fields by prefixing the fields set by our super
        this.fields = this.prefixFields(this.fields, this.entity.v.table);
    }

    private prefixFields(fields: string[], prefix: string,
                         aliased?: boolean): string[] {
        if (aliased) {
            return Array.from(
                fields,
                (oldField) => `${prefix}.${oldField} "${prefix}_${oldField}"`);
        } else {
            return Array.from(fields, (oldField) => `${prefix}.${oldField}`);
        }
    }

    async createQuery(context: IContext, query?: Query): Promise<Query> {
        const finalFilter = await this.createFilter(context, query?.filter);
        let finalFields =
            (query && !query.selectStar) ?
               this.prefixFields(query.fields, this.entity.v.table) :
               Array.from(this.fields);
        for (const joinTable of this.joinedWith) {
            const joinEntity = this.joinedEntities.get(joinTable.entity);
            let joinFields: string[] = [];
            if (joinTable.fields && !Query.isSelectStar(joinTable.fields)) {
                joinFields =
                    this.prefixFields(joinTable.fields, joinTable.alias, true);
            } else {
                joinFields =
                    this.prefixFields(joinEntity!.allFieldColumns,
                                      joinTable.alias, true);
            }
            finalFields = finalFields.concat(joinFields);
        }
        let fromClause = `from ${this.entity.v.table}`;
        for (const joinTable of this.joinedWith) {
            const joinEntity = this.joinedEntities.get(joinTable.entity);
            const joinClause =
                ` ${joinTable.join} join ${joinEntity!.table} ` +
                `as ${joinTable.alias} on ` +
                `(${joinTable.alias}._id = ` +
                  `${this.entity.v.table}.${joinTable.on})`;
            fromClause += joinClause;
        }
        const finalOrderBy = (query && query.orderBy.length > 0) ?
            query.orderBy : this.orderBy;
        const result = new Query(finalFields, finalFilter, finalOrderBy);
        result.fromClause = fromClause;
        return result;
    }
}

