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

import { Entity, IConfiguration } from "../base/core.js";
import { CreatorFactory } from "../base/core-ddl.js";
import { stateTableDDL } from "./replication.js";

export class PgCreator {
    config: IConfiguration;
    factory: CreatorFactory;
    output: string[];

    constructor(config: IConfiguration) {
        this.config = config;
        this.factory = new CreatorFactory();
        this.output = [];
    }

    private createTableDDL(doVersion: boolean, entity: Entity): void {
        const creator = this.factory.entityCreator(entity.ddlCreatorClass);
        const ddl = creator.creationDDL(this.factory, entity, doVersion, true);
        this.output.push(ddl);
    }

    async allNewDDL(configJson: string): Promise<void> {
        await this.factory.load(this.config);
        this.output.push(`
create extension if not exists ltree;

drop table if exists dbinfo;
create table dbinfo (
   id varchar(32) primary key,
   info text not null
);

insert into dbinfo (id, info) values (
    'uuid',
    replace(('' || gen_random_uuid()), '-', '')
);

create or replace function db_uuid() returns varchar(32) as $$
    select info from dbinfo where id = 'uuid';
$$ language sql;

drop table if exists deferredtoken;
create table deferredtoken (
    parent varchar(64) not null,
    contained varchar(64) not null,
    parentfield varchar(64) not null,
    containedfield varchar(64) not null,
    id uuid not null,
    token uuid not null,
    updatedbynum text not null,
    updatedby uuid not null,
    updated timestamptz not null,
    constraint deferredtoken_token unique (token),
    primary key (parent, contained, parentfield, containedfield, id)
);

create index deferredtoken_updated on deferredtoken (updated);

drop table if exists leaderelect;
create table leaderelect (
    id integer primary key,
    lastping timestamptz not null,
    leader uuid not null
);
insert into leaderelect values (1, now(), '00000000-0000-1000-8000-000000000000');
`
        );
        const table = false;
        const version = true;
        for (const entity of this.config.entities.values()) {
            this.createTableDDL(table, entity);
            this.createTableDDL(version, entity);
        }
        // Create the replication artifacts
        this.output.push(stateTableDDL(true));
        // Create the NOBODY account
        this.output.push(`
insert into useraccount_vc (
    _id,
    _rev,
    updateseq,
    updated,
    updatedby,
    versiondepth, ancestry,
    isleaf, isdeleted, isstub, isconflict, iswinner)
values (
    '00000000-0000-1000-8000-000000000000',
    '1-f0000000000000000000000000000000',
    nextval('useraccount_vc_useq'),
    CURRENT_TIMESTAMP,
    '00000000-0000-1000-8000-000000000000',
    1, 'f0000000000000000000000000000000',
    true, false, false, false, true
);

insert into useraccount (
    _id,
    _rev,
    useraccountnum, name, email, persona, status)
values (
    '00000000-0000-1000-8000-000000000000',
    '1-f0000000000000000000000000000000',
    'NOBODY', 'Nobody', 'nobody@nobody.com', 'nobody', 'ACTIVE'
);

insert into useraccount_v (
    _id,
    _rev,
    useraccountnum, name, email, persona, status)
values (
    '00000000-0000-1000-8000-000000000000',
    '1-f0000000000000000000000000000000',
    'NOBODY', 'Nobody', 'nobody@nobody.com', 'nobody', 'ACTIVE'
);

copy dbinfo (id, info) from STDIN;
`
        );
        this.output.push(this.encodeContents(configJson));
        this.output.push("\\.\n\n");
    }

    private addOutput(result: string): void {
        if (result) {
            this.output.push(result);
        }
    }

    encodeContents(configJson: string): string {
        //
        // config[tab]<very_long_output_string>
        //
        // 'config' is the value for the 'id' column, followed by an
        // actual tab character (ASCII 9), followed by the contents string,
        // that has all its line breaks replaced with \n.
        //
        const newlineReplaced = configJson.replaceAll('\n', "\\n");
        return `config\t${newlineReplaced}\n`;
    }

    async updateDDL(fromConfig: IConfiguration,
                    configJson: string): Promise<void> {
        await this.factory.load(this.config);
        await this.factory.load(fromConfig);
        for (const into of this.config.entities.values()) {
            const creator = this.factory.entityCreator(into.ddlCreatorClass);
            if (fromConfig.entities.has(into.name)) {
                // Modified entities
                const from = fromConfig.getEntity(into.name);
                this.addOutput(
                    creator.updateDDL(this.factory, from, into, false, false));
                this.addOutput(
                    creator.updateDDL(this.factory, from, into, true, false));
            } else {
                // New entities
                this.addOutput(
                    creator.creationDDL(this.factory, into, false, false));
                this.addOutput(
                    creator.creationDDL(this.factory, into, true, false));
            }
        }
        for (const from of fromConfig.entities.values()) {
            if (!this.config.entities.has(from.name)) {
                const creator = this.factory.entityCreator(
                    from.ddlCreatorClass);
                // Removed entities
                this.addOutput(
                    creator.dropDDL(this.factory, from, false));
                this.addOutput(
                    creator.dropDDL(this.factory, from, true));
            }
        }
        this.output.push(`

delete from dbinfo where id = 'config';
copy dbinfo (id, info) from STDIN;
`
        );
        this.output.push(this.encodeContents(configJson));
        this.output.push("\\.\n\n");
    }
}

