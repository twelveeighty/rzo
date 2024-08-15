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
    Row, IService, IContext, IResultSet, EmptyResultSet, Entity, DeferredToken,
    Collection, Filter, Query, Source, TypeCfg, ClassSpec, Logger
} from "./core.js";

export class FauxService implements IService {
    async getGeneratorNext(logger: Logger, context: IContext,
                     generatorName: string): Promise<string> {
        return "";
    }

    async getOne(logger: Logger, context: IContext, entity: Entity, id: string,
           rev?: string): Promise<Row> {
        return new Row();
    }

    async getQueryOne(logger: Logger, context: IContext, entity: Entity,
                filter: Filter): Promise<Row> {
        return new Row();
    }

    async queryCollection(logger: Logger, context: IContext, collection: Collection,
                    query?: Query): Promise<IResultSet> {
        return new EmptyResultSet();
    }

    async getQuery(logger: Logger, context: IContext, entity: Entity,
             query: Query): Promise<IResultSet> {
        return new EmptyResultSet();
    }

    async getSequenceId(logger: Logger, context: IContext,
                  entity: Entity): Promise<string> {
        return "";
    }

    async put(logger: Logger, context: IContext, entity: Entity, id: string,
        row: Row): Promise<Row> {
        return new Row();
    }

    async post(logger: Logger, context: IContext, entity: Entity,
         row: Row): Promise<Row> {
        return new Row();
    }

    async delete(logger: Logger, context: IContext, entity: Entity, id: string,
           rev: string): Promise<void> {
    }

    async deleteImmutable(logger: Logger, context: IContext, entity: Entity,
                    id: string): Promise<void> {
    }

    async queryDeferredToken(logger: Logger, context: IContext, parent: string,
                       contained: string, parentField: string,
                       containedField: string,
                       id: string): Promise<DeferredToken | null> {
        return null;
    }

    async getDeferredToken(logger: Logger, context: IContext,
                     tokenUuid: string): Promise<DeferredToken | null> {
        return null;
    }

    async putDeferredToken(logger: Logger, context: IContext,
                     token: DeferredToken): Promise<number> {
        return 0;
    }

    async getDBInfo(logger: Logger, context: IContext): Promise<Row> {
        return new Row();
    }
}

export class FauxSource extends Source {
    private _service: FauxService;

    constructor(config: TypeCfg<ClassSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this._service = new FauxService();
    }

    get service(): IService {
        return this._service;
    }
}

