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
    Collection, Filter, Query, Source, TypeCfg, ClassSpec
} from "./core.js";

export class FauxService implements IService {
    async getGeneratorNext(generatorName: string,
                           context: IContext): Promise<string> {
        return "";
    }

    async getOne(entity: Entity, id: string, rev?: string,
                 context?: IContext): Promise<Row> {
        return new Row();
    }

    async getQueryOne(entity: Entity, filter: Filter,
                      context?: IContext): Promise<Row> {
        return new Row();
    }

    async queryCollection(collection: Collection, context?: IContext,
                          query?: Query): Promise<IResultSet> {
        return new EmptyResultSet();
    }

    async getQuery(entity: Entity, query: Query,
                   context?: IContext): Promise<IResultSet> {
        return new EmptyResultSet();
    }

    async getSequenceId(entity: Entity): Promise<string> {
        return "";
    }

    async put(entity: Entity, id: string, row: Row,
              context: IContext): Promise<Row> {
        return new Row();
    }

    async post(entity: Entity, row: Row, context: IContext): Promise<Row> {
        return new Row();
    }

    async delete(entity: Entity, id: string, rev: string,
                 context: IContext): Promise<void> {
    }

    async deleteImmutable(entity: Entity, id: string,
                          context?: IContext): Promise<void> {
    }

    async queryDeferredToken(parent: string, contained: string,
                             parentField: string, containedField: string,
                             id: string): Promise<DeferredToken | null> {
        return null;
    }

    async getDeferredToken(tokenUuid: string): Promise<DeferredToken | null> {
        return null;
    }

    async putDeferredToken(token: DeferredToken,
                           context: IContext): Promise<number> {
        return 0;
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

