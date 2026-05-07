/*
    RZO - A Business Application Framework

    Copyright (C) 2026 Frank Vanderham

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
         TypeCfg, Collection, CollectionSpec, IConfiguration, Query, IResultSet,
         IContext
} from "../base/core.js";

type MobileCollectionSpec = CollectionSpec & {
    filteredBy: string[];
}

class MobileCollectionError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }
}

export class MobileCollection extends Collection {
    filteredBy: Set<string>;
    usesKeyIndex: boolean;

    constructor(config: TypeCfg<MobileCollectionSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.filteredBy = new Set(config.spec.filteredBy);
        // Add orderBy fields to filteredBy, if not already there
        this.orderBy.forEach((order) => this.filteredBy.add(order.field));
        // Check that the filteredBy does NOT contain _id
        if (this.filteredBy.has("_id")) {
            throw new MobileCollectionError(
                `Collection ${this.name} must not contain '_id' in ` +
                `'filteredBy' and/or 'orderBy'`);
        }
        if (this.via != "collection") {
            throw new MobileCollectionError(
                `MobileCollection ${this.name} must have its 'via' set ` +
                `to 'collection'`);
        }
        this.usesKeyIndex = true;
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        /* If filteredBy and orderBy result in only the key fields being used
         * in the index, mark it as such so that no additional index will be
         * created by the service.
         */
        const keyFields = new Set(this.entity.v.keyFields.keys());
        if (this.filteredBy.size == keyFields.size) {
            for (const field of this.filteredBy.values()) {
                if (!keyFields.has(field)) {
                    this.usesKeyIndex = false;
                    break;
                }
            }
        } else {
            this.usesKeyIndex = false;
        }
    }

    async query(context: IContext, query?: Query): Promise<IResultSet> {
        const finalQuery = await this.createQuery(context, query);
        return this.source.v.service.queryCollection(
            this.logger, context, this, finalQuery);
    }
}

