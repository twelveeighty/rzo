/*
    RZO - A Business Application Framework

    Copyright (C) 2025 Frank Vanderham

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

import { Collection, Cfg, Filter, Query, Row } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import { TOASTER } from "../toaster.js";

import { IPanel, PanelData } from "../panel.js";

import { SplitsBasePanel } from "./splitsbasepanel.js";

export class SplitsListPanel extends SplitsBasePanel implements IPanel {
    collection: Cfg<Collection>;
    div: HTMLElement;

    constructor() {
        super();
        this.prefix = "splits";
        this.div = this.qElement("-list-div");
        this.collection = new Cfg("accsplits");
    }

    get id(): string {
        return "splits-panel";
    }

    initialize(): void {
        super.initialize();
        this.collection.v = RZO.getCollection(this.collection.name);
    }

    private load(account: Row): void {
        const id = account.get("_id");
        this.qElement("-list-account-heading").innerText =
            `${account.get("name")} - ${account.get("description")}`;
        this.loadDetails(id, account.get("currency"), account.get("lcurrency"));
        const query = new Query(
            [],
            new Filter().op("account_id", "=", id),
            [
                {field: "account_id", order: "asc"},
                {field: "posted", order: "desc"}
            ]);
        this.collection.v.query(CONTEXT.c, query)
        .then((resultSet) => {
            this.renderSplits("sct", resultSet);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        this.div.hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            if (PanelData.typeOf(panelData) == "Row") {
                this.load(PanelData.rowOf(panelData));
            }
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

