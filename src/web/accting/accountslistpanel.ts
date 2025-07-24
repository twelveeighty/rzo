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

import {
    Collection, Cfg, ServiceSource, Filter, Query, IResultSet
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData } from "../panel.js";

export class AccountsListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    div: HTMLElement;
    listDiv: HTMLElement;
    abortController: AbortController | null;
    defaultQuery: Query;
    lastResult: IResultSet | null;

    constructor() {
        super();
        this.div = X.div("accounts-div");
        this.listDiv = X.div("accounts-list-div");
        this.collection = new Cfg("accounts");
        this.abortController = null;
        const filter = new Filter("or")
        .op("name", "=", "C100")
        .op("name", "=", "C200")
        .op("name", "=", "C300")
        .op("name", "=", "C400")
        .op("name", "=", "C500");
        this.defaultQuery = new Query(
            [], filter, [{field: "name", order: "asc"}]);
        this.lastResult = null;
    }

    get id(): string {
        return "accounts-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection(this.collection.name);
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
    }

    private async drillDownOrIn(id: string): Promise<void> {
        if (this.lastResult) {
            const accountRow = this.lastResult.find(
                (row) => row.getString("_id") == id);
            if (accountRow) {
                const query = new Query(
                    [],
                    new Filter().op("name", "<@", accountRow.getString("name")),
                    [{field: "name", order: "asc"}]);
                const resultSet = await this.collection.v.query(
                    CONTEXT.c, query);
                if (resultSet.rowCount > 0) {
                    this.lastResult = resultSet;
                    this.renderList(resultSet);
                }
            }
        }
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as Element;
        if (target && target.id && target.id.length > 4) {
            this.drillDownOrIn(target.id.slice(4))
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
            /*
            this.controller.v.stack(
                "account-view-panel", new PanelData("string", _id));
            */
        }
    }

    private renderList(resultSet: IResultSet): void {
        if (this.abortController !== null) {
            this.abortController.abort();
        }
        this.listDiv.innerHTML = "";
        this.abortController = new AbortController();
        while (resultSet.next()) {
            const anchor = document.createElement("a");
            anchor.href = "#";
            anchor.className =
                "list-group-item list-group-item-action";
            anchor.id = `arl-${resultSet.getString("_id")}`;
            anchor.addEventListener("click", (evt) => {
                evt.preventDefault();
                this.onAnchorClick(evt);
            },
            { signal: this.abortController.signal }
            );
            this.listDiv.appendChild(anchor);
            const headingDiv = document.createElement("div");
            headingDiv.className =
                "d-flex w-100 justify-content-between";
            anchor.appendChild(headingDiv);
            const heading5 = document.createElement("h5");
            heading5.className = "mb-1";
            heading5.innerText = resultSet.getString("name");
            const eleTypeSmall = document.createElement("small");
            eleTypeSmall.innerText = resultSet.getString("elementtype");
            headingDiv.appendChild(heading5);
            headingDiv.appendChild(eleTypeSmall);
            const para = document.createElement("p");
            para.className = "mb-1";
            para.innerText = resultSet.getString("description");
            anchor.appendChild(para);
            const holding = document.createElement("small");
            holding.innerText = resultSet.getString("holding");
            anchor.appendChild(holding);
        }
    }

    private queryList(): void {
        try {
            this.collection.v.query(CONTEXT.c, this.defaultQuery)
            .then((resultSet) => {
                this.lastResult = resultSet;
                this.renderList(resultSet);
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            TOASTER.error(`ERROR: ${err}`);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = X.a("nav-accounts-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.div.hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            this.queryList();
        }
    }

    hide(): void {
        const nav = X.a("nav-accounts-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.div.hidden = true;
    }

}

