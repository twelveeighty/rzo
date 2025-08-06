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
    Collection, Cfg, ServiceSource, Filter, Query, IResultSet, Row, Field
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData } from "../panel.js";

const DATELEN =  "XXXX-XX-XX".length;

export class SplitsListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    div: HTMLElement;
    title: HTMLElement;
    tbody: HTMLTableSectionElement;
    backBtn: HTMLButtonElement;
    postedField: Cfg<Field>;

    constructor() {
        super();
        this.div = X.div("splits-list-div");
        this.collection = new Cfg("accsplits");
        this.tbody = X.tsec("splits-list-table-tsec");
        this.title = X.heading("splits-list-account-heading");
        this.backBtn = X.btn("splits-list-back-btn");
        this.postedField = new Cfg("accsplit.posted");
    }

    get id(): string {
        return "splits-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection(this.collection.name);
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.postedField.v = RZO.getField(this.postedField.name);
        this.backBtn.addEventListener("click", (evt) => {
            this.onBack(evt);
        });
    }

    private dayFormat(rs: IResultSet, field: string): string {
        const dt = X.ensureDate(this.postedField.v.transform(rs.get(field)));
        return dt.toISOString().slice(0, DATELEN);
    }

    private addDateTd(tr: HTMLElement, rs: IResultSet, field: string): void {
        const td = document.createElement("td");
        if (!rs.isNull(field)) {
            td.innerText = this.dayFormat(rs, field);
        }
        tr.appendChild(td);
    }

    private addTd(tr: HTMLElement, rs: IResultSet, field: string,
                  tdClass?: string): void {
        const td = document.createElement("td");
        if (tdClass) {
            td.className = tdClass;
        }
        if (!rs.isNull(field)) {
            td.innerText = rs.getString(field);
        }
        tr.appendChild(td);
    }

    private renderSplits(resultSet: IResultSet): void {
        this.tbody.innerHTML = "";
        while (resultSet.next()) {
            const tr = document.createElement("tr");
            this.addTd(tr, resultSet, "splitnum");
            this.addDateTd(tr, resultSet, "posted");
            this.addTd(tr, resultSet, "acctrans");
            this.addTd(tr, resultSet, "memo");
            this.addTd(tr, resultSet, "quantity", "text-end");
            this.addTd(tr, resultSet, "price", "text-end");
            if (resultSet.get("change") == "Dr") {
                this.addTd(tr, resultSet, "amount", "text-end");
                tr.appendChild(document.createElement("td"));
            } else {
                tr.appendChild(document.createElement("td"));
                this.addTd(tr, resultSet, "amount", "text-end");
            }
            this.tbody.appendChild(tr);
        }
    }

    private loadAccountFromRow(account: Row): void {
        this.title.innerText = account.getString("name");
        const query = new Query(
            [],
            new Filter().op("account_id", "=", account.getString("_id")),
            [
                {field: "account_id", order: "asc"},
                {field: "posted", order: "desc"}
            ]);
        this.collection.v.query(CONTEXT.c, query)
        .then((resultSet) => {
            this.renderSplits(resultSet);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    private onBack(evt: Event): void {
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }

    async show(panelData?: PanelData): Promise<void> {
        this.div.hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            if (PanelData.typeOf(panelData) == "Row") {
                this.loadAccountFromRow(PanelData.rowOf(panelData));
            }
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

