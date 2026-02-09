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
         Collection, Cfg, ServiceSource, Filter, Query, IResultSet, Field
} from "../../base/core.js";
import { MaterializedCollection } from "../../base/collection.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import { IPanel, BasePanel, PanelData } from "../panel.js";

const DATELEN =  "XXXX-XX-XX".length;

export class DocViewPanel extends BasePanel implements IPanel {
    splits: Cfg<Collection>;
    findocs: Cfg<Collection>;
    accounts: Cfg<MaterializedCollection>;
    postedField: Cfg<Field>;

    constructor() {
        super();
        this.prefix = "docview";
        this.splits = new Cfg("splits");
        this.findocs = new Cfg("findocs");
        this.postedField = new Cfg("split.posted");
        this.accounts = new Cfg("accountscache");
    }

    get id(): string {
        return "transview-panel";
    }

    initialize(): void {
        this.splits.v = RZO.getCollection(this.splits.name);
        this.findocs.v = RZO.getCollection(this.findocs.name);
        this.accounts.setIfCast(
            `DocViewPanel: invalid collection: '${this.accounts.name}' `,
            RZO.getCollection(this.accounts.name),
            MaterializedCollection);
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.postedField.v = RZO.getField(this.postedField.name);
        this.qElement("-back-btn").addEventListener("click", (evt) => {
            this.onBack(evt);
        });
    }

    private dayFormat(rs: IResultSet, field: string): string {
        const dt = this.ensureDate(this.postedField.v.transform(rs.get(field)));
        return dt.toISOString().slice(0, DATELEN);
    }

    private addTextTd(tr: HTMLElement, value: string): void {
        const td = document.createElement("td");
        if (value) {
            td.innerText = value;
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

    private async renderFinDoc(resultSet: IResultSet): Promise<void> {
        const tbody = this.qElement("-details");
        tbody.innerHTML = "";
        if (resultSet.next()) {
            const account =
                this.accounts.v.find("_id", resultSet.get("account_id"));
            const acctDesc = !account.empty ?
                account.get("description") : "??unknown??";
            const fullAccount = `${resultSet.get("account")} - ${acctDesc}`;
            this.addTableRowText(tbody, "Num", resultSet.get("transnum"));
            this.addTableRowText(tbody, "Account", fullAccount);
            this.addTableRowText(tbody, "Memo", resultSet.get("memo"));
            this.addTableRowText(tbody,
                            "Posted", this.dayFormat(resultSet, "posted"));
            this.addTableRowText(tbody, "Amount", resultSet.get("amount"));
        }
    }

    private async renderSplits(resultSet: IResultSet): Promise<void> {
        const tbody = this.qElement("-splits-tsec");
        tbody.innerHTML = "";
        while (resultSet.next()) {
            const account =
                this.accounts.v.find("_id", resultSet.get("account_id"));
            const tr = document.createElement("tr");
            const acctDesc = !account.empty ?
                account.get("description") : "??unknown??";
            const fullAccount = `${resultSet.get("account")} - ${acctDesc}`;
            this.addTd(tr, resultSet, "splitnum");
            this.addTextTd(tr, fullAccount);
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
            tbody.appendChild(tr);
        }
    }

    private loadSplits(id: string): void {
        const query = new Query([], new Filter().op("findoc_id", "=", id));
        this.splits.v.query(CONTEXT.c, query)
        .then((resultSet) => {
            this.renderSplits(resultSet);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    private async load(id: string): Promise<void> {
        try {
            if (this.accounts.v.cache.rowCount == 0) {
                await this.accounts.v.build(CONTEXT.c);
            }
            const query = new Query([], new Filter().op("_id", "=", id));
            const rs = await this.findocs.v.query(CONTEXT.c, query);
            this.renderFinDoc(rs);
            this.loadSplits(id);
        } catch (err: any) {
            TOASTER.error(`ERROR: ${err}`);
        }
    }

    private onBack(evt: Event): void {
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }

    async show(panelData?: PanelData): Promise<void> {
        this.qElement("-div").hidden = false;
        if (PanelData.typeOf(panelData) == "string") {
            return this.load(PanelData.stringOf(panelData));
        }
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

