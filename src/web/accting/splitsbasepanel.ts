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
    Cfg, ServiceSource, IResultSet, Field, Collection, Query, Filter
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { BasePanel, PanelData, DynElement } from "../panel.js";

const DATELEN =  "XXXX-XX-XX".length;

export class SplitsBasePanel extends BasePanel {
    postedField: Cfg<Field>;
    accbalances: Cfg<Collection>;
    abortController: AbortController | null;

    constructor() {
        super();
        this.abortController = null;
        this.postedField = new Cfg("split.posted");
        this.accbalances = new Cfg("accountbalances");
    }

    initialize(): void {
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.postedField.v = RZO.getField(this.postedField.name);
        this.accbalances.v = RZO.getCollection(this.accbalances.name);
        this.qButton("-list-back-btn").addEventListener("click", (evt) => {
            this.onBack(evt);
        });
    }

    async loadDetails(id: string, currency: string,
                      lcurrency: string): Promise<void> {
        const tbody = this.qElement("-details");
        tbody.innerHTML = "";
        const balances = await this.accbalances.v.query(
            CONTEXT.c, new Query([], new Filter().op("account_id", "=", id)));
        if (balances.next()) {
            this.addTableRowText(
                tbody, "Current Balance",
                `${balances.get("balance")} ${currency}`, "asText",
                "text-end");
            this.addTableRowText(
                tbody, "Present Value",
                `${balances.get("presentvalue")} ${lcurrency}`, "asText",
                "text-end");
        }
    }

    dayFormat(rs: IResultSet, field: string): string {
        const dt = this.ensureDate(this.postedField.v.transform(rs.get(field)));
        return dt.toISOString().slice(0, DATELEN);
    }

    addDateTd(tr: HTMLElement, rs: IResultSet, field: string): void {
        const td = document.createElement("td");
        if (!rs.isNull(field)) {
            td.innerText = this.dayFormat(rs, field);
        }
        tr.appendChild(td);
    }

    addTd(tr: HTMLElement, rs: IResultSet, field: string,
                  tdClass?: string): void {
        const td = new DynElement(
        {
            tag: "td",
            text: rs.getString(field)
        })
        .addOptionalAttribute("class", tdClass);
        tr.appendChild(td.asElement());
    }

    addAnchor(tr: HTMLElement, rs: IResultSet, idField: string,
              valueField: string, listener: EventListener,
              abortController: AbortController): void {
        const td = new DynElement({ tag: "td" })
        .append(
            new DynElement(
            {
                tag: "a",
                href: "#",
                text: rs.get(valueField),
                data: {
                    id: rs.get(idField)
                }
            })
            .addListener(
                "click", listener, abortController
            )
        );
        tr.appendChild(td.asElement());
    }

    renderSplits(resultSet: IResultSet): void {
        if (this.abortController != null) {
            this.abortController.abort();
        }
        this.abortController = new AbortController();
        const tbody = this.qTableSection("-list-table-tsec");
        tbody.innerHTML = "";
        while (resultSet.next()) {
            const tr = document.createElement("tr");
            this.addTd(tr, resultSet, "splitnum");
            this.addDateTd(tr, resultSet, "posted");
            this.addAnchor(
                tr, resultSet, "findoc_id", "findoc",
                (evt) => {
                    evt.preventDefault();
                    this.onAccountClick(evt);
                },
                this.abortController);
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

    onAccountClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.controller.v.stack(
                "transview-panel", new PanelData("string", id));
        }
    }

    onBack(evt: Event): void {
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }
}

