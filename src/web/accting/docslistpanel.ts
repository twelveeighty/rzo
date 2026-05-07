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
         Collection, Cfg, Filter, Query, Row, Field, IResultSet
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import { IPanel, BasePanel, PanelData, DynElement } from "../panel.js";

export class DocsListPanel extends BasePanel implements IPanel {
    docsColl: Cfg<Collection>;
    postedField: Cfg<Field>;

    constructor() {
        super();
        this.prefix = "docview-list";
        this.postedField = new Cfg("findoc.posted");
        this.docsColl = new Cfg("findocs");
    }

    get id(): string {
        return "findocs-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("findoc");
        this.docsColl.v = RZO.getCollection(this.docsColl.name);
        this.postedField.v = RZO.getField(this.postedField.name);
        this.qButton("-back-btn").addEventListener("click", (evt) => {
            this.onBack(evt);
        });
    }

    renderDocs(rs: IResultSet): void {
        const tbody = this.qTableSection("-table-tsec");
        tbody.innerHTML = "";
        while (rs.next()) {
            const tr = document.createElement("tr");
            this.addTd(tr, rs, "docnum", "font-monospace");
            const posted = BasePanel.rzoLocalString(this.ensureDate(
                this.postedField.v.transform(rs.get("posted"))));
            tr.appendChild(
                new DynElement({ tag: "td", text: posted }).asElement());
            this.addTd(tr, rs, "memo");
            this.addTd(tr, rs, "amount", "font-monospace text-end");
            tbody.appendChild(tr);
        }
    }

    private load(account: Row): void {
        const query = new Query(
            [],
            new Filter()
            .op("account_id", "=", account.get("_id")),
            [
                {field: "posted", order: "desc"}
            ]
        );
        this.docsColl.v.query(CONTEXT.c, query)
        .then((rs) => {
            this.renderDocs(rs);
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        this.qElement("-div").hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            if (PanelData.typeOf(panelData) == "Row") {
                const row = PanelData.rowOf(panelData);
                this.qElement("-account-heading").innerText =
                    `${row.get("name")} - ${row.get("description")}`;
                this.load(row);
            }
        }
    }

    onBack(evt: Event): void {
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}


