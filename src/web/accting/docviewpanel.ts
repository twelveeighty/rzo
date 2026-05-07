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
import { OkCancelDialog } from "../dialogs.js";

export class DocViewPanel extends BasePanel implements IPanel {
    splits: Cfg<Collection>;
    findocs: Cfg<Collection>;
    accounts: Cfg<MaterializedCollection>;
    postedField: Cfg<Field>;
    docId: string | null;
    voidConfirmDlg: OkCancelDialog;

    constructor() {
        super();
        this.prefix = "docview";
        this.docId = null;
        this.splits = new Cfg("splits");
        this.findocs = new Cfg("findocs");
        this.postedField = new Cfg("split.posted");
        this.accounts = new Cfg("accountscache");
        this.voidConfirmDlg = new OkCancelDialog(
            "Void this Document?",
            "Voiding the document will reverse all splits. " +
            "This cannot be undone",
            "Yes, Void Document", "No",
            (evt) => { this.onVoidConfirm(evt); }
        );
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
        this.qElement("-void-btn").addEventListener("click", (evt) => {
            this.onVoid(evt);
        });
        this.qElement("-edit-btn").addEventListener("click", (evt) => {
            this.onEdit(evt);
        });
    }

    private dayFormat(rs: IResultSet, field: string): string {
        const dt = this.ensureDate(this.postedField.v.transform(rs.get(field)));
        return BasePanel.rzoLocalString(dt);
    }

    private getAccountDesc(id: string): string {
        const row = this.accounts.v.find("_id", id);
        return !row.empty ? row.get("description") : "??unknown??";
    }

    private async renderFinDoc(rs: IResultSet): Promise<void> {
        const tbody = this.qElement("-details");
        tbody.innerHTML = "";
        if (rs.next()) {
            this.addRow(tbody, rs, "Num", "docnum", "font-monospace");
            this.addRow(tbody, rs, "Account", "account", "font-monospace");
            this.addTableRowText(
                tbody, "", this.getAccountDesc(rs.get("account_id")));
            this.addRow(tbody, rs, "Memo", "memo");
            this.addTableRowText(tbody, "Posted", this.dayFormat(rs, "posted"));
            this.addRow(tbody, rs, "Amount", "amount", "font-monospace");
        }
    }

    private async renderSplits(rs: IResultSet): Promise<void> {
        const tbody = this.qElement("-splits-tsec");
        tbody.innerHTML = "";
        while (rs.next()) {
            const tr = document.createElement("tr");
            this.addTd(tr, rs, "splitnum", "font-monospace");
            this.addTd(tr, rs, "account", "font-monospace");
            this.addTdText(tr, this.getAccountDesc(rs.get("account_id")));
            this.addTd(tr, rs, "memo");
            this.addTd(tr, rs, "quantity", "font-monospace text-end");
            this.addTd(tr, rs, "price", "font-monospace text-end");
            if (rs.get("change") == "Dr") {
                this.addTd(tr, rs, "amount", "font-monospace text-end");
                tr.appendChild(document.createElement("td"));
            } else {
                tr.appendChild(document.createElement("td"));
                this.addTd(tr, rs, "amount", "font-monospace text-end");
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
            TOASTER.exc(err);
        });
    }

    private async load(id: string): Promise<void> {
        this.docId = null;
        try {
            if (this.accounts.v.cache.rowCount == 0) {
                await this.accounts.v.build(CONTEXT.c);
            }
            const query = new Query([], new Filter().op("_id", "=", id));
            const rs = await this.findocs.v.query(CONTEXT.c, query);
            this.docId = id;
            this.renderFinDoc(rs);
            this.loadSplits(id);
        } catch (err: any) {
            TOASTER.exc(err);
        }
    }

    private async voidDoc(id: string): Promise<void> {
    }

    private onVoidConfirm(evt: Event): void {
        if (this.docId) {
            this.voidDoc(this.docId);
        }
    }

    private onBack(evt: Event): void {
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }

    private onVoid(evt: Event): void {
        if (this.docId) {
            this.controller.v.stack(
                "findoc-void-panel", new PanelData("string", this.docId));
        }
    }

    private onEdit(evt: Event): void {
        if (this.docId) {
            this.controller.v.stack(
                "findoc-edit-panel", new PanelData("string", this.docId));
        }
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

