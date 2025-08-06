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
    Collection, Cfg, ServiceSource, Filter, Query, IResultSet, MemResultSet, Row
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData, PanelButton } from "../panel.js";

import { EntityList } from "../list.js";

export class AccountsListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    div: HTMLElement;
    title: HTMLElement;
    list: EntityList;
    defaultQuery: Query;
    lastResult: IResultSet | null;
    lastSelected: Row | null;
    createBtn: HTMLButtonElement;
    editBtn: HTMLButtonElement;
    splitsBtn: PanelButton;
    transBtn: PanelButton;

    constructor() {
        super();
        this.div = X.div("accounts-div");
        const btnDiv = X.div("accounts-buttons-div");
        this.createBtn = X.btn("accounts-div-create-btn");
        this.editBtn = X.btn("accounts-div-edit-btn");
        this.splitsBtn = new PanelButton(
            btnDiv, "accounts-splits-btn", "View Splits...");
        this.transBtn = new PanelButton(
            btnDiv, "accounts-trans-btn", "View Transactions...");
        this.list = new EntityList(
            X.div("accounts-list-div"),
            "arl",
            "name",
            "elementtype",
            "description",
            "holding");
        this.title = X.heading("accounts-heading");
        this.collection = new Cfg("accounts");
        const filter = new Filter("or")
        .op("name", "=", "C100")
        .op("name", "=", "C200")
        .op("name", "=", "C300")
        .op("name", "=", "C400")
        .op("name", "=", "C500");
        this.defaultQuery = new Query(
            [], filter, [{field: "name", order: "asc"}]);
        this.lastResult = null;
        this.lastSelected = null;
    }

    get id(): string {
        return "accounts-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection(this.collection.name);
        this.entity.v = RZO.getEntity("account");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.list.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
        this.createBtn.addEventListener("click", (evt) => {
            this.onCreate(evt);
        });
        this.editBtn.addEventListener("click", (evt) => {
            this.onEdit(evt);
        });
        this.splitsBtn.initialize((evt) => {
            this.onSplits(evt);
        });
        this.transBtn.initialize((evt) => {
            this.onTransactions(evt);
        });
    }

    private onCreate(evt: Event): void {
        this.controller.v.stack("account-edit-panel");
    }

    private onEdit(evt: Event): void {
        if (this.lastSelected) {
            this.controller.v.stack(
                "account-edit-panel",
                new PanelData("State",
                              this.entity.v.rowToState(this.lastSelected)));
        }
    }

    private onSplits(evt: Event): void {
        if (this.lastSelected) {
            this.navToAccountSplits(this.lastSelected);
        }
    }

    private onTransactions(evt: Event): void {
    }

    private navToAccountSplits(row: Row): void {
        this.controller.v.stack(
            "splits-panel", new PanelData("Row", row));
    }

    private setButtonsVisible(visible: boolean): void {
        if (visible) {
            this.splitsBtn.show();
            this.transBtn.show();
        } else {
            this.splitsBtn.hide();
            this.transBtn.hide();
        }
    }

    private async drillDownOrIn(id: string): Promise<void> {
        if (this.lastResult) {
            const selectedRow = this.lastResult.find(
                (row) => row.get("_id") == id);
            if (selectedRow) {
                this.lastSelected = selectedRow;
                this.title.innerText = selectedRow.getString("name");
                this.setButtonsVisible(true);
                const query = new Query(
                    [],
                    new Filter().op(
                        "name", "<@", selectedRow.getString("name")),
                        [{field: "name", order: "asc"}]);
                const resultSet = await this.collection.v.query(
                    CONTEXT.c, query);
                if (resultSet.rowCount > 0) {
                    this.lastResult = resultSet;
                    this.list.render(resultSet);
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
        }
    }

    private queryList(): void {
        this.setButtonsVisible(false);
        this.title.innerText = "Top-level Accounts";
        this.lastSelected = null;
        this.collection.v.query(CONTEXT.c, this.defaultQuery)
        .then((resultSet) => {
            this.lastResult = resultSet;
            this.list.render(resultSet);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = X.a("nav-accounts-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.div.hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            if (PanelData.typeOf(panelData) == "Row") {
                const row = PanelData.rowOf(panelData);
                this.lastResult = MemResultSet.fromRow(row);
                this.drillDownOrIn(row.get("_id"));
            } else {
                this.queryList();
            }
        }
    }

    hide(): void {
        const nav = X.a("nav-accounts-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.div.hidden = true;
    }
}

