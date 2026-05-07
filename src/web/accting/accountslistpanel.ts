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

import { ServiceSource, MemResultSet, Row } from "../../base/core.js";
import { RZO } from "../../base/configuration.js";
import { IPanel, BasePanel, PanelData } from "../panel.js";
import { AccountsList, RowListener } from "./accountlist.js";

export class AccountsListPanel extends BasePanel implements IPanel {
    accountsList: AccountsList;

    constructor() {
        super();
        this.prefix = "accounts";
        this.accountsList = new AccountsList(
            this.qElement("-list-div"), this.qElement("accounts-heading"),
            this.qElement("-crumbs"), "accounts");
    }

    get id(): string {
        return "accounts-panel";
    }

    initialize(): void {
        this.entity.v = RZO.getEntity("account");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        const drillIntoListener: RowListener =
            (row) => { this.onDrillDown(row); };
        const showDetailListener: RowListener =
            (row) => { this.onShowDetail(row); };
        this.accountsList.initialize(drillIntoListener, showDetailListener);
        this.qButton("-create-btn").addEventListener("click", (evt) => {
            this.onCreate(evt);
        });
        this.qButton("-edit-btn").addEventListener("click", (evt) => {
            this.onEdit(evt);
        });
    }

    private onCreate(evt: Event): void {
        this.controller.v.stack("account-edit-panel");
    }

    private onEdit(evt: Event): void {
        if (this.accountsList.lastParentShown) {
            this.controller.v.stack(
                "account-edit-panel",
                new PanelData(
                    "State",
                    this.entity.v.rowToState(
                        this.accountsList.lastParentShown)));
        }
    }

    private onShowDetail(row: Row): void {
        this.controller.v.stack(
            "account-view-panel",
            new PanelData("string", row.getString("_id")));
    }

    private onDrillDown(row: Row): void {
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = this.qElement("nav-accounts-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.qElement("-div").hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            if (PanelData.typeOf(panelData) == "Row") {
                const row = PanelData.rowOf(panelData);
                this.accountsList.setList(
                    MemResultSet.fromRow(row), row.get("_id"));
            } else {
                this.accountsList.showTopLevelAccounts();
            }
        }
    }

    hide(): void {
        const nav = this.qElement("nav-accounts-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.qElement("-div").hidden = true;
    }
}

