/*
    RZO - A Business Application Framework

    Copyright (C) 2024 Frank Vanderham

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

import { ServiceSource, State, BooleanField } from "../../base/core.js";
import { RZO } from "../../base/configuration.js";
import { IPanel, ViewPanel, PanelData, DynElement } from "../panel.js";


export class AccountViewPanel extends ViewPanel implements IPanel {
    constructor() {
        super("account", "-view-div", "-view-tsec", "-view-back-btn",
              "-view-edit-btn", "account-edit-panel");
    }

    get id(): string {
        return "account-view-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("account");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.qButton("-view-splits-btn").addEventListener("click", (evt) => {
            this.onSplits(evt);
        });
        this.qButton("-view-txns-btn").addEventListener("click", (evt) => {
            this.onTransactions(evt);
        });
    }

    private onSplits(evt: Event): void {
        if (this.state) {
            const row = this.entity.v.stateToRow(this.state);
            this.controller.v.stack(
                BooleanField.toBoolean(row.get("islogged")) ?
                    "logsplits-panel" :
                    "splits-panel",
                new PanelData("Row", row));
        }
    }

    private onTransactions(evt: Event): void {
    }

    protected stateToUI(state: State): void {
        this.tbody.innerHTML = "";
        this.addRowText("Name", state.asString("name"));
        this.addRowText("Description", state.asString("description"));
        this.addRowText("Ledger", state.asString("ledger"));
        this.addRowText("Holding", state.asString("holding"));
        this.addRowText("Ledger Holding", state.asString("lholding"));
        this.addRowText("Ledger Currency", state.asString("lcurrency"));
        this.addRowText("Element Type", state.asString("elementtype"));
        this.addRowText("Status", state.asString("status"));
        this.addTableRowElement(this.tbody, "DB Id",
            new DynElement({ tag: "samp", text: state.id }).asElement());
        this.addTableRowElement(this.tbody, "DB Version",
            new DynElement({ tag: "samp", text: state.rev }).asElement());
    }
}

