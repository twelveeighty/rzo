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

import { ServiceSource, Cfg, Entity, Row } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import {
    IPanel, FormPanel, Control, PanelMessage, PanelData
} from "../panel.js";


export class AccountEditPanel extends FormPanel implements IPanel {
    balanceEntity: Cfg<Entity>;

    constructor() {
        super("account-edit-div", "account-edit-form",
              "account-edit-btn", "account-edit-cancel-btn", [
                  new Control("account-name-txt", "name", true),
                  new Control("account-ledger-sel", "ledger", true),
                  new Control("account-holding-sel", "holding", true),
                  new Control("account-lholding-txt", "lholding", false),
                  new Control("account-lcurrency-txt", "lcurrency", false),
                  new Control("account-currency-txt", "currency", false),
                  new Control("account-description-txt", "description", false),
                  new Control("account-element-sel", "elementtype", true),
                  new Control("account-status-sel", "status", true)
              ]);
        this.balanceEntity = new Cfg("accountbalance");
    }

    get id(): string {
        return "account-edit-panel";
    }

    private loadLedgers(): void {
        this.loadDropdown("account-ledger-sel", "ledgers", "name");
    }

    private loadHoldings(): void {
        this.loadDropdown("account-holding-sel", "holdings", "name");
    }

    async onMessage(message: PanelMessage): Promise<void> {
        if (message == "logged-in") {
            if (CONTEXT.c.persona.name == "admins") {
                this.loadLedgers();
                this.loadHoldings();
            }
        }
    }

    protected async save(): Promise<Row> {
        if (this.state) {
            const hasBalance = this.state.hasId();
            const result = await super.save();
            if (!hasBalance) {
                const balance = await this.balanceEntity.v.create(
                    CONTEXT.c, this.service.v);
                balance.field("account").value = result.get("name");
                balance.field("account_id").value = result.get("_id");
                await this.balanceEntity.v.post(
                    this.service.v, balance, CONTEXT.c);
            }
            return result;
        } else {
            throw new Error("this.state must not be null at this point");
        }
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("account");
        this.balanceEntity.v = RZO.getEntity(this.balanceEntity.name);
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.initUI();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (PanelData.typeOf(panelData) == "string") {
            this.state = await this.entity.v.load(
                this.service.v, CONTEXT.c, PanelData.stringOf(panelData));
        } else if (PanelData.typeOf(panelData) == "State") {
            this.state = PanelData.stateOf(panelData);
        } else {
            this.state = await this.entity.v.create(CONTEXT.c, this.service.v);
        }
        this.fromState();
        this.toggleUI(true);
    }
}

