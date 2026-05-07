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

import { State, ServiceSource } from "../../base/core.js";
import { RZO } from "../../base/configuration.js";
import { IPanel, ViewPanel, DynElement } from "../panel.js";

export class TestViewPanel extends ViewPanel implements IPanel {
    constructor() {
        super("test", "-view-div", "-view-tsec", "-view-back-btn",
              "-view-edit-btn", "test-edit-panel");
    }

    get id(): string {
        return "test-view-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("testversioned");
        this.service.v =
            (<ServiceSource>RZO.getSource("pouchdb")
             .ensure(ServiceSource)).service;
    }

    protected stateToUI(state: State): void {
        this.tbody.innerHTML = "";
        this.addRowText("Key", state.value("keyfield"));
        this.addRowText("String", state.value("stringfield"));
        this.addRowText("Unique", state.value("uniquefield"));
        this.addRowText("Integer", state.value("integerfield"));
        this.addRowText("Alias", state.value("aliasvaluelist"));
        this.addRowText("Number", state.value("numberfield"));
        this.addRowText("Amount", state.value("amountfield"));
        this.addRowText("Boolean", state.asString("booleanfield"));
        this.addRowText("Date", state.asString("datefield"));
        this.addRowText("DateTime", state.asString("datetimefield"));
        this.addRowText("Uuid", state.value("uuidfield"));
        this.addRowText("Path", state.value("pathfield"));
        this.addTableRowElement(this.tbody, "DB Id",
            new DynElement({ tag: "samp", text: state.id }).asElement());
        this.addTableRowElement(this.tbody, "DB Version",
            new DynElement({ tag: "samp", text: state.rev }).asElement());
    }
}

