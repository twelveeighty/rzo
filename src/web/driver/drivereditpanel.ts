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

import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { IPanel, FormPanel, Control, PanelData } from "../panel.js";

export class DriverEditPanel extends FormPanel implements IPanel {

    overrideCheckbox: HTMLInputElement;
    maplinkManual: HTMLInputElement;
    maplinkManualControl: Control;

    constructor() {
        super("driver-edit-div", "driver-edit-form",
              "driver-edit-btn", "driver-edit-cancel-btn", [
                  new Control("driver-drivernum-txt", "drivernum", true),
                  new Control("driver-status-sel", "status", true),
                  new Control("driver-name-txt", "name", true),
                  new Control("driver-address1-txt", "address1", true),
                  new Control("driver-address2-txt", "address2", false),
                  new Control("driver-city-txt", "city", false),
                  new Control("driver-stateprov-txt", "stateprov", false),
                  new Control("driver-postalcode-txt", "postalcode", false),
                  new Control("driver-maplink-txt", "maplink", false),
                  new Control("driver-maplinkmanual-txt", "maplinkmanual",
                              false),
                  new Control("driver-phone1-txt", "phone1", true),
                  new Control("driver-phone2-txt", "phone2", false),
                  new Control("driver-phone3-txt", "phone2", false),
              ]);

        this.overrideCheckbox = X.cbox("driver-override-cbox");
        this.maplinkManual = this.getInput("driver-maplinkmanual-txt");
        this.maplinkManualControl = this.getControl("driver-maplinkmanual-txt");
    }

    get id(): string {
        return "driver-edit-panel";
    }

    protected initUI(): void {
        super.initUI();

        this.overrideCheckbox.addEventListener("change", (evt) => {
            this.toggleOverride();
        });
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("driver");
        this.service.v = RZO.getSource("db").service;
        this.initUI();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            this.state = panelData.state;
        } else {
            this.state = await this.entity.v.create(
                CONTEXT.session, this.service.v);
        }
        this.fromState();
        this.toggleUI(true);
    }

    private toggleOverride(): void {
        if (!this.overrideCheckbox.checked) {
            // This is the same as clearing the manual field
            this.maplinkManual.value = "";
            this.onBlur(this.maplinkManualControl);
        }
        this.toggleReadOnly(
            this.maplinkManual, !this.overrideCheckbox.checked);
    }

    protected fromState(): void {
        super.fromState();

        if (this.state) {
            const isChecked = !!this.state.value("maplinkmanual");
            this.overrideCheckbox.checked = isChecked;
            this.toggleReadOnly(this.maplinkManual, !isChecked);
        }
    }
}

