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

import { ServiceSource } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { IPanel, FormPanel, Control, PanelData } from "../panel.js";

export class DriverEditPanel extends FormPanel implements IPanel {
    maplinkManualControl: Control;

    constructor() {
        super("driver", "-edit-div", "-edit-form",
              "-edit-btn", "-edit-cancel-btn", [
                  new Control("driver-drivernum-txt", "drivernum", true),
                  new Control("driver-status-sel", "status", true),
                  new Control("driver-name-txt", "name", true),
                  new Control("driver-address1-txt", "address1", true),
                  new Control("driver-address2-txt", "address2", false),
                  new Control("driver-city-txt", "city", false),
                  new Control("driver-stateprov-txt", "stateprov", false),
                  new Control("driver-postalcode-txt", "postalcode", false),
                  new Control("driver-maplink-txt", "maplink", false),
                  new Control("driver-maplinkmanual-txt", "maplinkmanual", false),
                  new Control("driver-phone1-txt", "phone1", true),
                  new Control("driver-phone2-txt", "phone2", false),
                  new Control("driver-phone3-txt", "phone2", false),
              ]);
        this.maplinkManualControl = this.getControl("-maplinkmanual-txt");
    }

    get id(): string {
        return "driver-edit-panel";
    }

    protected initUI(): void {
        super.initUI();
        this.qInput("-override-cbox").addEventListener("change", (evt) => {
            this.toggleOverride();
        });
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("driver");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.initUI();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            this.state = panelData.state;
        } else {
            this.state = await this.entity.v.create(CONTEXT.c, this.service.v);
        }
        this.fromState();
        this.toggleUI(true);
    }

    private toggleOverride(): void {
        if (!this.qInput("-override-cbox").checked) {
            // This is the same as clearing the manual field
            this.qInput("-maplinkmanual-txt").value = "";
            this.onBlur(this.maplinkManualControl);
        }
        this.toggleReadOnly(
            this.qInput("-maplinkmanual-txt"),
            !this.qInput("-override-cbox").checked);
    }

    protected fromState(): void {
        super.fromState();

        if (this.state) {
            const isChecked = !!this.state.value("maplinkmanual");
            this.qInput("-override-cbox").checked = isChecked;
            this.toggleReadOnly(this.qInput("-maplinkmanual-txt"), !isChecked);
        }
    }
}

