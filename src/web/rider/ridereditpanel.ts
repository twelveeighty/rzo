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
import { TOASTER } from "../toaster.js";
import {
    IPanel, FormPanel, Control, PanelMessage, PanelData
} from "../panel.js";

export class RiderEditPanel extends FormPanel implements IPanel {
    maplinkManualControl: Control;

    constructor() {
        super("rider", "-edit-div", "-edit-form",
              "-edit-btn", "-edit-cancel-btn",
              [
                  new Control("rider-ridernum-txt", "ridernum", true),
                  new Control("rider-zone-sel", "zone", true),
                  new Control("rider-status-sel", "status", true),
                  new Control("rider-name-txt", "name", true),
                  new Control("rider-address1-txt", "address1", true),
                  new Control("rider-address2-txt", "address2", false),
                  new Control("rider-city-txt", "city", false),
                  new Control("rider-stateprov-txt", "stateprov", false),
                  new Control("rider-postalcode-txt", "postalcode", false),
                  new Control("rider-maplink-txt", "maplink", false),
                  new Control("rider-maplinkmanual-txt", "maplinkmanual",
                              false),
                  new Control("rider-phone1-txt", "phone1", true),
                  new Control("rider-phone2-txt", "phone2", false),
                  new Control("rider-phone3-txt", "phone2", false),
                  new Control("rider-comments-tarea", "comments", false),
                  new Control("rider-tripinfo-tarea", "tripinfo", false),
              ]);
        this.maplinkManualControl = this.getControl("-maplinkmanual-txt");
        this.prefix = "rider";
    }

    get id(): string {
        return "rider-edit-panel";
    }

    private loadZones(): void {
        const riderZoneSel = this.getSelect("-zone-sel");
        this.service.v.queryCollection(
            this.logger, CONTEXT.c, RZO.getCollection("zones"))
        .then((resultSet) => {
            while (riderZoneSel.options.length > 1) {
                riderZoneSel.remove(1);
            }
            while (resultSet.next()) {
                const opt = document.createElement("option");
                const zone = resultSet.getString("zone");
                opt.value = zone;
                opt.text = zone;
                riderZoneSel.add(opt);
            }
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    protected initUI(): void {
        super.initUI();
        this.qInput("-override-cbox").addEventListener("change", (evt) => {
            this.toggleOverride();
        });
    }

    async onMessage(message: PanelMessage): Promise<void> {
        if (message == "logged-in") {
            this.loadZones();
        }
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("rider");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.initUI();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (PanelData.typeOf(panelData) == "State") {
            this.state = PanelData.stateOf(panelData);
        } else if (PanelData.typeOf(panelData) == "Row") {
            this.state = this.entity.v.rowToState(PanelData.rowOf(panelData));
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

