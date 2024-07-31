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

import { Field, Cfg, Row } from "../../base/core.js";
import { RZO } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData, AttributeJoiner } from "../panel.js";

export class TripViewPanel extends BasePanel implements IPanel {
    appointmentTsField: Cfg<Field>;

    div: HTMLElement;
    nameElement: HTMLElement;
    statusElement: HTMLElement;

    fromAddressPre: HTMLPreElement;
    toAddressPre: HTMLPreElement;

    driverElement: HTMLElement;

    assignBtn: HTMLButtonElement;
    editBtn: HTMLButtonElement;
    backBtn: HTMLButtonElement;

    row: Row | null;
    dateFormat: Intl.DateTimeFormat;
    timeFormat: Intl.DateTimeFormat;

    constructor() {
        super();

        this.div = X.div("trip-view-div");
        this.nameElement = X.heading("trip-view-name-heading");
        this.statusElement = X.p("trip-view-status-p");

        this.fromAddressPre = X.pre("trip-view-fromaddress-pre");
        this.toAddressPre = X.pre("trip-view-toaddress-pre");

        this.driverElement = X.p("trip-view-driver-p");

        this.assignBtn = X.btn("trip-view-assign-btn");
        this.editBtn = X.btn("trip-view-edit-btn");
        this.backBtn = X.btn("trip-view-back-btn");

        this.appointmentTsField = new Cfg("appointmentTsField");

        this.row = null;
        this.dateFormat = new Intl.DateTimeFormat(
            "en",
            { hour12: true, hourCycle: "h12", weekday: "short", month: "short",
              day: "2-digit", formatMatcher: "basic" }
        );
        this.timeFormat = new Intl.DateTimeFormat(
            "en",
            { hour12: true, hourCycle: "h12", hour: "numeric",
              minute: "2-digit", formatMatcher: "basic" }
        );
    }

    get id(): string {
        return "trip-view-panel";
    }

    initialize(): void {
        this.entity.v = RZO.getEntity("trip");
        this.service.v = RZO.getSource("db").service;
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");

        this.editBtn.addEventListener("click", (evt) => {
            this.onEdit(evt);
        });
        this.assignBtn.addEventListener("click", (evt) => {
            this.onAssign(evt);
        });
        this.backBtn.addEventListener("click", (evt) => {
            this.onBack(evt);
        });
    }

    private onAssign(evt: Event): void {
        if (this.row) {
            this.controller.v.stack(
                "trip-assign-panel", new PanelData("Row", this.row));
        }
    }

    private onBack(evt: Event): void {
        this.controller.v.pop();
    }

    private onEdit(evt: Event): void {
        // Stack on the 'TripEdit' panel
        if (this.row) {
            this.controller.v.stack(
                "trip-edit-panel", new PanelData("string", this.row.core._id));
        }
    }

    private rowToUI(row: Row): void {

        const appointmentts =
            this.appointmentTsField.v.transform(row.get("appointmentts"));
        const returnts = row.get("returnts") ?
                this.appointmentTsField.v.transform(row.get("returnts")) :
                null;

        const appointmentDateTime =
            `${this.dateFormat.format(appointmentts)} ` +
            `${this.timeFormat.format(appointmentts)}`;
        const returnTime =
            returnts ? ` - ${this.timeFormat.format(returnts)}` : "";

        this.nameElement.innerText =
            `${row.getString("ridername")} - ` +
            `${appointmentDateTime}${returnTime}`;

        this.statusElement.innerText = new AttributeJoiner().
            add("", `${row.getString("triptype")} - ` +
                    `${row.getString("description")}`).
            add("", `${row.getString("tripnum")} (${row.getString("status")})`).
            toText();

        this.fromAddressPre.innerText = new AttributeJoiner().
            add("Zone", row.getString("zone")).
            add("Address", row.getString("oaddress1")).
            add("Address2", row.getString("oaddress2")).
            add("City", row.getString("ocity")).
            add("Prov/State", row.getString("ostateprov")).
            add("Zip", row.getString("opostalcode")).
            add("Map", row.getString("omaplink")).
            add("Phone", row.getString("ophone")).
            toText();

        this.toAddressPre.innerText = new AttributeJoiner().
            add("Address", row.getString("daddress1")).
            add("Address2", row.getString("daddress2")).
            add("City", row.getString("dcity")).
            add("Prov/State", row.getString("dstateprov")).
            add("Zip", row.getString("dpostalcode")).
            add("Map", row.getString("dmaplink")).
            add("Phone", row.getString("dphone")).
            toText();

        if (!row.isNull("drivername")) {
            this.driverElement.innerText = row.getString("drivername");
        } else {
            this.driverElement.innerText = "(none)";
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            this.service.v.getOne(this.entity.v, panelData.asString)
            .then((row) => {
                this.row = row;
                this.rowToUI(this.row);
                this.div.hidden = false;
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } else if (this.row) {
            this.rowToUI(this.row);
            this.div.hidden = false;
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

