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

import { Modal } from "bootstrap";

import { Entity, Field, Cfg, Row, ServiceSource } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import {
    IPanel, BasePanel, PanelMessage, PanelData, AttributeJoiner, PanelButton
} from "../panel.js";

export class TripViewPanel extends BasePanel implements IPanel {
    appointmentTsField: Cfg<Field>;

    div: HTMLElement;
    nameElement: HTMLElement;
    statusElement: HTMLElement;

    fromAddressPre: HTMLPreElement;
    toAddressPre: HTMLPreElement;

    driverElement: HTMLElement;

    acceptButton: PanelButton;
    assignButton: PanelButton;
    editButton: PanelButton;
    backBtn: HTMLButtonElement;

    confirmModal: Modal;
    tripAcceptConfirmBtn: HTMLButtonElement;

    driverEntity: Cfg<Entity>;
    driver: Row | null;
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

        const parentDiv = X.div("trip-view-buttons-div");
        this.acceptButton = new PanelButton(
            parentDiv, "trip-view-accept-btn", "Accept ride...");
        this.assignButton = new PanelButton(
            parentDiv, "trip-view-assign-btn", "Assign Driver...");
        this.editButton = new PanelButton(
            parentDiv, "trip-view-edit-btn", "Edit Trip...");
        this.backBtn = X.btn("trip-view-back-btn");

        this.confirmModal = new Modal(X.div("trip-confirm-accept-div"));
        this.tripAcceptConfirmBtn = X.btn("trip-accept-confirm-btn");

        this.appointmentTsField = new Cfg("appointmentTsField");

        this.driverEntity = new Cfg("driver");
        this.driver = null;
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

    private async loadDriver(): Promise<void> {
        const driverId = CONTEXT.session.getSubject("driver");
        this.driver = await this.service.v.getOne(
            this.logger, CONTEXT.session, this.driverEntity.v,
            driverId);
    }

    private onLogin(): void {
        this.driver = null;
        const persona = CONTEXT.session.persona.name;
        if (persona == "drivers") {
            this.acceptButton.show();
            this.assignButton.hide();
            this.editButton.hide();
            this.loadDriver();
        } else if (persona == "planners" || persona == "admins") {
            this.acceptButton.hide();
            this.assignButton.show();
            this.editButton.show();
        } else {
            this.acceptButton.hide();
            this.assignButton.hide();
            this.editButton.hide();
        }
    }

    async onMessage(message: PanelMessage): Promise<void> {
        if (message == "logged-in") {
            this.onLogin();
        }
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("trip");
        this.driverEntity.v = RZO.getEntity("driver");

        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");

        this.acceptButton.initialize((evt) => {
            this.onAccept(evt);
        });
        this.assignButton.initialize((evt) => {
            this.onAssign(evt);
        });
        this.editButton.initialize((evt) => {
            this.onEdit(evt);
        });
        this.backBtn.addEventListener("click", (evt) => {
            this.onBack(evt);
        });
        this.tripAcceptConfirmBtn.addEventListener("click", (evt) => {
            this.onAcceptConfirm(evt);
        });
    }

    private onAcceptConfirm(evt: Event): void {
        if (this.row && this.driver) {
            this.confirmModal.hide();
            const state = this.entity.v.rowToState(this.row);
            this.entity.v.setValue(
                state, "drivernum", this.driver.get("drivernum"),
                CONTEXT.session)
            .then(() => {
                this.entity.v.put(this.service.v, state, CONTEXT.session)
                .then((row) => {
                    TOASTER.info(`Saved: ${row.getString("_id")}`);
                    this.row = row;
                    this.rowToUI(this.row);
                })
                .catch((err) => {
                    console.error(err);
                    TOASTER.error(`ERROR: ${err}`);
                });
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    private onAccept(evt: Event): void {
        if (this.row) {
            this.confirmModal.show();
        }
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
            add("", `${row.getString("status")} ` +
                    `${row.getString("triptype")} - ` +
                    `${row.getString("description")}`).
            add("", `${row.getString("tripnum")}`).
            add("", `${row.getString("_id")} / ${row.getString("_rev")}`).
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

        const persona = CONTEXT.session.persona.name;
        if (persona == "drivers") {
            this.acceptButton.enabled = row.isNull("drivername");
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            this.service.v.getOne(
                this.logger, CONTEXT.session, this.entity.v, panelData.asString)
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

