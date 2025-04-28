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

import {
    Entity, Field, Cfg, Row, ServiceSource
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import { Trip } from "../../scheduler/trip.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import {
    IPanel, BasePanel, PanelMessage, PanelData, AttributeJoiner, PanelButton
} from "../panel.js";


export class TripViewPanel extends BasePanel implements IPanel {
    appointmentTsField: Cfg<Field>;

    div: HTMLElement;
    statusElement: HTMLElement;

    driverElement: HTMLElement;

    pickupTBody: HTMLTableSectionElement;
    destTBody: HTMLTableSectionElement;
    acceptButton: PanelButton;
    assignButton: PanelButton;
    editButton: PanelButton;
    cloneButton: PanelButton;
    splitButton: PanelButton;
    backBtn: HTMLButtonElement;

    confirmModal: Modal;
    tripAcceptConfirmBtn: HTMLButtonElement;

    splitModal: Modal;
    tripSplitConfirmBtn: HTMLButtonElement;

    driverEntity: Cfg<Entity>;
    driver: Row | null;
    row: Row | null;

    dateFormat: Intl.DateTimeFormat;
    timeFormat: Intl.DateTimeFormat;
    dirty: boolean;

    constructor() {
        super();

        this.div = X.div("trip-view-div");
        this.statusElement = X.htmlElement("trip-view-status-sm");

        this.driverElement = X.p("trip-view-driver-p");
        this.pickupTBody = X.tsec("trip-view-pickup-table-tsec");
        this.destTBody = X.tsec("trip-view-dest-table-tsec");

        const parentDiv = X.div("trip-view-buttons-div");
        this.acceptButton = new PanelButton(
            parentDiv, "trip-view-accept-btn", "Accept ride...");
        this.assignButton = new PanelButton(
            parentDiv, "trip-view-assign-btn", "Assign Driver...");
        this.editButton = new PanelButton(
            parentDiv, "trip-view-edit-btn", "Edit Trip...");
        this.splitButton = new PanelButton(
            parentDiv, "trip-view-split-btn", "Split Return Trip");
        this.cloneButton = new PanelButton(
            parentDiv, "trip-view-clone-btn", "Create Similar...");
        this.backBtn = X.btn("trip-view-back-btn");

        this.confirmModal = new Modal(X.div("trip-confirm-accept-div"));
        this.tripAcceptConfirmBtn = X.btn("trip-accept-confirm-btn");

        this.splitModal = new Modal(X.div("trip-confirm-split-div"));
        this.tripSplitConfirmBtn = X.btn("trip-split-confirm-btn");

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
        this.dirty = false;
    }

    get id(): string {
        return "trip-view-panel";
    }

    private async loadDriver(): Promise<void> {
        const driverId = CONTEXT.c.getSubject("driver");
        this.driver = await this.service.v.getOne(
            this.logger, CONTEXT.c, this.driverEntity.v,
            driverId);
    }

    private onLogin(): void {
        this.driver = null;
        const persona = CONTEXT.c.persona.name;
        if (persona == "drivers") {
            this.acceptButton.show();
            this.assignButton.hide();
            this.editButton.hide();
            this.splitButton.hide();
            this.cloneButton.hide();
            this.loadDriver();
        } else if (persona == "planners" || persona == "admins") {
            this.acceptButton.hide();
            this.assignButton.show();
            this.editButton.show();
            this.splitButton.show();
            this.cloneButton.show();
        } else {
            this.acceptButton.hide();
            this.assignButton.hide();
            this.editButton.hide();
            this.splitButton.hide();
            this.cloneButton.hide();
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
        this.splitButton.initialize((evt) => {
            this.onSplit(evt);
        });
        this.cloneButton.initialize((evt) => {
            this.onClone(evt);
        });
        this.backBtn.addEventListener("click", (evt) => {
            this.onBack(evt);
        });
        this.tripSplitConfirmBtn.addEventListener("click", (evt) => {
            this.onSplitConfirm(evt);
        });
        this.tripAcceptConfirmBtn.addEventListener("click", (evt) => {
            this.onAcceptConfirm(evt);
        });
    }

    private onSplitConfirm(evt: Event): void {
        this.splitModal.hide();
        if (this.row) {
            (<Trip>this.entity.v).splitReturnTrip(
                CONTEXT.c, this.service.v, this.row)
            .then((returnTrip) => {
                this.controller.v.show(
                    "trip-edit-panel", new PanelData("State", returnTrip));
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    private onAcceptConfirm(evt: Event): void {
        this.confirmModal.hide();
        if (this.row && this.driver) {
            const state = this.entity.v.rowToState(this.row);
            this.entity.v.setValue(
                state, "drivernum", this.driver.get("drivernum"),
                CONTEXT.c)
            .then(() => {
                this.entity.v.put(this.service.v, state, CONTEXT.c)
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

    private onSplit(evt: Event): void {
        if (this.row) {
            this.splitModal.show();
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
        if (!this.dirty) {
            this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
        } else {
            this.dirty = false;
            this.controller.v.pop();
        }
    }

    private onEdit(evt: Event): void {
        if (this.row) {
            this.controller.v.stack(
                "trip-edit-panel", new PanelData("string", this.row.core._id));
        }
    }

    private onClone(evt: Event): void {
        if (this.row) {
            (<Trip>this.entity.v).cloneTrip(CONTEXT.c, this.service.v,
                                            this.row)
            .then((newTrip) => {
                this.controller.v.stack(
                    "trip-edit-panel", new PanelData("State", newTrip));
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    private addDirections(tBody: HTMLTableSectionElement, row: Row): void {
        const driverHome = this.driver != null ?
            X.mapLink(this.driver.get("maplink")) : null;
        const riderOrigin = X.mapLink(row.get("omaplink"));
        const riderDest = X.mapLink(row.get("dmaplink"));
        const tr = document.createElement("tr");
        const th = document.createElement("th");
        th.setAttribute("scope", "row");
        th.innerText = "Directions";

        const td = document.createElement("td");

        const dirAnchor = document.createElement("a");
        dirAnchor.setAttribute(
            "href", `https://maps.google.com/maps?saddr=${riderOrigin}&` +
                `daddr=${riderDest}`);
        dirAnchor.setAttribute("target", "new");
        X.addSVG(dirAnchor, "trip-directions-sym");
        dirAnchor.appendChild(new Text("Trip Directions"));

        let threeWayAnchor;
        if (driverHome) {
            threeWayAnchor = document.createElement("a");
            threeWayAnchor.setAttribute(
                "href", `https://maps.google.com/maps?saddr=${driverHome}&` +
                    `daddr=${riderOrigin}+to:${riderDest}`);
            threeWayAnchor.setAttribute("target", "new");
            X.addSVG(threeWayAnchor, "trip-3way-sym");
            threeWayAnchor.appendChild(new Text("Three-way directions"));
        }

        td.appendChild(dirAnchor);
        if (threeWayAnchor) {
            td.appendChild(new Text("\u00A0\u00A0"));
            td.appendChild(threeWayAnchor);
        }

        tr.appendChild(th);
        tr.appendChild(td);
        tBody.appendChild(tr);
    }

    private rowToUI(row: Row): void {
        this.pickupTBody.innerHTML = "";
        this.destTBody.innerHTML = "";
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

        X.addRowText(this.pickupTBody, "Rider", row.getString("ridername"));
        X.addRowText(this.pickupTBody, "Date/Time",
                    `${appointmentDateTime}${returnTime}`);
        X.addRowText(this.pickupTBody, "Type", row.getString("triptype"));
        X.addRowText(this.pickupTBody, "Zone", row.getString("zone"));
        X.addRowText(this.pickupTBody, "From",
                        row.getString("odescription"));
        X.addRowElement(this.pickupTBody, "Address",
                    X.addressMapAnchor(row.getString("oaddress1"),
                                       row.getString("omaplink")));
        X.addRowText(this.pickupTBody, "Address2", row.getString("oaddress2"));
        X.addRowText(this.pickupTBody, "City", row.getString("ocity"));
        X.addRowText(this.pickupTBody, "Prov/State",
                    row.getString("ostateprov"));
        X.addRowText(this.pickupTBody, "Zip", row.getString("opostalcode"));
        X.addRowText(this.pickupTBody, "Phone", row.getString("ophone"));
        X.addRowText(this.pickupTBody, "Notes", row.getString("comments"),
                           true);

        X.addRowText(this.destTBody, "To", row.getString("ddescription"));
        X.addRowElement(this.destTBody, "Address",
                    X.addressMapAnchor(row.getString("daddress1"),
                                       row.getString("dmaplink")));
        X.addRowText(this.destTBody, "Address2", row.getString("daddress2"));
        X.addRowText(this.destTBody, "City", row.getString("dcity"));
        X.addRowText(this.destTBody, "Prov/State",
                    row.getString("dstateprov"));
        X.addRowText(this.destTBody, "Zip", row.getString("dpostalcode"));
        X.addRowText(this.destTBody, "Phone", row.getString("dphone"));
        this.addDirections(this.destTBody, row);

        if (!row.isNull("drivername")) {
            this.driverElement.innerText = row.getString("drivername");
        } else {
            this.driverElement.innerText = "(none)";
        }

        this.statusElement.innerText = new AttributeJoiner().
            add("", `${row.getString("status")} `).
            add("", `${row.getString("tripnum")}`).
            add("", `${row.getString("_id")} / ${row.getString("_rev")}`).
            toText();

        const persona = CONTEXT.c.persona.name;
        if (persona == "drivers") {
            this.acceptButton.enabled = row.isNull("drivername");
        } else if (persona == "planners" || persona == "admins") {
            this.splitButton.enabled = row.isNull("drivername") &&
                row.get("triptype") == "RETURN";
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        if (PanelData.typeOf(panelData) == "string") {
            this.service.v.getOne(
                this.logger, CONTEXT.c, this.entity.v,
                PanelData.stringOf(panelData))
            .then((row) => {
                this.dirty = false;
                this.row = row;
                this.rowToUI(this.row);
                this.div.hidden = false;
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } else if (PanelData.typeOf(panelData) == "Row") {
            this.row = PanelData.rowOf(panelData);
            this.dirty = true;
            this.rowToUI(this.row);
            this.div.hidden = false;
        } else if (PanelData.isParam("NoRefresh", panelData)) {
            this.dirty = false;
            this.div.hidden = false;
        } else {
            this.div.hidden = false;
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

