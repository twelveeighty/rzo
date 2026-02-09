/*
    RZO - A Business Application Framework

    Copyright (C) 2024-2025 Frank Vanderham

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

import {
    Entity, Field, Cfg, Row, ServiceSource
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { Trip } from "../../scheduler/trip.js";
import { TOASTER } from "../toaster.js";
import {
    IPanel, BasePanel, PanelMessage, PanelData, DynElement, PanelButton
} from "../panel.js";
import { OkCancelDialog } from "../dialogs.js";

export class TripViewPanel extends BasePanel implements IPanel {
    appointmentTsField: Cfg<Field>;
    div: HTMLElement;
    acceptButton: PanelButton;
    assignButton: PanelButton;
    editButton: PanelButton;
    cloneButton: PanelButton;
    splitButton: PanelButton;
    acceptConfirmDlg: OkCancelDialog;
    splitConfirmDlg: OkCancelDialog;
    driverEntity: Cfg<Entity>;
    driver: Row | null;
    row: Row | null;
    dateFormat: Intl.DateTimeFormat;
    timeFormat: Intl.DateTimeFormat;
    dirty: boolean;
    driverAbortController: AbortController | null;
    driverClickedListener: EventListener;

    constructor() {
        super();
        this.prefix = "trip";
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
        this.div = this.qElement("-view-div");
        const parentDiv = this.qElement("-view-buttons-div");
        this.acceptButton = new PanelButton(
            parentDiv, this.fqId("-view-accept-btn"), "Accept ride...",
            "btn btn-primary me-2");
        this.assignButton = new PanelButton(
            parentDiv, this.fqId("-view-assign-btn"), "Assign Driver...",
            "btn btn-primary me-2");
        this.editButton = new PanelButton(
            parentDiv, this.fqId("-view-edit-btn"), "Edit Trip...",
            "btn btn-primary me-2");
        this.splitButton = new PanelButton(
            parentDiv, this.fqId("-view-split-btn"), "Split Return Trip",
            "btn btn-primary me-2");
        this.cloneButton = new PanelButton(
            parentDiv, this.fqId("-view-clone-btn"), "Create Similar...",
            "btn btn-primary me-2");
        this.acceptConfirmDlg = new OkCancelDialog(
            "Accept this trip?",
            "This ride will be added to your trips.",
            "Yes, accept", "No",
            (evt) => { this.onAcceptConfirm(evt); }
        );
        this.splitConfirmDlg = new OkCancelDialog(
            "Split Return to One-Ways?",
            "This return trip will be split into two one-ways.",
            "Yes, split", "No",
            (evt) => { this.onSplitConfirm(evt); }
        );
        this.dirty = false;
        this.driverAbortController = null;
        this.driverClickedListener = (evt) => { this.onDriverClicked(evt); };
    }

    get id(): string {
        return "trip-view-panel";
    }

    private async loadDriver(): Promise<Row | null> {
        const driverId = CONTEXT.c.getSubject("driver");
        if (driverId) {
            this.driver = await this.service.v.getOne(
                this.logger, CONTEXT.c, this.driverEntity.v, driverId);
        } else {
            this.driver = null;
        }
        return this.driver;
    }

    private onLogin(): void {
        this.driver = null;
        const persona = CONTEXT.c.persona.name;
        if (persona == "drivers") {
            this.loadDriver().then((driver) => {
                if (driver) {
                    this.acceptButton.show();
                    this.assignButton.hide();
                    this.editButton.hide();
                    this.splitButton.hide();
                    this.cloneButton.hide();
                } else {
                    this.acceptButton.hide();
                    this.assignButton.hide();
                    this.editButton.hide();
                    this.splitButton.hide();
                    this.cloneButton.hide();
                }
            });
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
        this.qButton("-view-back-btn").addEventListener("click", (evt) => {
            this.onBack(evt);
        });
    }

    private onSplitConfirm(evt: Event): void {
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

    private onDriverClicked(evt: Event): void {
        evt.preventDefault();
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.controller.v.stack(
                "driver-view-panel",
                new PanelData("string", id));
        }
    }

    private onAcceptConfirm(evt: Event): void {
        if (this.row && this.driver) {
            const state = this.entity.v.rowToState(this.row);
            this.entity.v.setValue(
                state, "drivernum", this.driver.get("drivernum"),
                CONTEXT.c)
            .then(() => {
                this.entity.v.put(this.service.v, state, CONTEXT.c)
                .then((row) => {
                    TOASTER.info(
                        "Thank you, this trip has been added to your trips");
                    this.row = row;
                    this.rowToUI(this.row);
                })
                .catch((err) => {
                    TOASTER.exc(err);
                });
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
        }
    }

    private onSplit(evt: Event): void {
        if (this.row) {
            this.splitConfirmDlg.show();
        }
    }

    private onAccept(evt: Event): void {
        if (this.row) {
            this.acceptConfirmDlg.show();
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

    private addDirections(tBody: HTMLElement, row: Row): void {
        const driverHome = this.driver != null ?
            this.mapLink(this.driver.get("maplink")) : null;
        const riderOrigin = this.mapLink(row.get("omaplink"));
        const riderDest = this.mapLink(row.get("dmaplink"));
        const td = new DynElement({ tag: "td" })
        .append(
            new DynElement(
            {
                tag: "a",
                href: `https://maps.google.com/maps?saddr=${riderOrigin}&` +
                        `daddr=${riderDest}`
            })
            .addAttribute("target", "new")
            .appendSVG("trip-directions-sym")
            .appendTextNode("Trip Directions")
        );
        if (driverHome) {
            td.appendTextNode("\u00A0\u00A0")
            .append(
                new DynElement(
                {
                    tag: "a",
                    href: `https://maps.google.com/maps?saddr=${driverHome}&` +
                             `daddr=${riderOrigin}+to:${riderDest}`
                })
                .addAttribute("target", "new")
                .appendSVG("trip-3way-sym")
                .appendTextNode("Three-way directions")
            );
        }
        const tr = new DynElement({ tag: "tr" })
        .append(
            new DynElement(
            {
                tag: "th",
                text: "Directions"
            })
            .addAttribute("scope", "row")
        )
        .append(td);
        tBody.appendChild(tr.asElement());
    }

    private setDriverInfo(row: Row): void {
        const driverTbody = this.qElement("-view-driver-table-tsec");
        if (this.driverAbortController != null) {
            this.driverAbortController.abort();
        }
        driverTbody.innerHTML = "";
        const driverAnchor = new DynElement(
            {
                tag: "a",
                href: "#",
                text: row.get("drivernum"),
                data: {
                    id: row.get("drivernum_id")
                }
            }
        )
        .addListener(
            "click", this.driverClickedListener, this.driverAbortController);
        this.addTableRowElement(
            driverTbody, "Driver", driverAnchor.asElement());
        this.addTableRowText(driverTbody, "Name", row.get("drivername"));
    }

    private rowToUI(row: Row): void {
        const pickup = this.qElement("-view-pickup-table-tsec");
        const dest = this.qElement("-view-dest-table-tsec");
        const status = this.qElement("-view-status-tsec");
        pickup.innerHTML = "";
        dest.innerHTML = "";
        status.innerHTML = "";
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
        this.addTableRowText(pickup, "Rider", row.get("ridername"));
        this.addTableRowText(pickup, "Date/Time",
                    `${appointmentDateTime}${returnTime}`);
        this.addTableRowText(pickup, "Type", row.get("triptype"));
        this.addTableRowText(pickup, "Zone", row.get("zone"));
        this.addTableRowText(pickup, "From", row.get("odescription"));
        this.addTableRowElement(
            pickup, "Address",
            this.addressMapAnchor(row.get("oaddress1"), row.get("omaplink")));
        this.addTableRowText(pickup, "Address2", row.get("oaddress2"));
        this.addTableRowText(pickup, "City", row.get("ocity"));
        this.addTableRowText(pickup, "Prov/State",
                    row.get("ostateprov"));
        this.addTableRowText(pickup, "Zip", row.get("opostalcode"));
        this.addTableRowText(pickup, "Phone", row.get("ophone"));
        this.addTableRowText(pickup, "Notes", row.get("comments"), "asHTML");
        this.addTableRowText(dest, "To", row.get("ddescription"));
        this.addTableRowElement(
            dest, "Address",
            this.addressMapAnchor(row.get("daddress1"), row.get("dmaplink")));
        this.addTableRowText(dest, "Address2", row.get("daddress2"));
        this.addTableRowText(dest, "City", row.get("dcity"));
        this.addTableRowText(dest, "Prov/State", row.get("dstateprov"));
        this.addTableRowText(dest, "Zip", row.get("dpostalcode"));
        this.addTableRowText(dest, "Phone", row.get("dphone"));
        this.addDirections(dest, row);
        this.setDriverInfo(row);
        this.addTableRowText(status, "Status", row.get("status"));
        this.addTableRowText(status, "Trip No", row.get("tripnum"));
        this.addTableRowElement(status, "DB Id",
            new DynElement({ tag: "samp", text: row.get("_id") }).asElement());
        this.addTableRowElement(status, "DB Version",
            new DynElement({ tag: "samp", text: row.get("_rev") }).asElement());
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

