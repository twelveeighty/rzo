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

import {
    Field, State, Filter, Query, Collection, Cfg, Row, ServiceSource
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import { IPanel, BasePanel, PanelData } from "../panel.js";
import { OkCancelDialog } from "../dialogs.js";
import { EntityList } from "../list.js";

export class TripAssignPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    appointmentTsField: Cfg<Field>;
    drivernumField: Cfg<Field>;
    row: Row | null;
    state: State | null;
    dateTimeFormat: Intl.DateTimeFormat;
    list: EntityList;
    assignConfirmDlg: OkCancelDialog;
    unAssignConfirmDlg: OkCancelDialog;

    constructor() {
        super();
        this.prefix = "trip-assign";
        this.collection = new Cfg("collection");
        this.appointmentTsField = new Cfg("appointmentTsField");
        this.drivernumField = new Cfg("drivernumField");
        this.row = null;
        this.state = null;
        this.dateTimeFormat = new Intl.DateTimeFormat(
            "en",
            { dateStyle: "full", timeStyle: "short" }
        );
        this.list = new EntityList(
            this.qElement("-drivers-div"),
            "name",
            "status",
            "drivernum",
            "phone1",
            ["drivernum"]
        );
        this.assignConfirmDlg = new OkCancelDialog(
            "Assign Driver?", "Are you sure you want to assign driver X",
            "Yes, assign", "No",
            (evt) => { this.onAssignConfirm(evt); }
        );
        this.unAssignConfirmDlg = new OkCancelDialog(
            "Remove Driver?", "Are you sure you want to unassign the driver?",
            "Yes, unassign", "No",
            (evt) => { this.onUnAssignConfirm(evt); }
        );
    }

    get id(): string {
        return "trip-assign-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection("drivers");
        this.entity.v = RZO.getEntity("trip");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");
        this.drivernumField.v = RZO.getField("trip.drivernum");
        this.qButton("-back-btn").addEventListener("click", (evt) => {
            this.onBack(evt);
        });
        this.qButton("-unassign-btn").addEventListener("click", (evt) => {
            this.onUnassign(evt);
        });
        this.list.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private onBack(evt: Event): void {
        this.controller.v.pop();
    }

    private onUnAssignConfirm(evt: Event): void {
        if (this.state && this.row) {
            const trip_id = this.row.getString("_id");
            this.drivernumField.v.setValue(this.state, null, CONTEXT.c)
            .then(() => {
                this.entity.v.put(this.service.v, this.state!, CONTEXT.c)
                .then((row) => {
                    this.controller.v.pop(new PanelData("string", trip_id));
                })
                .catch((err) => {
                    TOASTER.error(`ERROR: ${err}`);
                });
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    private onUnassign(evt: Event): void {
        if (this.state && this.row) {
            this.unAssignConfirmDlg.show();
        }
    }

    private onAssignConfirm(evt: Event): void {
        if (this.state && this.row) {
            const tripId = this.row.get("_id");
            const driverNum = this.assignConfirmDlg.row.get("drivernum");
            if (driverNum) {
                this.drivernumField.v.setValue(
                    this.state, driverNum, CONTEXT.c)
                .then(() => {
                    this.entity.v.put(
                        this.service.v, this.state!, CONTEXT.c)
                    .then((row) => {
                        this.controller.v.pop(new PanelData("string", tripId));
                    })
                    .catch((err) => {
                        TOASTER.error(`ERROR: ${err}`);
                    });
                })
                .catch((err) => {
                    TOASTER.error(`ERROR: ${err}`);
                });
            }
        }
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const newDriverNum = target.dataset["drivernum"];
        if (newDriverNum) {
            if (this.state && this.state.value("drivernum")) {
                this.assignConfirmDlg.prompt =
                    `Are you sure you want to reassign this trip from driver ` +
                    `${this.state.value("drivernum")} to ${newDriverNum}?`;
            } else {
                this.assignConfirmDlg.prompt =
                    `Are you sure you want to assign driver ${newDriverNum}?`;
            }
            this.assignConfirmDlg.show(new Row({drivernum: newDriverNum}));
        }
    }

    private async queryDrivers(): Promise<void> {
        try {
            const query = new Query(
                [],
                new Filter().op("status", "=", "ACTIVE")
            );
            const rs = await this.collection.v.query(CONTEXT.c, query);
            this.list.render(rs);
        } catch (err) {
            TOASTER.exc(err);
        }
    }

    private rowToUI(row: Row): void {
        this.qElement("-name-heading").innerText = row.getString("ridername");
        const tbody = this.qElement("-tsec");
        tbody.innerHTML = "";
        this.addTableRowText(tbody, "Trip", row.get("tripnum"));
        this.addTableRowText(tbody, "Description", row.get("odescription"));
        this.addTableRowText(tbody, "Type", row.get("triptype"));
        this.addTableRowText(
            tbody, "Pickup",
            this.dateTimeFormat.format(
                this.appointmentTsField.v.transform(row.get("appointmentts"))));
        this.addTableRowText(
            tbody, "Return",
            row.isNotNullish("returnts") ?
                this.dateTimeFormat.format(
                    this.appointmentTsField.v.transform(row.get("returnts"))) :
                "");
        if (!row.isNull("drivername")) {
            this.qElement("-driver-p").innerText = row.getString("drivername");
            this.qButton("-unassign-btn").disabled = false;
        } else {
            this.qElement("-driver-p").innerText = "(none)";
            this.qButton("-unassign-btn").disabled = true;
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        if (PanelData.typeOf(panelData) == "Row") {
            this.row = PanelData.rowOf(panelData);
        }
        if (this.row) {
            this.state = this.entity.v.rowToState(this.row);
            this.rowToUI(this.row);
            this.queryDrivers();
            this.qElement("-div").hidden = false;
        }
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

