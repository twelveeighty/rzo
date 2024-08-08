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
    Field, State, Filter, Query, Collection, Cfg, Row
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import {
    IPanel, BasePanel, PanelData, AttributeJoiner
} from "../panel.js";

export class TripAssignPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    appointmentTsField: Cfg<Field>;
    drivernumField: Cfg<Field>;

    div: HTMLElement;
    nameElement: HTMLElement;
    statusElement: HTMLElement;
    driverElement: HTMLElement;

    driversDiv: HTMLElement;

    backBtn: HTMLButtonElement;
    unassignBtn: HTMLButtonElement;

    row: Row | null;
    state: State | null;
    dateTimeFormat: Intl.DateTimeFormat;
    abortController: AbortController | null;

    constructor() {
        super();

        this.div = X.div("trip-assign-div");
        this.nameElement = X.heading("trip-assign-name-heading");
        this.statusElement = X.p("trip-assign-status-p");
        this.driverElement = X.p("trip-assign-driver-p");
        this.driversDiv = X.div("trip-assign-drivers-div");
        this.backBtn = X.btn("trip-assign-back-btn");
        this.unassignBtn = X.btn("trip-assign-unassign-btn");

        this.collection = new Cfg("collection");
        this.appointmentTsField = new Cfg("appointmentTsField");
        this.drivernumField = new Cfg("drivernumField");

        this.row = null;
        this.state = null;
        this.abortController = null;

        this.dateTimeFormat = new Intl.DateTimeFormat(
            "en",
            { dateStyle: "full", timeStyle: "short" }
        );
    }

    get id(): string {
        return "trip-assign-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection("drivers");
        this.entity.v = RZO.getEntity("trip");
        this.service.v = RZO.getSource("db").service;
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");
        this.drivernumField.v = RZO.getField("trip.drivernum");

        this.backBtn.addEventListener("click", (evt) => {
            this.onBack(evt);
        });
        this.unassignBtn.addEventListener("click", (evt) => {
            this.onUnassign(evt);
        });
    }

    private onBack(evt: Event): void {
        this.controller.v.pop();
    }

    private onUnassign(evt: Event): void {
        if (this.state && this.row) {
            const trip_id = this.row.getString("_id");
            this.drivernumField.v.setValue(this.state, null, CONTEXT.session)
            .then(() => {
                this.entity.v.put(this.service.v, this.state!, CONTEXT.session)
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

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as Element;
        if (target && target.id && target.id.length > 4) {
            const driver_num = target.id.slice(4);
            if (this.state && this.row) {
                const trip_id = this.row.getString("_id");
                this.drivernumField.v.setValue(
                    this.state, driver_num, CONTEXT.session)
                .then(() => {
                    this.entity.v.put(
                        this.service.v, this.state!, CONTEXT.session)
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
    }

    private queryDrivers(): void {
        try {
            if (this.abortController !== null) {
                this.abortController.abort();
                this.abortController = null;
            }
            const query = new Query(
                [],
                new Filter().op("status", "=", "ACTIVE")
            );
            this.collection.v.query(CONTEXT.session, query)
            .then((resultSet) => {
                this.abortController = new AbortController();
                this.driversDiv.innerHTML = "";
                while (resultSet.next()) {
                    const anchor = document.createElement("a");
                    anchor.href = "#";
                    anchor.className =
                        "list-group-item list-group-item-action";
                    anchor.id = `adl-${resultSet.getString("drivernum")}`;

                    anchor.addEventListener("click", (evt) => {
                        evt.preventDefault();
                        this.onAnchorClick(evt);
                    },
                    { signal: this.abortController.signal }
                    );

                    this.driversDiv.appendChild(anchor);

                    const headingDiv = document.createElement("div");
                    headingDiv.className =
                        "d-flex w-100 justify-content-between";

                    anchor.appendChild(headingDiv);

                    const heading5 = document.createElement("h5");
                    heading5.className = "mb-1";
                    heading5.innerText = resultSet.getString("name");

                    const statusSmall = document.createElement("small");
                    statusSmall.innerText = resultSet.getString("status");

                    headingDiv.appendChild(heading5);
                    headingDiv.appendChild(statusSmall);

                    const para = document.createElement("p");
                    para.className = "mb-1";
                    para.innerText = resultSet.getString("drivernum");

                    anchor.appendChild(para);

                    const phone1 = document.createElement("small");
                    phone1.innerText = resultSet.getString("phone1");

                    anchor.appendChild(phone1);
                }
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            console.error(err);
        }
    }

    private rowToUI(row: Row): void {
        this.nameElement.innerText = row.getString("ridername");

        const appointmentts = this.dateTimeFormat.format(
            this.appointmentTsField.v.transform(row.get("appointmentts")));

        const returnts = row.get("returnts") ?
            this.dateTimeFormat.format(
                this.appointmentTsField.v.transform(row.get("returnts"))) :
            "";

        this.statusElement.innerText = new AttributeJoiner().
            add("", `${row.getString("tripnum")} (${row.getString("status")})`).
            add("", row.getString("description")).
            add("", row.getString("triptype")).
            add("Pickup", appointmentts).
            add("Return", returnts).
            toText();

        if (!row.isNull("drivername")) {
            this.driverElement.innerText = row.getString("drivername");
            this.unassignBtn.disabled = false;
        } else {
            this.driverElement.innerText = "(none)";
            this.unassignBtn.disabled = true;
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData && panelData.dataType == "Row") {
            this.row = panelData.row;
        }
        if (this.row) {
            this.state = new State(this.entity.v, this.row.core);
            this.entity.v.loadState(this.row, this.state);
            this.rowToUI(this.row);
            this.queryDrivers();
            this.div.hidden = false;
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

