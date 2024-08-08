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
    Entity, Field, State, MemResultSet, Filter, Query, Collection, Cfg
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData } from "../panel.js";

export class DriverViewPanel extends BasePanel implements IPanel {
    tripEntity: Cfg<Entity>;
    tripDrivernumField: Cfg<Field>;
    tripsCollection: Cfg<Collection>;
    appointmentTsField: Cfg<Field>;

    div: HTMLElement;
    nameElement: HTMLElement;
    statusElement: HTMLElement;
    addressPre: HTMLPreElement;
    tripsDivElement: HTMLElement;
    editBtn: HTMLButtonElement;

    abortController: AbortController | null;
    state: State | null;
    dateTimeFormat: Intl.DateTimeFormat;


    constructor() {
        super();

        this.div = X.div("driver-view-div");
        this.nameElement = X.heading("driver-view-name-heading");
        this.statusElement = X.p("driver-view-status-p");
        this.addressPre = X.pre("driver-view-address-pre");
        this.tripsDivElement = X.div("driver-view-trips-div");
        this.editBtn = X.btn("driver-view-edit-btn");

        this.tripEntity = new Cfg("tripEntity");
        this.tripDrivernumField = new Cfg("tripDrivernumField");
        this.tripsCollection = new Cfg("tripsCollection");
        this.appointmentTsField = new Cfg("appointmentTsField");

        this.state = null;
        this.abortController = null;

        this.dateTimeFormat = new Intl.DateTimeFormat(
            "en",
            { dateStyle: "full", timeStyle: "short" }
        );
    }

    get id(): string {
        return "driver-view-panel";
    }

    initialize(): void {
        this.entity.v = RZO.getEntity("driver");
        this.service.v = RZO.getSource("db").service;
        this.tripEntity.v = RZO.getEntity("trip");
        this.tripDrivernumField.v = this.tripEntity.v.getField("drivernum");
        this.tripsCollection.v = RZO.getCollection("trips");
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");

        this.editBtn.addEventListener("click", (evt) => {
            this.onEdit(evt);
        });
    }

    private onEdit(evt: Event): void {
        // Stack on the 'DriverEdit' panel
        if (this.state) {
            this.controller.v.stack(
                "driver-edit-panel", new PanelData("State", this.state));
        }
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as Element;
        if (target && target.id && target.id.length > 4) {
            const _id = target.id.slice(4);
            this.controller.v.stack(
                "trip-view-panel", new PanelData("string", _id));
        }
    }

    private queryTrips(): void {
        if (!this.state) {
            return;
        }
        try {
            if (this.abortController !== null) {
                this.abortController.abort();
                this.abortController = null;
            }
            const query = new Query(
                [],
                new Filter().op(
                    "drivernum_id", "=", this.state.id),
                [ { field: "appointmentts", order: "desc" } ]
            );
            this.tripsCollection.v.query(CONTEXT.session, query)
            .then((resultSet) => {
                this.abortController = new AbortController();
                this.tripsDivElement.innerHTML = "";
                while (resultSet.next()) {
                    const anchor = document.createElement("a");
                    anchor.href = "#";
                    anchor.className =
                        "list-group-item list-group-item-action";
                    anchor.id = `dtl-${resultSet.getString("_id")}`;

                    anchor.addEventListener("click", (evt) => {
                        evt.preventDefault();
                        this.onAnchorClick(evt);
                    },
                    { signal: this.abortController.signal }
                    );

                    this.tripsDivElement.appendChild(anchor);

                    const headingDiv = document.createElement("div");
                    headingDiv.className =
                        "d-flex w-100 justify-content-between";

                    anchor.appendChild(headingDiv);

                    const heading5 = document.createElement("h5");
                    heading5.className = "mb-1";
                    heading5.innerText =
                        `${resultSet.getString("description")}`;

                    const statusSmall = document.createElement("small");
                    const appointmentts = this.dateTimeFormat.format(
                        this.appointmentTsField.v.transform(
                            resultSet.get("appointmentts")));
                    // statusSmall.innerText = resultSet.getString("status");
                    statusSmall.innerText = appointmentts;

                    headingDiv.appendChild(heading5);
                    headingDiv.appendChild(statusSmall);

                    const para = document.createElement("p");
                    para.className = "mb-1";
                    para.innerText = resultSet.getString("daddress1");

                    anchor.appendChild(para);

                    const regionSmall = document.createElement("small");
                    regionSmall.innerText = resultSet.getString("zone");

                    anchor.appendChild(regionSmall);
                }
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            console.error(err);
        }
    }

    private addIfPresent(target: string[], prefix: string, value?: string) {
        if (value) {
            const header = prefix ? `${prefix}: ` : "";
            target.push(`${header}${value}`);
        }
    }

    private stateToUI(state: State): void {
        this.nameElement.innerText = state.asString("name");
        this.statusElement.innerText =
            `${state.asString("drivernum")} (${state.asString("status")})`;

        const values: string[] = [];
        this.addIfPresent(values, "Address", state.asString("address1"));
        this.addIfPresent(values, "Address2", state.asString("address2"));
        this.addIfPresent(values, "City", state.asString("city"));
        this.addIfPresent(values, "Prov/State", state.asString("stateprov"));
        this.addIfPresent(values, "Zip", state.asString("postalcode"));
        this.addIfPresent(values, "Map", state.asString("maplink"));
        this.addIfPresent(values, state.asString("phone1label"),
                          state.asString("phone1"));
        this.addIfPresent(values, state.asString("phone2label"),
                          state.asString("phone2"));
        this.addIfPresent(values, state.asString("phone3label"),
                          state.asString("phone3"));
        this.addressPre.innerText = values.join(`\n`);

        this.queryTrips();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData && panelData.dataType == "Row") {
            const rs = MemResultSet.fromRow(panelData.row);
            rs.next();
            this.state = this.entity.v.from(rs);
            this.stateToUI(this.state);
            this.div.hidden = false;
        } else if (panelData) {
            this.entity.v.load(
                this.service.v, CONTEXT.session, panelData.asString)
            .then((state) => {
                this.state = state;
                this.stateToUI(this.state);
                this.div.hidden = false;
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } else if (this.state) {
            this.stateToUI(this.state);
            this.div.hidden = false;
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

