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
    Entity, Field, State, Filter, Query, Collection, Cfg, ServiceSource
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData } from "../panel.js";
import { TripList } from "../trip/triplist.js";


export class RiderViewPanel extends BasePanel implements IPanel {
    tripEntity: Cfg<Entity>;
    tripRidernumField: Cfg<Field>;
    tripsCollection: Cfg<Collection>;
    appointmentTsField: Cfg<Field>;

    div: HTMLElement;
    nameElement: HTMLElement;
    statusElement: HTMLElement;
    addressPre: HTMLPreElement;
    commentsHeading: HTMLElement;
    commentsDiv: HTMLElement;
    tripInfoHeading: HTMLElement;
    tripInfoDiv: HTMLElement;
    createBtn: HTMLButtonElement;
    editBtn: HTMLButtonElement;
    tripList: TripList;

    state: State | null;
    dateTimeFormat: Intl.DateTimeFormat;

    constructor() {
        super();

        this.div = X.div("rider-view-div");
        this.nameElement = X.heading("rider-view-name-heading");
        this.statusElement = X.p("rider-view-status-p");
        this.addressPre = X.pre("rider-view-address-pre");
        this.commentsHeading = X.heading("rider-view-comments-heading");
        this.commentsDiv = X.div("rider-view-comments-div");
        this.tripInfoHeading = X.heading("rider-view-tripinfo-heading");
        this.tripInfoDiv = X.div("rider-view-tripinfo-div");

        this.tripList = new TripList(X.div("rider-view-trips-div"), "vtl");

        this.createBtn = X.btn("rider-view-create-btn");
        this.editBtn = X.btn("rider-view-edit-btn");

        this.tripEntity = new Cfg("tripEntity");
        this.tripRidernumField = new Cfg("tripRidernumField");
        this.tripsCollection = new Cfg("tripsCollection");
        this.appointmentTsField = new Cfg("appointmentTsField");

        this.state = null;

        this.dateTimeFormat = new Intl.DateTimeFormat(
            "en",
            { dateStyle: "full", timeStyle: "short" }
        );
    }

    get id(): string {
        return "rider-view-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("rider");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.tripEntity.v = RZO.getEntity("trip");
        this.tripRidernumField.v = this.tripEntity.v.getField("ridernum");
        this.tripsCollection.v = RZO.getCollection("trips");
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");

        this.createBtn.addEventListener("click", (evt) => {
            this.onCreateTrip(evt);
        });
        this.editBtn.addEventListener("click", (evt) => {
            this.onEdit(evt);
        });

        this.tripList.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private onCreateTrip(evt: Event): void {
        if (this.state) {
            const ridernum = this.state.value("ridernum");
            this.tripEntity.v.create(CONTEXT.c, this.service.v)
            .then((newTrip) => {
                this.tripRidernumField.v.setValue(
                    newTrip, ridernum, CONTEXT.c)
                .then(() => {
                    this.controller.v.stack(
                        "trip-edit-panel", new PanelData("State", newTrip));
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

    private onEdit(evt: Event): void {
        // Stack on the 'RiderEdit' panel
        if (this.state) {
            this.controller.v.stack(
                "rider-edit-panel", new PanelData("State", this.state));
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
            const query = new Query(
                [],
                new Filter().op(
                    "ridernum_id", "=", this.state.id),
                [ { field: "appointmentts", order: "desc" } ]
            );
            this.tripsCollection.v.query(CONTEXT.c, query)
            .then((resultSet) => {
                this.tripList.render(resultSet);
            })
            .catch((err) => {
                console.error(err);
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            console.error(err);
            TOASTER.error(`ERROR: ${err}`);
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
            `${state.asString("ridernum")} (${state.asString("status")})`;

        const values: string[] = [];
        this.addIfPresent(values, "_id", state.id);
        this.addIfPresent(values, "_rev", state.rev);
        this.addIfPresent(values, "Zone", state.asString("zone"));
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

        const comments = state.asString("comments");
        if (comments) {
            this.commentsHeading.hidden = false;
            this.commentsDiv.innerText = comments;
        } else {
            this.commentsDiv.innerText = "";
            this.commentsHeading.hidden = true;
        }

        const tripinfo = state.asString("tripinfo");
        if (tripinfo) {
            this.tripInfoHeading.hidden = false;
            this.tripInfoDiv.innerText = tripinfo;
        } else {
            this.tripInfoDiv.innerText = "";
            this.tripInfoHeading.hidden = true;
        }

        this.queryTrips();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (PanelData.typeOf(panelData) == "Row") {
            this.state = this.entity.v.rowToState(PanelData.rowOf(panelData));
            this.stateToUI(this.state);
            this.div.hidden = false;
        } else if (PanelData.typeOf(panelData) == "string") {
            this.entity.v.load(
                this.service.v, CONTEXT.c, PanelData.stringOf(panelData))
            .then((state) => {
                this.state = state;
                this.stateToUI(this.state);
                this.div.hidden = false;
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } else if (!PanelData.isParam("NoRefresh", panelData) && this.state) {
            this.stateToUI(this.state);
            this.div.hidden = false;
        } else {
            this.div.hidden = false;
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

