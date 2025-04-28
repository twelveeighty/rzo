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

    div: HTMLElement;
    createBtn: HTMLButtonElement;
    editBtn: HTMLButtonElement;
    tripList: TripList;

    tableTBody: HTMLTableSectionElement;
    statusElement: HTMLElement;

    state: State | null;

    constructor() {
        super();

        this.div = X.div("rider-view-div");
        this.tableTBody = X.tsec("rider-view-tsec");
        this.statusElement = X.htmlElement("rider-view-status-sm");

        this.tripList = new TripList(X.div("rider-view-trips-div"), "vtl");

        this.createBtn = X.btn("rider-view-create-btn");
        this.editBtn = X.btn("rider-view-edit-btn");

        this.tripEntity = new Cfg("tripEntity");
        this.tripRidernumField = new Cfg("tripRidernumField");
        this.tripsCollection = new Cfg("tripsCollection");

        this.state = null;
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

    private stateToUI(state: State): void {
        this.tableTBody.innerHTML = "";
        X.addRowText(this.tableTBody, "Rider", state.asString("name"));
        X.addRowText(this.tableTBody, "Rider Num", state.asString("ridernum"));
        X.addRowText(this.tableTBody, "Status", state.asString("status"));
        X.addRowText(this.tableTBody, "Zone", state.asString("zone"));
        X.addRowElement(this.tableTBody, "Address",
                    X.addressMapAnchor(state.asString("address1"),
                                      state.asString("maplink")));
        X.addRowText(this.tableTBody, "Address2", state.asString("address2"));
        X.addRowText(this.tableTBody, "City", state.asString("city"));
        X.addRowText(this.tableTBody, "Prov/State", state.asString("stateprov"));
        X.addRowText(this.tableTBody, "Zip", state.asString("postalcode"));

        X.addRowText(this.tableTBody, state.asString("phone1label"),
                     state.asString("phone1"));
        X.addRowText(this.tableTBody, state.asString("phone2label"),
                     state.asString("phone2"));
        X.addRowText(this.tableTBody, state.asString("phone3label"),
                     state.asString("phone3"));
        X.addRowText(this.tableTBody, "Comments", state.asString("comments"),
                           true);
        X.addRowText(this.tableTBody, "Trip Comments",
                     state.asString("tripinfo"), true);

        this.statusElement.innerText = `${state.id} / ${state.rev}`;

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

