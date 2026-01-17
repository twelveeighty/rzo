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
    Entity, State, Filter, Query, Collection, Cfg,
    ServiceSource
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import { IPanel, ViewPanel, PanelData, DynElement } from "../panel.js";
import { TripList } from "../trip/triplist.js";


export class DriverViewPanel extends ViewPanel implements IPanel {
    tripEntity: Cfg<Entity>;
    tripsCollection: Cfg<Collection>;
    tripList: TripList;

    constructor() {
        super("driver", "-view-div", "-view-tsec", "-view-back-btn",
              "-view-edit-btn", "driver-edit-panel");
        this.tripList = new TripList(this.qElement("-view-trips-div"));
        this.tripEntity = new Cfg("tripEntity");
        this.tripsCollection = new Cfg("tripsCollection");
    }

    get id(): string {
        return "driver-view-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("driver");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.tripEntity.v = RZO.getEntity("trip");
        this.tripsCollection.v = RZO.getCollection("trips");

        this.tripList.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.controller.v.stack(
                "trip-view-panel", new PanelData("string", id));
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
                    "drivernum_id", "=", this.state.id),
                [ { field: "appointmentts", order: "desc" } ]
            );
            this.tripsCollection.v.query(CONTEXT.c, query)
            .then((resultSet) => {
                this.tripList.render(resultSet);
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            TOASTER.error(`ERROR: ${err}`);
        }
    }

    protected stateToUI(state: State): void {
        this.tbody.innerHTML = "";
        this.addRowText("Driver", state.asString("name"));
        this.addRowText("Driver Num",
                     state.asString("drivernum"));
        this.addRowText("Status", state.asString("status"));
        this.addTableRowElement(
            this.tbody, "Address",
            this.addressMapAnchor(state.asString("address1"),
                                  state.asString("maplink")));
        this.addRowText("Address2", state.asString("address2"));
        this.addRowText("City", state.asString("city"));
        this.addRowText("Prov/State", state.asString("stateprov"));
        this.addRowText("Zip", state.asString("postalcode"));
        this.addRowText(state.asString("phone1label"),
                        state.asString("phone1"));
        this.addRowText(state.asString("phone2label"),
                        state.asString("phone2"));
        this.addRowText(state.asString("phone3label"),
                        state.asString("phone3"));
        this.addTableRowElement(this.tbody, "DB Id",
            new DynElement({ tag: "samp", text: state.id }).asElement());
        this.addTableRowElement(this.tbody, "DB Version",
            new DynElement({ tag: "samp", text: state.rev }).asElement());
        this.queryTrips();
    }
}

