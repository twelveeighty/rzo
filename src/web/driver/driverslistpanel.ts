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

import { Collection, Cfg, ServiceSource } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import { IPanel, BasePanel, PanelData } from "../panel.js";
import { EntityList } from "../list.js";

export class DriversListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    list: EntityList;

    constructor() {
        super();
        this.prefix = "drivers";
        this.list = new EntityList(
            this.qElement("-list-div"),
            "name",
            "status",
            "drivernum",
            "phone1");
        this.collection = new Cfg("collection");
    }

    get id(): string {
        return "drivers-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection("drivers");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.qButton("-create-btn").addEventListener("click", (evt) => {
            this.createDriver(evt);
        });
        this.list.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private createDriver(evt: Event): void {
        this.controller.v.stack("driver-edit-panel");
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.controller.v.stack(
                "driver-view-panel", new PanelData("string", id));
        }
    }

    private queryList(): void {
        this.collection.v.query(CONTEXT.c)
        .then((resultSet) => {
            this.list.render(resultSet);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = this.qElement("nav-drivers-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.qElement("-div").hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            this.queryList();
        }
    }

    hide(): void {
        const nav = this.qElement("nav-drivers-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.qElement("-div").hidden = true;
    }
}

