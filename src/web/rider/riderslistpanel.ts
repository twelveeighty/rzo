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

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData } from "../panel.js";

import { EntityList } from "../list.js";

export class RidersListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    div: HTMLElement;
    createBtn: HTMLButtonElement;
    list: EntityList;

    constructor() {
        super();
        this.div = X.div("riders-div");
        this.list = new EntityList(
            X.div("riders-list-div"),
            "ril",
            "name",
            "status",
            "ridernum",
            "zone");
        this.createBtn = X.btn("rider-create-btn");
        this.collection = new Cfg("collection");
    }

    get id(): string {
        return "riders-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection("riders");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.createBtn.addEventListener("click", (evt) => {
            this.createRider(evt);
        });
        this.list.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private createRider(evt: Event): void {
        this.controller.v.stack("rider-edit-panel");
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as Element;
        if (target && target.id && target.id.length > 4) {
            // console.log(`clicked: ${target.id}`);
            const _id = target.id.slice(4);
            this.controller.v.stack(
                "rider-view-panel", new PanelData("string", _id));
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
        const nav = X.a("nav-riders-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.div.hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            this.queryList();
        }
    }

    hide(): void {
        const nav = X.a("nav-riders-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.div.hidden = true;
    }

}

