/*
    RZO - A Business Application Framework

    Copyright (C) 2026 Frank Vanderham

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
         Collection, Cfg, ServiceSource, Entity, SideEffects
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { IReplicableService } from "../../mobile/pouch-client.js";
import { TOASTER } from "../toaster.js";
import { IPanel, BasePanel, PanelData } from "../panel.js";
import { EntityList } from "../list.js";

export class TestsListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    list: EntityList;

    constructor() {
        super();
        this.prefix = "tests";
        this.list = new EntityList(
            this.qElement("-list-div"),
            "keyfield",
            "uniquefield",
            "stringfield",
            "integerfield");
        this.collection = new Cfg("testversioneds");
    }

    get id(): string {
        return "tests-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection(this.collection.name);
        this.service.v =
            (<ServiceSource>RZO.getSource("pouchdb")
             .ensure(ServiceSource)).service;
        this.entity.v = RZO.getEntity("testversioned");
        this.qButton("-create-btn").addEventListener("click", (evt) => {
            this.createVersioned(evt);
        });
        this.qButton("-replicate-btn").addEventListener("click", (evt) => {
            this.replicate(evt);
        });
        this.list.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private async replicate(evt: Event): Promise<void> {
        if ((<any>(this.service.v)).isReplicableService) {
            const replService = this.service.v as unknown as IReplicableService;
            await replService.replicate(CONTEXT.c, this.entity.v);
            this.queryList();
        } else {
            TOASTER.error(`Service ${this.service.name} is not Replicable`);
        }
    }

    private async createVersioned(evt: Event): Promise<void> {
        const entity = this.entity.v;
        try {
            const state = await entity.create(CONTEXT.c, this.service.v);
            const se: Promise<SideEffects>[] = [];
            entity.cpSetValue(
                state, "keyfield", "TV710", CONTEXT.c, se);
            entity.cpSetValue(
                state, "stringfield", "stringfield1", CONTEXT.c, se);
            entity.cpSetValue(
                state, "uniquefield", "TV701Unique", CONTEXT.c, se);
            entity.cpSetValue(
                state, "integerfield", "2048", CONTEXT.c, se);
            entity.cpSetValue(
                state, "aliasvaluelist", "GREEN", CONTEXT.c, se);
            entity.cpSetValue(
                state, "numberfield", 42.02, CONTEXT.c, se);
            entity.cpSetValue(
                state, "amountfield", "192.34", CONTEXT.c, se);
            entity.cpSetValue(
                state, "booleanfield", true, CONTEXT.c, se);
            entity.cpSetValue(
                state, "datefield", new Date(), CONTEXT.c, se);
            entity.cpSetValue(
                state, "datetimefield", new Date(), CONTEXT.c, se);
            entity.cpSetValue(
                state, "uuidfield", Entity.generateId(), CONTEXT.c, se);
            entity.cpSetValue(
                state, "pathfield", "TV710.001", CONTEXT.c, se);
            await Promise.all(se);
            await this.entity.v.post(this.service.v, state, CONTEXT.c);
            this.queryList();
        } catch (err) {
            TOASTER.exc(err);
        }
        //this.controller.v.stack("versioned-edit-panel");
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.controller.v.stack(
                "test-view-panel", new PanelData("string", id));
        }
    }

    private queryList(): void {
        this.collection.v.query(CONTEXT.c)
        .then((rs) => {
            this.list.render(rs);
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = this.qElement("nav-tests-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.qElement("-div").hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            this.queryList();
        }
    }

    hide(): void {
        const nav = this.qElement("nav-tests-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.qElement("-div").hidden = true;
    }
}

