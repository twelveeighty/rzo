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

import { Collection, Cfg } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import { IPanel, BasePanel, PanelData } from "../panel.js";

export class RidersListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;

    div: HTMLElement;
    listDiv: HTMLElement;
    createBtn: HTMLButtonElement;

    abortController: AbortController | null;

    constructor() {
        super();

        this.div = X.div("riders-div");
        this.listDiv = X.div("riders-list-div");
        this.createBtn = X.btn("rider-create-btn");

        this.collection = new Cfg("collection");

        this.abortController = null;
    }

    get id(): string {
        return "riders-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection("riders");
        this.service.v = RZO.getSource("db").service;

        this.createBtn.addEventListener("click", (evt) => {
            this.createRider(evt);
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
            this.controller.v.show(
                "rider-view-panel", new PanelData("string", _id));
        }
    }

    private queryList(): void {
        try {
            if (this.abortController !== null) {
                this.abortController.abort();
                this.abortController = null;
            }
            this.collection.v.query(CONTEXT.session)
            .then((resultSet) => {
                this.abortController = new AbortController();
                this.listDiv.innerHTML = "";
                while (resultSet.next()) {
                    const anchor = document.createElement("a");
                    anchor.href = "#";
                    anchor.className =
                        "list-group-item list-group-item-action";
                    anchor.id = `ril-${resultSet.getString("_id")}`;

                    anchor.addEventListener("click", (evt) => {
                        evt.preventDefault();
                        this.onAnchorClick(evt);
                    },
                    { signal: this.abortController.signal }
                    );

                    this.listDiv.appendChild(anchor);

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
                    para.innerText = resultSet.getString("ridernum");

                    anchor.appendChild(para);

                    const regionSmall = document.createElement("small");
                    regionSmall.innerText = resultSet.getString("zone");

                    anchor.appendChild(regionSmall);
                }
            })
            .catch((err) => {
                console.error(err);
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            console.error(err);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        this.div.hidden = false;
        this.queryList();
    }

    hide(): void {
        this.div.hidden = true;
    }

}

