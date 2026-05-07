/*
    RZO - A Business Application Framework

    Copyright (C) 2025 Frank Vanderham

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
         LocalDay, Collection, Cfg, Filter, Query, Row
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import { IPanel, PanelData } from "../panel.js";
import { SplitsBasePanel } from "./splitsbasepanel.js";

export class LogSplitsListPanel extends SplitsBasePanel implements IPanel {
    splitsColl: Cfg<Collection>;
    balanceLogsColl: Cfg<Collection>;
    div: HTMLElement;
    account: Row | null;

    constructor() {
        super();
        this.prefix = "logsplits";
        this.div = this.qElement("-list-div");
        this.splitsColl = new Cfg("splits");
        this.balanceLogsColl = new Cfg("balancelogs");
        this.account = null;
    }

    get id(): string {
        return "logsplits-panel";
    }

    initialize(): void {
        super.initialize();
        this.splitsColl.v = RZO.getCollection(this.splitsColl.name);
        this.balanceLogsColl.v = RZO.getCollection(this.balanceLogsColl.name);
        this.qElement("-start-txt").addEventListener("change", (evt) => {
            this.onPeriodChange(evt);
        });
        this.qButton("-create-btn").addEventListener("click", (evt) => {
            this.onCreate(evt);
        });
    }

    private loadBalanceLog(day: LocalDay, account: Row): void {
        const period = day.getPeriodString();
        const query = new Query(
            [],
            new Filter()
            .op("account_id", "=", account.get("_id"))
            .op("type", "=", "D")
            .op("period", "=", day.getPeriodString(), Filter.AS_IS),
            [
                {field: "posted", order: "desc"}
            ]
        );
        this.balanceLogsColl.v.query(CONTEXT.c, query)
        .then((resultSet) => {
            if (resultSet.next()) {
                this.qElement("-startbal-div").innerText =
                    `Opening balance: ${resultSet.get("balance")}`;
            } else {
                this.qElement("-startbal-div").innerText =
                    `No Opening Balance Found for period ${period}`;
            }
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    private load(account: Row): void {
        this.account = account;
        const id = account.get("_id");
        const startDt = this.qInput("-start-txt").valueAsDate;
        let day: LocalDay;
        if (startDt) {
            day = LocalDay.fromDateInputValueAsDate(startDt);
        } else {
            day = LocalDay.fromDate(new Date());
        }
        this.qElement("-period-div").innerText = day.getPeriodString();
        this.loadDetails(id, account.get("currency"), account.get("lcurrency"));
        this.loadBalanceLog(day, account);
        const query = new Query(
            [],
            new Filter()
            .op("account_id", "=", id)
            .op("posted", ">=", day.utc.toISOString())
            .op("posted", "<", day.next().utc.toISOString()),
            [
                {field: "posted", order: "desc"}
            ]
        );
        this.splitsColl.v.query(CONTEXT.c, query)
        .then((resultSet) => {
            this.renderSplits(resultSet);
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    private onCreate(evt: Event): void {
        if (this.account) {
            this.controller.v.stack(
                "doccreate-panel", new PanelData("Row", this.account));
        }
    }

    private onPeriodChange(evt: Event): void {
        if (this.account) {
            this.load(this.account);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        this.div.hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            const startInput = this.qInput("-start-txt");
            if (!startInput.valueAsDate) {
                startInput.valueAsDate = new Date();
            }
            if (PanelData.typeOf(panelData) == "Row") {
                const row = PanelData.rowOf(panelData);
                this.qElement("-list-account-heading").innerText =
                    `${row.get("name")} - ${row.get("description")}`;
                this.load(row);
            }
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}

