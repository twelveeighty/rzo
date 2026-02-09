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
    Collection, Cfg, Field, Filter, Query, ServiceSource
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import {
    IPanel, BasePanel, PanelData
} from "../panel.js";

import { TripList } from "./triplist.js";


export class TripsMyListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    appointmentTsField: Cfg<Field>;
    refreshBtn: HTMLButtonElement;
    tripList: TripList;
    dayOfMonthFormat: Intl.DateTimeFormat;
    monthFormat: Intl.DateTimeFormat;
    startDate: Date | null;
    endDate: Date | null;

    constructor() {
        super();
        this.prefix = "trip";
        this.collection = new Cfg("collection");
        this.appointmentTsField = new Cfg("appointmentTsField");
        this.refreshBtn = this.qButton("-my-search-refresh-btn");
        this.tripList = new TripList(this.qElement("-my-list-trips-div"));
        this.dayOfMonthFormat = new Intl.DateTimeFormat(
            "en", { day: "2-digit", formatMatcher: "basic" });
        this.monthFormat = new Intl.DateTimeFormat(
            "en", { month: "short", formatMatcher: "basic" });
        this.startDate = null;
        this.endDate = null;
    }

    get id(): string {
        return "my-trips-panel";
    }

    initialize(): void {
        this.collection.v = RZO.getCollection("trips");
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;

        this.qElement("-my-search-time-sel")
        .addEventListener("change", (evt) => {
            this.onTimeframeChange(evt);
        });

        this.refreshBtn.addEventListener("click", (evt) => {
            this.onRefresh();
        });

        this.tripList.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private shortDates(date1: Date | null, date2: Date | null): string {
        let result = date1 ? `${this.dayOfMonthFormat.format(date1)} ` +
               `${this.monthFormat.format(date1)} -` : "<- ";
        result =
            date2 ? `${result} ${this.dayOfMonthFormat.format(date2)} ` +
           `${this.monthFormat.format(date2)}` : `${result}>`;
        return result;
    }

    private onTimeframeChange(evt: Event): void {
        this.onRefresh();
    }

    private onRefresh(): void {
        this.queryList(this.qSelect("-my-search-time-sel").value);
    }

    private onAnchorClick(evt: Event): void {
        const id = (<HTMLElement>(evt.currentTarget)).dataset["id"];
        if (id) {
            this.controller.v.stack(
                "trip-view-panel", new PanelData("string", id));
        }
    }

    private midnight(): Date {
        const result = new Date();
        result.setHours(0, 0, 0, 0);
        return result;
    }

    private until(daysAfter: number, from: Date,
                  boundary: "start" | "end"): Date {
        const result = new Date(from);
        result.setDate(result.getDate() + daysAfter);
        if (boundary == "start") {
            result.setHours(0, 0, 0, 0);
        } else {
            result.setHours(23, 59, 59, 999);
        }
        return result;
    }

    private async queryList(timeFrame: string): Promise<void> {
        try {
            const todayMidnight = this.midnight();
            switch (timeFrame) {
                case "ALLPAST":
                    this.startDate = null;
                    this.endDate = this.until(-1, todayMidnight, "end");
                    break;
                case "PAST30D":
                    this.startDate = this.until(-30, todayMidnight, "start");
                    this.endDate = this.until(-1, todayMidnight, "end");
                    break;
                case "DAY":
                    this.startDate = todayMidnight;
                    this.endDate = this.until(0, todayMidnight, "end");
                    break;
                case "TWODAYS":
                    this.startDate = todayMidnight;
                    this.endDate = this.until(1, todayMidnight, "end");
                    break;
                case "WEEK":
                    this.startDate = todayMidnight;
                    this.endDate = this.until(6, todayMidnight, "end");
                    break;
                case "ALL":
                    this.startDate = todayMidnight;
                    this.endDate = null;
                    break;
                default:
                    this.startDate = todayMidnight;
                    this.endDate = this.until(0, todayMidnight, "end");
            }
            const filter = new Filter();
            if (this.startDate) {
                filter.op("appointmentts", ">=", this.startDate.toISOString());
            }
            if (this.endDate) {
                filter.op("appointmentts", "<=", this.endDate.toISOString());
            }
            filter.op("drivernum_id", "=", CONTEXT.c.getSubject("driver"));
            this.qElement("-my-search-daterange-pre").innerText =
                this.shortDates(this.startDate, this.endDate);
            const query = new Query(
                [],
                filter,
                [{field: "appointmentts", order: "asc"}]
            );
            this.collection.v.query(CONTEXT.c, query)
            .then((resultSet) => {
                this.tripList.render(resultSet);
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
        } catch (err) {
            TOASTER.exc(err);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = this.qElement("nav-my-trips-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.qElement("-my-list-div").hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            this.onRefresh();
        }
    }

    hide(): void {
        const nav = this.qElement("nav-my-trips-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.qElement("-my-list-div").hidden = true;
    }

}

