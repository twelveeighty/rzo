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
    leftBtn: HTMLButtonElement;
    rightBtn: HTMLButtonElement;
    refreshBtn: HTMLButtonElement;
    tripList: TripList;
    dayOfMonthFormat: Intl.DateTimeFormat;
    monthFormat: Intl.DateTimeFormat;
    startDate: Date;
    endDate: Date | null;

    constructor() {
        super();
        this.prefix = "trip";
        this.collection = new Cfg("collection");
        this.appointmentTsField = new Cfg("appointmentTsField");
        this.refreshBtn = this.qButton("-my-search-refresh-btn");
        this.leftBtn = this.qButton("-my-search-left-btn");
        this.rightBtn = this.qButton("-my-search-right-btn");
        this.tripList = new TripList(this.qElement("-my-list-trips-div"));
        this.dayOfMonthFormat = new Intl.DateTimeFormat(
            "en", { day: "2-digit", formatMatcher: "basic" });
        this.monthFormat = new Intl.DateTimeFormat(
            "en", { month: "short", formatMatcher: "basic" });
        this.startDate = new Date();
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

        this.leftBtn.addEventListener("click", (evt) => {
            this.onLeft(evt);
        });

        this.rightBtn.addEventListener("click", (evt) => {
            this.onRight(evt);
        });

        this.tripList.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private shortDates(date1: Date | null, date2: Date | null): string {
        let result = date1 ? `${this.dayOfMonthFormat.format(date1)} ` +
               `${this.monthFormat.format(date1)} -` : "All";
        if (date2) {
            result =
                `${result} ${this.dayOfMonthFormat.format(date2)} ` +
               `${this.monthFormat.format(date2)}`;
        }
        return result;
    }

    private shiftTimeWindow(numDays: number): Date[] {
        const startDayOfMonth = this.startDate.getDate();
        let newStartDate = new Date(this.startDate);
        newStartDate.setDate(startDayOfMonth + numDays);
        const newEndDate = this.applyTimeframe(newStartDate);
        if (newEndDate) {
            return [newStartDate, newEndDate];
        } else {
            return [];
        }
    }

    private shiftBy(direction: number): void {
        const selTimeframe = this.qSelect("-my-search-time-sel").value;
        if (this.endDate && selTimeframe != "ALL") {
            let newDates: Date[] = [];
            switch (selTimeframe) {
                case "DAY":
                    newDates = this.shiftTimeWindow(direction * 1);
                    break;
                case "TWODAYS":
                    newDates = this.shiftTimeWindow(direction * 2);
                    break;
                case "WEEK":
                    newDates = this.shiftTimeWindow(direction * 7);
                    break;
            }
            if (newDates.length == 2) {
                this.queryList(newDates[0], newDates[1]);
            }
        }
    }

    private onTimeframeChange(evt: Event): void {
        switch (this.qSelect("-my-search-time-sel").value) {
            case "ALL":
                this.leftBtn.disabled = true;
                this.rightBtn.disabled = true;
                break;
            case "DAY":
            case "TWODAYS":
            case "WEEK":
                this.leftBtn.disabled = false;
                this.rightBtn.disabled = false;
                break;
        }
        this.onRefresh();
    }

    private onLeft(evt: Event): void {
        this.shiftBy(-1);
    }

    private onRight(evt: Event): void {
        this.shiftBy(1);
    }

    private onRefresh(): void {
        const now = new Date();
        const endDate = this.applyTimeframe(now);
        this.queryList(endDate ? now : null, endDate);
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.controller.v.stack(
                "trip-view-panel", new PanelData("string", id));
        }
    }

    private applyTimeframe(fromDate: Date): Date | null {
        const selTimeframe = this.qSelect("-my-search-time-sel").value;
        if (selTimeframe == "ALL") {
            return null;
        }
        fromDate.setHours(0, 0, 0, 0);
        const endDate = new Date(fromDate);
        const startDayOfMonth = fromDate.getDate();
        switch (selTimeframe) {
            case "DAY":
                endDate.setDate(startDayOfMonth + 1);
                endDate.setHours(23, 23, 23, 23);
                return endDate;
            case "TWODAYS":
                endDate.setDate(startDayOfMonth + 2);
                endDate.setHours(23, 23, 23, 23);
                return endDate;
            case "WEEK":
                endDate.setDate(startDayOfMonth + 6);
                endDate.setHours(23, 23, 23, 23);
                return endDate;
            default:
                return null;
        }
    }

    private queryList(newStartDate: Date | null,
                      newEndDate: Date | null): void {
        try {
            const filter = new Filter();
            if (newStartDate) {
                filter.op("appointmentts", ">=", newStartDate.toISOString());
            }
            if (newEndDate) {
                filter.op("appointmentts", "<=", newEndDate.toISOString());
            }
            filter.op("drivernum_id", "=", CONTEXT.c.getSubject("driver"));
            if (newStartDate) {
                this.startDate = newStartDate;
            }
            this.endDate = newEndDate;
            this.qElement("-my-search-daterange-pre").innerText =
                this.shortDates(newStartDate, newEndDate);
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
                console.error(err);
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            console.error(err);
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

