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

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import {
    IPanel, BasePanel, PanelMessage, PanelData
} from "../panel.js";
import { TripList } from "./triplist.js";


export class TripsListPanel extends BasePanel implements IPanel {
    collection: Cfg<Collection>;
    appointmentTsField: Cfg<Field>;

    div: HTMLElement;
    zones: HTMLSelectElement;
    timeframes: HTMLSelectElement;
    leftBtn: HTMLButtonElement;
    rightBtn: HTMLButtonElement;
    refreshBtn: HTMLButtonElement;
    daterangePre: HTMLPreElement;

    tripList: TripList;
    dayOfMonthFormat: Intl.DateTimeFormat;
    monthFormat: Intl.DateTimeFormat;

    startDate: Date;
    endDate: Date | null;

    constructor() {
        super();

        this.collection = new Cfg("collection");
        this.appointmentTsField = new Cfg("appointmentTsField");

        this.div = X.div("trip-list-div");
        this.zones = X.sel("trip-search-zone-sel");
        this.timeframes = X.sel("trip-search-time-sel");
        this.refreshBtn = X.btn("trip-search-refresh-btn");
        this.leftBtn = X.btn("trip-search-left-btn");
        this.rightBtn = X.btn("trip-search-right-btn");
        this.daterangePre = X.pre("trip-search-daterange-pre");

        this.tripList = new TripList(X.div("trip-list-trips-div"), "tpl");

        this.dayOfMonthFormat = new Intl.DateTimeFormat(
            "en", { day: "2-digit", formatMatcher: "basic" });
        this.monthFormat = new Intl.DateTimeFormat(
            "en", { month: "short", formatMatcher: "basic" });
        this.startDate = new Date();
        this.endDate = null;
    }

    get id(): string {
        return "trips-panel";
    }

    async onMessage(message: PanelMessage): Promise<void> {
        if (message == "logged-in") {
            this.loadZones();
        }
    }

    initialize(): void {
        this.collection.v = RZO.getCollection("trips");
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;

        this.zones.addEventListener("change", (evt) => {
            this.queryList(this.startDate, this.endDate);
        });

        this.timeframes.addEventListener("change", (evt) => {
            this.onTimeframeChange(evt);
        });

        this.refreshBtn.addEventListener("click", (evt) => {
            this.onRefresh();
        });

        this.leftBtn.disabled = true;

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

    private shortDates(date1: Date, date2: Date | null): string {
        let result = `${this.dayOfMonthFormat.format(date1)} ` +
               `${this.monthFormat.format(date1)} -`;
        if (date2) {
            result =
                `${result} ${this.dayOfMonthFormat.format(date2)} ` +
               `${this.monthFormat.format(date2)}`;
        }
        return result;
    }

    private loadZones(): void {
        this.service.v.queryCollection(
            this.logger, CONTEXT.c, RZO.getCollection("zones"))
        .then((resultSet) => {
            while (this.zones.options.length > 1) {
                this.zones.remove(1);
            }
            while (resultSet.next()) {
                const opt = document.createElement("option");
                opt.value = resultSet.getString("_id");
                opt.text = resultSet.getString("zone");
                this.zones.add(opt);
            }
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    private shiftTimeWindow(numDays: number): Date[] {
        const startDayOfMonth = this.startDate.getDate();
        let newStartDate = new Date(this.startDate);
        newStartDate.setDate(startDayOfMonth + numDays);
        const now = new Date();
        if (newStartDate.valueOf() < now.valueOf()) {
            newStartDate = now;
            this.leftBtn.disabled = true;
        } else if (this.leftBtn.disabled) {
            this.leftBtn.disabled = false;
        }
        const newEndDate = this.applyTimeframe(newStartDate);
        if (newEndDate) {
            return [newStartDate, newEndDate];
        } else {
            return [];
        }
    }

    private shiftBy(direction: number): void {
        if (this.startDate && this.endDate) {
            const selTimeframe = this.timeframes.value;
            let newDates: Date[] = [];
            switch (selTimeframe) {
                case "ALL":
                    break;
                case "DAY":
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
        switch (this.timeframes.value) {
            case "ALL":
                this.leftBtn.disabled = true;
                this.rightBtn.disabled = true;
                break;
            case "DAY":
            case "WEEK":
                this.leftBtn.disabled = true;
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
        this.queryList(now, endDate);
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as Element;
        if (target && target.id && target.id.length > 4) {
            // console.log(`clicked: ${target.id}`);
            const _id = target.id.slice(4);
            this.controller.v.stack(
                "trip-view-panel", new PanelData("string", _id));
        }
    }

    private applyTimeframe(fromDate: Date): Date | null {
        const selTimeframe = this.timeframes.value;
        fromDate.setHours(0, 0, 0, 0);
        const endDate = new Date(fromDate);
        const startDayOfMonth = fromDate.getDate();
        switch (selTimeframe) {
            case "ALL":
                break;
            case "DAY":
                endDate.setDate(startDayOfMonth + 1);
                endDate.setHours(23, 59, 59);
                return endDate;
            case "WEEK":
                endDate.setDate(startDayOfMonth + 6);
                endDate.setHours(23, 59, 59);
                return endDate;
        }
        return null;
    }

    private queryList(newStartDate: Date, newEndDate: Date | null): void {
        try {
            const filter = new Filter()
                .op("appointmentts", ">=", newStartDate.toISOString());
            const zoneFilter = this.zones.value;
            if (zoneFilter) {
                filter.op("zone_id", "=", zoneFilter);
            }
            if (newEndDate) {
                filter.op("appointmentts", "<=", newEndDate.toISOString());
            }
            filter.isNull("drivernum_id");
            this.startDate = newStartDate;
            this.endDate = newEndDate;
            this.daterangePre.innerText = this.shortDates(
                newStartDate, newEndDate);
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
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            TOASTER.error(`ERROR: ${err}`);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = X.a("nav-trips-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.div.hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            this.onRefresh();
        }
    }

    hide(): void {
        const nav = X.a("nav-trips-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.div.hidden = true;
    }
}

