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

import { IResultSet, Field, Cfg } from "../../base/core.js";
import { RZO } from "../../base/configuration.js";

import { AttributeJoiner } from "../panel.js";


export class TripList {
    abortController: AbortController | null;
    parentDiv: HTMLElement;
    listIdPrefix: string;
    appointmentTsField: Cfg<Field>;
    listener: EventListener | null;
    dateFormat: Intl.DateTimeFormat;
    timeFormat: Intl.DateTimeFormat;

    constructor(parentDiv: HTMLElement, listIdPrefix: string) {
        this.parentDiv = parentDiv;
        this.listIdPrefix = listIdPrefix;
        this.abortController = null;
        this.listener = null;
        this.appointmentTsField = new Cfg("appointmentTsField");
        this.dateFormat = new Intl.DateTimeFormat(
            "en",
            { hour12: true, hourCycle: "h12", weekday: "short", month: "short",
              day: "2-digit", formatMatcher: "basic" }
        );
        this.timeFormat = new Intl.DateTimeFormat(
            "en",
            { hour12: true, hourCycle: "h12", hour: "numeric",
              minute: "2-digit", formatMatcher: "basic" }
        );
    }

    initialize(listener: EventListener): void {
        this.appointmentTsField.v = RZO.getField("trip.appointmentts");
        this.listener = listener;
    }

    colorCoding(startOfToday: Date, endOfToday: Date, appointmentts: Date,
                resultSet: IResultSet): string {
        let result: string = "ist-group-item-light";
        const assigned = !resultSet.isNull("drivernum_id");
        if (assigned) {
            if (appointmentts > startOfToday) {
                return "list-group-item-primary";
            } else {
                return "list-group-item-success";
            }
        } else {
            if (appointmentts > startOfToday && appointmentts < endOfToday) {
                return "list-group-item-warning";
            } else if (appointmentts < startOfToday) {
                return "list-group-item-danger";
            }
        }
        return result;
    }

    render(resultSet: IResultSet): void {
        if (this.abortController != null) {
            this.abortController.abort();
        }
        this.abortController = new AbortController();
        this.parentDiv.innerHTML = "";
        const startOfToday = new Date();
        startOfToday.setHours(0, 0, 0, 0);
        const endOfToday = new Date();
        endOfToday.setHours(23, 59, 59);
        while (resultSet.next()) {
            const anchor = document.createElement("a");
            anchor.href = "#";
            const appointmentts =
                this.appointmentTsField.v.transform(
                    resultSet.get("appointmentts"));
            const colorCode = this.colorCoding(
                startOfToday, endOfToday, appointmentts, resultSet);
            anchor.className =
                `list-group-item list-group-item-action ${colorCode}`;
            anchor.id = `${this.listIdPrefix}-${resultSet.getString("_id")}`;

            anchor.addEventListener(
                "click",
                this.listener!,
                { signal: this.abortController.signal }
            );

            this.parentDiv.appendChild(anchor);

            const headingDiv = document.createElement("div");
            headingDiv.className =
                "d-flex w-100 justify-content-between";

            anchor.appendChild(headingDiv);

            const heading5 = document.createElement("h5");
            heading5.className = "mb-1";
            heading5.innerText =
                `${resultSet.getString("ridername")} - ` +
                `${resultSet.getString("description")}`;

            const returnts = resultSet.get("returnts") ?
                    this.appointmentTsField.v.transform(
                        resultSet.get("returnts")) :
                    null;

            const appointmentDateTime =
                `${this.dateFormat.format(appointmentts)} ` +
                `${this.timeFormat.format(appointmentts)}`;
            const returnTime = returnts ?
                ` - ${this.timeFormat.format(returnts)}` : "";

            const statusSmall = document.createElement("small");

            const tripTypeSVG = document.createElementNS(
                "http://www.w3.org/2000/svg", "svg");
            tripTypeSVG.setAttribute("class", "bi me-1");
            tripTypeSVG.setAttribute("width", "2em");
            tripTypeSVG.setAttribute("height", "1em");
            tripTypeSVG.setAttribute("role", "img");
            tripTypeSVG.setAttribute("aria-label", "TripType");

            const tripTypeUse = document.createElementNS(
                "http://www.w3.org/2000/svg", "use");
            const tripType = resultSet.getString("triptype");
            if (tripType == "ONEWAY") {
                tripTypeUse.setAttribute("href", "#oneway-sym");
            } else {
                tripTypeUse.setAttribute("href", "#return-sym");
            }
            tripTypeSVG.appendChild(tripTypeUse);
            statusSmall.appendChild(tripTypeSVG);
            statusSmall.appendChild(document.createTextNode(
                `  ${tripType} - ` +
                `${appointmentDateTime}${returnTime}`
            ));

            headingDiv.appendChild(heading5);
            headingDiv.appendChild(statusSmall);

            const para = document.createElement("p");
            para.className = "mb-1";
            para.innerText = new AttributeJoiner().
                add("", resultSet.getString("daddress1")).
                add("", resultSet.getString("comments")).
                toText();

            anchor.appendChild(para);

            const regionSmall = document.createElement("small");
            regionSmall.innerText = resultSet.getString("zone");

            anchor.appendChild(regionSmall);
        }
    }
}

