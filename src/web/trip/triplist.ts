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
import { DynElement } from "../panel.js";

export class TripList {
    abortController: AbortController | null;
    parentDiv: HTMLElement;
    appointmentTsField: Cfg<Field>;
    listener: EventListener | null;
    dateFormat: Intl.DateTimeFormat;
    timeFormat: Intl.DateTimeFormat;

    constructor(parentDiv: HTMLElement) {
        this.parentDiv = parentDiv;
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
            const appointmentts =
                this.appointmentTsField.v.transform(
                    resultSet.get("appointmentts"));
            const colorCode = this.colorCoding(
                startOfToday, endOfToday, appointmentts, resultSet);
            const returnts = resultSet.get("returnts") ?
                    this.appointmentTsField.v.transform(
                        resultSet.get("returnts")) :
                    null;
            const appointmentDateTime =
                `${this.dateFormat.format(appointmentts)} ` +
                `${this.timeFormat.format(appointmentts)}`;
            const returnTime = returnts ?
                ` - ${this.timeFormat.format(returnts)}` : "";
            const tripType = resultSet.getString("triptype");
            const anchor = new DynElement(
            {
                tag: "a",
                href: "#",
                css: `list-group-item list-group-item-action ${colorCode}`,
                data: {
                    id: resultSet.get("_id")
                }
            })
            .addOptionalListener(
                "click", this.listener, this.abortController
            )
            .append(// headingDiv
                new DynElement(
                {
                    tag: "div",
                    css: "d-flex w-100 justify-content-between"
                })
                .append(// heading5
                    new DynElement(
                    {
                        tag: "h5",
                        css: "mb-1",
                        text: `${resultSet.get("ridername")} - ` +
                                 `${resultSet.get("odescription")}`
                    })
                )
                .append(// statusSmall
                    new DynElement( { tag: "small" })
                    .append(// tripTypeSVG
                        new DynElement(
                        {
                            ns: "http://www.w3.org/2000/svg",
                            tag: "svg"
                        })
                        .addAttribute("class", "bi me-1")
                        .addAttribute("width", "2em")
                        .addAttribute("height", "1em")
                        .addAttribute("role", "img")
                        .addAttribute("aria-label", "TripType")
                        .append(// tripTypeUse
                            new DynElement(
                            {
                                ns: "http://www.w3.org/2000/svg",
                                tag: "use",
                                href: tripType == "ONEWAY" ?
                                    "#oneway-sym" : "#return-sym"
                            })
                        )
                    )
                    .appendTextNode(
                        `  ${tripType} - ${appointmentDateTime}${returnTime}`
                    )
                )
            )
            .append(// para
                new DynElement(
                {
                    tag: "p",
                    css: "mb-1",
                    html: `${resultSet.get("daddress1")}<br/>` +
                             `${resultSet.getString("comments")}`
                })
            )
            .append(// regionSmall
                new DynElement(
                {
                    tag: "small",
                    text: resultSet.get("zone")
                })
            );
            this.parentDiv.appendChild(anchor.asElement());
        }
    }
}

