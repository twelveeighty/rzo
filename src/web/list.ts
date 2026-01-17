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

import { IResultSet, Row } from "../base/core.js";
import { DynElement } from "./panel.js";

export class EntityList {
    /*
     *    |----------------------------------------------------------------|
     *    |   TITLE                                                SHORT   |
     *    |   Description                                                  |
     *    |   Minor                                                        |
     *    |----------------------------------------------------------------|
     */
    abortController: AbortController | null;
    parentDiv: HTMLElement;
    listener: EventListener | null;
    titleField: string;
    shortField: string;
    descField: string;
    minorField: string;
    dataFields: string[];

    constructor(parentDiv: HTMLElement, titleField: string, shortField: string,
                descField: string, minorField: string, dataFields?: string[]) {
        this.parentDiv = parentDiv;
        this.titleField = titleField;
        this.shortField = shortField;
        this.descField = descField;
        this.minorField = minorField;
        this.listener = null;
        this.abortController = null;
        this.dataFields = dataFields || [];
    }

    initialize(listener: EventListener): void {
        this.listener = listener;
    }

    render(rs: IResultSet): void {
        if (this.abortController !== null) {
            this.abortController.abort();
        }
        this.parentDiv.innerHTML = "";
        if (this.listener) {
            this.abortController = new AbortController();
        } else {
            this.abortController = null;
        }
        while (rs.next()) {
            const dataRow = new Row ( { id: rs.get("_id") } );
            for (const field of this.dataFields) {
                dataRow.add(field, rs.get(field));
            }
            const anchor = new DynElement(
            {
                tag: "a",
                href: "#",
                css: "list-group-item list-group-item-action",
                data: dataRow.raw()
            })
            .append(// headingDiv
              new DynElement(
                  {
                      tag: "div",
                      css: "d-flex w-100 justify-content-between"
                  })
                  .append(// titleElement
                      new DynElement(
                      {
                          tag: "h5",
                          css: "mb-1",
                          text: rs.get(this.titleField)
                      })
                  )
                  .append(// shortElement
                      new DynElement(
                      {
                          tag: "small",
                          text: rs.get(this.shortField)
                      })
                  )
            )
            .append(// descElement
              new DynElement(
                  {
                      tag: "p",
                      css: "mb-1",
                      text: rs.get(this.descField)
                  })
            )
            .append(// minorElement
              new DynElement(
                  {
                      tag: "small",
                      text: rs.get(this.minorField)
                  })
            );
            anchor.addOptionalListener(
                "click", this.listener, this.abortController);
            this.parentDiv.appendChild(anchor.asElement());
        }
    }
}


