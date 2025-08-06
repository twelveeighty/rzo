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

import { IResultSet } from "../base/core.js";

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
    idPrefix: string;
    listener: EventListener | null;
    titleField: string;
    shortField: string;
    descField: string;
    minorField: string;

    constructor(parentDiv: HTMLElement, idPrefix: string, titleField: string,
               shortField: string, descField: string, minorField: string) {
        this.parentDiv = parentDiv;
        this.idPrefix = idPrefix;
        this.titleField = titleField;
        this.shortField = shortField;
        this.descField = descField;
        this.minorField = minorField;
        this.listener = null;
        this.abortController = null;
    }

    initialize(listener: EventListener): void {
        this.listener = listener;
    }

    render(resultSet: IResultSet): void {
        if (this.abortController !== null) {
            this.abortController.abort();
        }
        this.parentDiv.innerHTML = "";
        if (this.listener) {
            this.abortController = new AbortController();
        }
        while (resultSet.next()) {
            const anchor = document.createElement("a");
            anchor.href = "#";
            anchor.className =
                "list-group-item list-group-item-action";
            anchor.id = `${this.idPrefix}-${resultSet.getString("_id")}`;
            if (this.listener && this.abortController) {
                anchor.addEventListener(
                    "click",
                    this.listener,
                    { signal: this.abortController.signal });
            }
            this.parentDiv.appendChild(anchor);
            const headingDiv = document.createElement("div");
            headingDiv.className =
                "d-flex w-100 justify-content-between";
            anchor.appendChild(headingDiv);
            const titleElement = document.createElement("h5");
            titleElement.className = "mb-1";
            titleElement.innerText = resultSet.getString(this.titleField);
            const shortElement = document.createElement("small");
            shortElement.innerText = resultSet.getString(this.shortField);
            headingDiv.appendChild(titleElement);
            headingDiv.appendChild(shortElement);
            const descElement = document.createElement("p");
            descElement.className = "mb-1";
            descElement.innerText = resultSet.getString(this.descField);
            anchor.appendChild(descElement);
            const minorElement = document.createElement("small");
            minorElement.innerText = resultSet.getString(this.minorField);
            anchor.appendChild(minorElement);
        }
    }
}


