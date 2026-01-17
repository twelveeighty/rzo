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

import { Toast } from "bootstrap";
import { Cfg } from "../base/core.js";

export class Toaster {
    private bsToast: Cfg<Toast>;

    constructor() {
        this.bsToast = new Cfg("toaster-div");
    }

    qElement(id: string): HTMLElement {
        const element = document.getElementById(id);
        if (element) {
            return element;
        } else {
            throw new Error(`No such element: ${id}`);
        }
    }

    initialize(): void {
        this.bsToast.v = Toast.getOrCreateInstance(
            this.qElement(this.bsToast.name));
    }

    private setTimestamp(): void {
        this.qElement("toaster-timestamp-sm").innerText =
            (new Date()).toLocaleTimeString();
    }

    private setMsg(msg: string): void {
        this.qElement("toaster-msg-div").innerText = msg;
    }

    info(msg: string): void {
        this.setTimestamp();
        this.setMsg(msg);
        this.bsToast.v.show();
    }

    error(msg: string): void {
        this.setTimestamp();
        console.error(msg);
        this.setMsg(msg);
        this.bsToast.v.show();
    }

    exc(err: any): void {
        console.error(err);
        this.error(`ERROR: ${err}`);
    }
}

export const TOASTER = new Toaster();

