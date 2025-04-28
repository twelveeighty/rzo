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
import * as X from "./common.js";

export class Toaster {
    private bsToast: Cfg<Toast>;
    private toasterMsgDiv: Cfg<HTMLElement>;
    private toasterTimestamp: Cfg<HTMLElement>;

    constructor() {
        this.bsToast = new Cfg("toaster-div");
        this.toasterMsgDiv = new Cfg("toaster-msg-div");
        this.toasterTimestamp = new Cfg("toaster-timestamp-sm");
    }

    initialize(): void {
        this.bsToast.v = Toast.getOrCreateInstance(X.div(this.bsToast.name));
        this.toasterMsgDiv.v = X.div(this.toasterMsgDiv.name);
        this.toasterTimestamp.v = X.htmlElement(this.toasterTimestamp.name);
    }

    private timestamp(): void {
        this.toasterTimestamp.v.innerText = (new Date()).toLocaleTimeString();
    }

    info(msg: string): void {
        this.timestamp();
        this.toasterMsgDiv.v.innerText = msg;
        this.bsToast.v.show();
    }

    error(msg: string): void {
        this.timestamp();
        console.error(msg);
        this.toasterMsgDiv.v.innerText = msg;
        this.bsToast.v.show();
    }
}

export const TOASTER = new Toaster();

