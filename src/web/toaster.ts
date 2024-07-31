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

import * as X from "./common.js";

export class Toaster {
    private bsToast?: Toast;
    private toasterMsgDiv?: HTMLElement;

    constructor() {
    }

    initialize(): void {
        this.bsToast = Toast.getOrCreateInstance(X.div("toaster-div"));
        this.toasterMsgDiv = X.div("toaster-msg-div");
    }

    info(msg: string): void {
        this.toasterMsgDiv!.innerText = msg;
        this.bsToast!.show();
    }

    error(msg: string): void {
        console.error(msg);
        this.toasterMsgDiv!.innerText = msg;
        this.bsToast!.show();
    }
}

export const TOASTER = new Toaster();

