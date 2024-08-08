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

import { Cfg, IAuthenticator } from "../base/core.js";

import { RZO, CONTEXT } from "../base/configuration.js";

import * as X from "./common.js";

import { TOASTER } from "./toaster.js";

import { PanelController, IPanel, PanelData, PanelMessage } from "./panel.js";

export class NavigationPanel implements IPanel {
    navTrips: HTMLAnchorElement;
    navRiders: HTMLAnchorElement;
    navDrivers: HTMLAnchorElement;
    navLogout: HTMLAnchorElement;
    controller?: PanelController;
    authenticator: Cfg<IAuthenticator>;
    loggedIn: boolean;

    constructor() {
        this.navTrips = X.a("nav-trips-a");
        this.navRiders = X.a("nav-riders-a");
        this.navDrivers = X.a("nav-drivers-a");
        this.navLogout = X.a("nav-logout-a");
        this.authenticator = new Cfg("auth");
        this.loggedIn = false;
    }

    private onTrips(evt: Event): void {
        if (this.loggedIn) {
            this.controller!.show("trips-panel");
        } else {
            TOASTER.error("You must log in first.");
        }
    }

    private onRiders(evt: Event): void {
        if (this.loggedIn) {
            this.controller!.show("riders-panel");
        } else {
            TOASTER.error("You must log in first.");
        }
    }

    private onDrivers(evt: Event): void {
        if (this.loggedIn) {
            this.controller!.show("drivers-panel");
        } else {
            TOASTER.error("You must log in first.");
        }
    }

    private onLogout(evt: Event): void {
        if (this.loggedIn) {
            this.authenticator.v.logout(CONTEXT.session)
            .then(() => {
                CONTEXT.reset();
                this.controller!.broadcast("logged-out");
                this.controller!.show("login-panel");
            })
            .catch((err) => {
                console.error(err);
                TOASTER.error(`ERROR: ${err}`);
            });
        } else {
            TOASTER.error("You must log in first.");
        }
    }

    get id(): string {
        return "nav-panel";
    }

    initialize(): void {
        this.authenticator.v = RZO.getAuthenticator("auth").service;

        this.navTrips.addEventListener("click", (evt) => {
            evt.preventDefault();
            this.onTrips(evt);
        });
        this.navRiders.addEventListener("click", (evt) => {
            evt.preventDefault();
            this.onRiders(evt);
        });
        this.navDrivers.addEventListener("click", (evt) => {
            evt.preventDefault();
            this.onDrivers(evt);
        });
        this.navLogout.addEventListener("click", (evt) => {
            evt.preventDefault();
            this.onLogout(evt);
        });
    }

    register(controller: PanelController): void {
        this.controller = controller;
    }

    async show(panelData?: PanelData): Promise<void> {
        // no-op
    }

    canHide(): boolean {
        return true;
    }

    hide(): void {
        // no-op
    }

    async onMessage(message: PanelMessage): Promise<void> {
        if (message == "logged-in") {
            this.loggedIn = true;
        } else if (message == "logged-out") {
            this.loggedIn = false;
        }
    }
}

