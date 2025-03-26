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

import { Cfg, IAuthenticator, Logger } from "../base/core.js";

import { RZO, CONTEXT } from "../base/configuration.js";

import * as X from "./common.js";

import { TOASTER } from "./toaster.js";

import {
    PanelController, IPanel, PanelData, PanelMessage, NavMenuItem
} from "./panel.js";

export class NavigationPanel implements IPanel {
    navLogout: HTMLAnchorElement;

    myTripsMenu: NavMenuItem;
    tripsMenu: NavMenuItem;
    ridersMenu: NavMenuItem;
    driversMenu: NavMenuItem;

    welcomeHeading: HTMLElement;
    controller?: PanelController;
    authenticator: Cfg<IAuthenticator>;
    loggedIn: boolean;
    logger: Logger;

    constructor() {
        this.logger = new Logger("client");
        const navList = X.ul("nav-list-ul");
        this.myTripsMenu = new NavMenuItem(
            navList, "nav-my-trips-a", "My Trips");
        this.tripsMenu = new NavMenuItem(navList, "nav-trips-a", "Open Trips");
        this.ridersMenu = new NavMenuItem(navList, "nav-riders-a", "Riders");
        this.driversMenu = new NavMenuItem(
            navList, "nav-drivers-a", "Drivers");
        this.navLogout = X.a("nav-logout-a");
        this.welcomeHeading = X.heading("welcome-heading");
        this.authenticator = new Cfg("auth");
        this.loggedIn = false;
    }

    private onMyTrips(evt: Event): void {
        if (this.loggedIn) {
            this.controller!.show("my-trips-panel");
        } else {
            TOASTER.error("You must log in first.");
        }
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
            this.authenticator.v.logout(this.logger, CONTEXT.session)
            .then(() => {
                CONTEXT.reset();
                this.myTripsMenu.hide();
                this.tripsMenu.hide();
                this.ridersMenu.hide();
                this.driversMenu.hide();
                this.welcomeHeading.innerText = "Drive";
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

    private onLogin(): void {
        this.loggedIn = true;
        const persona = CONTEXT.session.persona.name;
        if (persona == "drivers") {
            this.myTripsMenu.show();
            this.tripsMenu.show();
        }
        if (persona == "planners" || persona == "admins") {
            this.tripsMenu.show();
            this.ridersMenu.show();
            this.driversMenu.show();
        }
    }

    get id(): string {
        return "nav-panel";
    }

    initialize(): void {
        this.logger.configure(RZO);
        this.authenticator.v = RZO.getAuthenticator("auth").service;

        this.myTripsMenu.initialize((evt) => {
            evt.preventDefault();
            this.onMyTrips(evt);
        });
        this.tripsMenu.initialize((evt) => {
            evt.preventDefault();
            this.onTrips(evt);
        });
        this.ridersMenu.initialize((evt) => {
            evt.preventDefault();
            this.onRiders(evt);
        });
        this.driversMenu.initialize((evt) => {
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
            this.onLogin();
        } else if (message == "logged-out") {
            this.loggedIn = false;
        }
    }
}

