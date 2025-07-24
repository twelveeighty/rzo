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
    accountsMenu: NavMenuItem;

    welcomeHeading: HTMLElement;
    controller: Cfg<PanelController>;
    authenticator: Cfg<IAuthenticator>;
    loggedIn: boolean;
    logger: Logger;

    constructor() {
        this.logger = new Logger("client");
        this.controller = new Cfg("controller");
        const navList = X.ul("nav-list-ul");
        this.myTripsMenu = new NavMenuItem(
            navList, "nav-my-trips-a", "My Trips");
        this.tripsMenu = new NavMenuItem(navList, "nav-trips-a", "Open Trips");
        this.ridersMenu = new NavMenuItem(navList, "nav-riders-a", "Riders");
        this.driversMenu = new NavMenuItem(navList, "nav-drivers-a", "Drivers");
        this.accountsMenu = new NavMenuItem(navList, "nav-accounts-a",
                                            "Accounts");
        this.navLogout = X.a("nav-logout-a");
        this.welcomeHeading = X.heading("welcome-heading");
        this.authenticator = new Cfg("auth");
        this.loggedIn = false;
    }

    private checkLogin(): boolean {
        if (this.loggedIn) {
            return true;
        } else {
            TOASTER.error("You must log in first.");
        }
        return false;
    }

    private onMyTrips(evt: Event): void {
        if (this.checkLogin()) {
            this.controller.v.show("my-trips-panel");
        }
    }

    private onTrips(evt: Event): void {
        if (this.checkLogin()) {
            this.controller.v.show("trips-panel");
        }
    }

    private onRiders(evt: Event): void {
        if (this.checkLogin()) {
            this.controller.v.show("riders-panel");
        }
    }

    private onDrivers(evt: Event): void {
        if (this.checkLogin()) {
            this.controller.v.show("drivers-panel");
        }
    }

    private onAccounts(evt: Event): void {
        if (this.checkLogin()) {
            this.controller.v.show("accounts-panel");
        }
    }

    private onLogout(evt: Event): void {
        if (this.checkLogin()) {
            this.authenticator.v.logout(this.logger, CONTEXT.c)
            .then(() => {
                CONTEXT.reset();
                this.welcomeHeading.innerText = "Drive";
                this.controller.v.broadcast("logged-out");
                this.controller.v.show("login-panel").then(() => {
                    this.myTripsMenu.hide();
                    this.tripsMenu.hide();
                    this.ridersMenu.hide();
                    this.driversMenu.hide();
                    this.accountsMenu.hide();
                });
            })
            .catch((err) => {
                console.error(err);
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    private onLogin(): void {
        this.loggedIn = true;
        const persona = CONTEXT.c.persona.name;
        if (persona == "drivers") {
            this.myTripsMenu.show();
            this.tripsMenu.show();
        }
        if (persona == "planners" || persona == "admins") {
            this.tripsMenu.show();
            this.ridersMenu.show();
            this.driversMenu.show();
            this.accountsMenu.show();
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
        this.accountsMenu.initialize((evt) => {
            evt.preventDefault();
            this.onAccounts(evt);
        });
        this.navLogout.addEventListener("click", (evt) => {
            evt.preventDefault();
            this.onLogout(evt);
        });
    }

    register(controller: PanelController): void {
        this.controller.v = controller;
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

