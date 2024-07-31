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

import { RZO } from "./base/configuration.js";

import { TOASTER } from "./web/toaster.js";
import { PanelController, PanelData } from "./web/panel.js";
import { LoginPanel } from "./web/loginpanel.js";
import { OpenIDPanel } from "./web/openidpanel.js";
import { NavigationPanel } from "./web/navigationpanel.js";

import { RidersListPanel } from "./web/rider/riderslistpanel.js";
import { RiderEditPanel } from "./web/rider/ridereditpanel.js";
import { RiderViewPanel } from "./web/rider/riderviewpanel.js";

import { TripViewPanel } from "./web/trip/tripviewpanel.js";
import { TripAssignPanel } from "./web/trip/tripassignpanel.js";
import { TripEditPanel } from "./web/trip/tripeditpanel.js";
import { TripsListPanel } from "./web/trip/tripslistpanel.js";

import { DriversListPanel } from "./web/driver/driverslistpanel.js";
import { DriverEditPanel } from "./web/driver/drivereditpanel.js";
import { DriverViewPanel } from "./web/driver/driverviewpanel.js";

const metadataName = "./metadata.js";

TOASTER.initialize();

const panelController = new PanelController();

const loginPanel = new LoginPanel();
const openIDPanel = new OpenIDPanel();
const navigationPanel = new NavigationPanel();

const riderEditPanel = new RiderEditPanel();
const ridersListPanel = new RidersListPanel();
const riderViewPanel = new RiderViewPanel();

const tripsListPanel = new TripsListPanel();
const tripViewPanel = new TripViewPanel();
const tripAssignPanel = new TripAssignPanel();
const tripEditPanel = new TripEditPanel();

const driverEditPanel = new DriverEditPanel();
const driversListPanel = new DriversListPanel();
const driverViewPanel = new DriverViewPanel();

// Wire in the dependant panel(s) that require a login before they
// can be initialized
loginPanel.addSubmitInitPanel(riderEditPanel);
loginPanel.addSubmitInitPanel(ridersListPanel);
loginPanel.addSubmitInitPanel(riderViewPanel);

loginPanel.addSubmitInitPanel(tripViewPanel);
loginPanel.addSubmitInitPanel(tripAssignPanel);
loginPanel.addSubmitInitPanel(tripEditPanel);
loginPanel.addSubmitInitPanel(tripsListPanel);

loginPanel.addSubmitInitPanel(driverEditPanel);
loginPanel.addSubmitInitPanel(driversListPanel);
loginPanel.addSubmitInitPanel(driverViewPanel);

// Add panels to the controller
panelController.add(loginPanel);
panelController.add(openIDPanel);
panelController.add(navigationPanel);

panelController.add(ridersListPanel);
panelController.add(riderEditPanel);
panelController.add(riderViewPanel);

panelController.add(tripViewPanel);
panelController.add(tripAssignPanel);
panelController.add(tripEditPanel);
panelController.add(tripsListPanel);

panelController.add(driversListPanel);
panelController.add(driverEditPanel);
panelController.add(driverViewPanel);

function locationHashToMap(hash: string): Map<string, string> {
    const result = new Map();
    if (hash) {
        const params = hash.split("&");
        for (const param in params) {
            const entry = param.split("=");
            const key = entry.length > 0 ? entry[0] : "";
            const value = entry.length > 1 ? entry[1] : "";
            if (key) {
                result.set(key, value);
            }
        }
    }
    return result;
}

function entryPoint(): void {
    const hashMap = locationHashToMap(document.location.hash.slice(1));
    const url = new URL(document.location.href);
    if ("auth" == url.searchParams.get("act") && hashMap.size > 0) {
        if (hashMap.has("error")) {
            const errorDesc = hashMap.has("error_description") ?
                "- " + decodeURIComponent(hashMap.get("error_description")!) :
                "";
            const fullError = `${hashMap.get("error")}${errorDesc}`;
            panelController.show(openIDPanel.id,
                                 new PanelData("string", fullError));
        }
    } else {
        panelController.show(loginPanel.id);
    }
}

import(metadataName)
.then((mod) => {
    const METADATA = mod["METADATA"];
    if (METADATA) {
        RZO.bootstrap(METADATA)
        .then(() => {
            navigationPanel.initialize();
            loginPanel.initialize();
            entryPoint();
        })
        .catch((error) => {
            console.log(error);
        });
        // await RZO.startAsyncTasks();
    } else {
        console.error("METADATA is undefined or null");
    }
})
.catch((error) => {
    console.log("(Main) Caught error");
    console.error(error);
});

