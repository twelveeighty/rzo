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

// import { Filter, Query } from "../base/core.js";
import { RZO, CONTEXT } from "../base/configuration.js";

import * as X from "./common.js";
import { TOASTER } from "./toaster.js";

import { IPanel, BasePanel, PanelData } from "./panel.js";

export class LoginPanel extends BasePanel implements IPanel {
    onSubmitInitPanels: IPanel[];

    form: HTMLFormElement;
    welcomeHeading: HTMLElement;
    loginDiv: HTMLElement;
    userTxt: HTMLInputElement;
    personaSel: HTMLSelectElement;
    personOpenPath: SVGPathElement;
    personFilledPath: SVGPathElement;
    personCheckPath: SVGPathElement;

    constructor() {
        super();

        this.form = X.form("login-form");
        this.welcomeHeading = X.heading("welcome-heading");
        this.loginDiv = X.div("login-div");
        this.userTxt = X.txt("user-txt");
        this.personaSel = X.sel("persona-sel");
        this.personOpenPath = X.path("person-open-path");
        this.personFilledPath = X.path("person-filled-path");
        this.personCheckPath = X.path("person-check-path");

        this.onSubmitInitPanels = [];
    }

    get id(): string {
        return "login-panel";
    }

    addSubmitInitPanel(panel: IPanel) {
        this.onSubmitInitPanels.push(panel);
    }

    initialize(): void {
        this.entity.setIf("Entity ", RZO.getEntity("useraccount"));
        this.service.setIf("Service ", RZO.getSource("db").service);

        this.form.addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
        this.welcomeHeading.addEventListener("click", (evt) => {
            evt.preventDefault();
        });

        for (const personaKey of RZO.personas.keys()) {
            const opt = document.createElement("option");
            opt.value = personaKey;
            opt.text = personaKey;
            this.personaSel.add(opt);
        }
    }

    private onSubmit(evt: Event): void {
        try {
            /*
            const targetPersona = this.personaSel.value;
            if (!targetPersona) {
                TOASTER.error("You must select a valid Persona");
                throw new Error("No persona selected");
            }
            const persona = RZO.personas.get(targetPersona);
            if (!persona) {
                TOASTER.error(`Cannot find persona: ${targetPersona}`);
                throw new Error(`Cannot find persona: ${targetPersona}`);
            }
            CONTEXT.persona = persona;
            */
            const targetUserId = this.userTxt.value;
            if (!targetUserId) {
                TOASTER.error("You must enter a valid User");
                throw new Error("No User entered");
            }
            this.service.v.createSession(targetUserId)
            .then((session) => {
                CONTEXT.userAccountId = targetUserId;
                CONTEXT.persona = RZO.getPersona(session.get("persona"));
                CONTEXT.sessionId = session.get("_id");
                this.welcomeHeading.innerText =
                    `${session.get("useraccountnum")}`;
                // Initialize panels that required a login
                for (const panel of this.onSubmitInitPanels) {
                    panel.initialize();
                }

                // Broadcast the "logged-in" message
                this.controller.v.broadcast("logged-in");

                // Switch the icon on the nav bar
                this.personOpenPath.classList.toggle("invisible");
                this.personFilledPath.classList.toggle("invisible");
                this.personCheckPath.classList.toggle("invisible");

                // Switch to the main 'Trips' panel
                this.controller.v.show("trips-panel");
            })
            .catch((err) => {
                console.error(err);
                TOASTER.error(`ERROR: ${err}`);
            });
            /*
            const filter = new Filter().
                op("useraccountnum", "=", targetUserId);
            const query = new Query([], filter);
            this.service.v.getQuery(this.entity.v, query)
            .then((resultSet) => {
                if (resultSet.next()) {
                    CONTEXT.userAccountId = resultSet.getString("_id");
                    this.welcomeHeading.innerText =
                        `${resultSet.getString("name")}`;

                    // Initialize panels that required a login
                    for (const panel of this.onSubmitInitPanels) {
                        panel.initialize();
                    }

                    // Broadcast the "logged-in" message
                    this.controller.v.broadcast("logged-in");

                    // Switch the icon on the nav bar
                    this.personOpenPath.classList.toggle("invisible");
                    this.personFilledPath.classList.toggle("invisible");
                    this.personCheckPath.classList.toggle("invisible");

                    // Switch to the main 'Trips' panel
                    this.controller.v.show("trips-panel");
                } else {
                    TOASTER.error(`Cannot log in: ${targetUserId}`);
                }
            })
            .catch((err) => {
                console.error(err);
                TOASTER.error(`ERROR: ${err}`);
            });
            */
        } catch (err) {
            console.error(err);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        this.userTxt.value = "";
        this.personaSel.value = "";
        this.loginDiv.hidden = false;
    }

    hide(): void {
        this.loginDiv.hidden = true;
    }
}

