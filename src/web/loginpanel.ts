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

import { Cfg, IAuthenticator, Row } from "../base/core.js";
import { RZO, CONTEXT } from "../base/configuration.js";

import * as X from "./common.js";
import { TOASTER } from "./toaster.js";

import { IPanel, BasePanel, PanelData } from "./panel.js";

export class CreateLoginPanel extends BasePanel implements IPanel {
    div: HTMLElement;
    form: HTMLFormElement;
    passwordTxt: HTMLInputElement;
    authenticator: Cfg<IAuthenticator>;

    constructor() {
        super();
        this.authenticator = new Cfg("auth");
        this.div = X.div("create-password-div");
        this.form = X.form("create-password-form");
        this.passwordTxt = X.txt("create-password-txt");
    }

    get id(): string {
        return "create-password-panel";
    }

    initialize(): void {
        super.initialize();

        this.authenticator.v = RZO.getAuthenticator("auth").service;

        this.form.addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
    }

    private onSubmit(evt: Event): void {
        const row = new Row(
            { "password": this.passwordTxt.value });
        this.authenticator.v.createLogin(this.logger, CONTEXT.session, row)
        .then((session) => {
            CONTEXT.reset();
            this.controller.v.show("login-panel");
        })
        .catch((err) => {
            console.error(err);
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        this.passwordTxt.value = "";
        this.div.hidden = false;
    }

    hide(): void {
        this.div.hidden = true;
    }
}

export class OneTimeLoginPanel extends BasePanel implements IPanel {
    div: HTMLElement;
    form: HTMLFormElement;
    userTxt: HTMLInputElement;
    emailTxt: HTMLInputElement;
    codeTxt: HTMLInputElement;
    authenticator: Cfg<IAuthenticator>;

    constructor() {
        super();
        this.authenticator = new Cfg("auth");
        this.div = X.div("onetimelogin-div");
        this.form = X.form("onetimelogin-form");
        this.emailTxt = X.txt("onetimelogin-email-txt");
        this.userTxt = X.txt("onetimelogin-user-txt");
        this.codeTxt = X.txt("onetimelogin-code-txt");
    }

    get id(): string {
        return "onetimelogin-panel";
    }

    initialize(): void {
        super.initialize();
        this.authenticator.v = RZO.getAuthenticator("auth").service;

        this.form.addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
    }

    private onSubmit(evt: Event): void {
        const row = new Row(
            { "username": this.userTxt.value, "code": this.codeTxt.value });
        this.authenticator.v.oneTimeLogin(this.logger, row)
        .then((session) => {
            CONTEXT.session = session;
            this.controller.v.show("create-password-panel");
        })
        .catch((err) => {
            console.error(err);
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            const row = panelData.row;
            this.userTxt.value = row.get("user");
            this.emailTxt.value = row.get("email");
        }
        this.div.hidden = false;
    }

    hide(): void {
        this.div.hidden = true;
    }
}

export class PasswordResetPanel extends BasePanel implements IPanel {
    div: HTMLElement;
    form: HTMLFormElement;
    userTxt: HTMLInputElement;
    authenticator: Cfg<IAuthenticator>;

    constructor() {
        super();
        this.authenticator = new Cfg("auth");
        this.div = X.div("password-reset-div");
        this.form = X.form("password-reset-form");
        this.userTxt = X.txt("password-reset-user-txt");
    }

    get id(): string {
        return "password-reset-panel";
    }

    initialize(): void {
        super.initialize();
        this.authenticator.setIf(
            "Authenticator ", RZO.getAuthenticator("auth").service);

        this.form.addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
    }

    private onSubmit(evt: Event): void {
        const row = new Row({ "user": this.userTxt.value });
        this.authenticator.v.resetAuthentication(this.logger, row)
        .then((resultRow) => {
            this.controller.v.show(
                "onetimelogin-panel", new PanelData("Row", resultRow));
        })
        .catch((err) => {
            console.error(err);
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        this.userTxt.value = "";
        this.div.hidden = false;
    }

    hide(): void {
        this.div.hidden = true;
    }
}

export class LoginPanel extends BasePanel implements IPanel {
    onSubmitInitPanels: IPanel[];

    form: HTMLFormElement;
    welcomeHeading: HTMLElement;
    loginDiv: HTMLElement;
    userTxt: HTMLInputElement;
    passwordTxt: HTMLInputElement;
    personOpenPath: SVGPathElement;
    personFilledPath: SVGPathElement;
    personCheckPath: SVGPathElement;
    passwordResetBtn: HTMLButtonElement;
    authenticator: Cfg<IAuthenticator>;

    constructor() {
        super();

        this.form = X.form("login-form");
        this.welcomeHeading = X.heading("welcome-heading");
        this.loginDiv = X.div("login-div");
        this.userTxt = X.txt("login-user-txt");
        this.passwordTxt = X.txt("login-password-txt");
        this.personOpenPath = X.path("person-open-path");
        this.personFilledPath = X.path("person-filled-path");
        this.personCheckPath = X.path("person-check-path");
        this.passwordResetBtn = X.btn("login-reset-btn");

        this.onSubmitInitPanels = [];
        this.authenticator = new Cfg("auth");
    }

    get id(): string {
        return "login-panel";
    }

    addSubmitInitPanel(panel: IPanel) {
        this.onSubmitInitPanels.push(panel);
    }

    initialize(): void {
        super.initialize();
        this.authenticator.v = RZO.getAuthenticator("auth").service;

        this.form.addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
        this.welcomeHeading.addEventListener("click", (evt) => {
            evt.preventDefault();
        });
        this.passwordResetBtn.addEventListener("click", (evt) => {
            this.onResetPassword(evt);
        });
    }

    private onResetPassword(evt: Event): void {
        this.controller.v.show("password-reset-panel");
    }

    private onSubmit(evt: Event): void {
        try {
            const targetUsername = this.userTxt.value;
            const targetPassword = this.passwordTxt.value;
            if (!targetUsername || !targetPassword) {
                TOASTER.error("You must enter a valid User and Password");
                throw new Error("No User or Password entered");
            }
            const credsRow = new Row(
                { "username": targetUsername, "password": targetPassword });
            this.authenticator.v.login(this.logger, credsRow)
            .then((session) => {
                CONTEXT.session = session;
                this.welcomeHeading.innerText = targetUsername;
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
        } catch (err) {
            console.error(err);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        this.userTxt.value = "";
        this.passwordTxt.value = "";
        this.loginDiv.hidden = false;
    }

    hide(): void {
        this.loginDiv.hidden = true;
    }
}

