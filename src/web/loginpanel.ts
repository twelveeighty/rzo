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

import { Cfg, IAuthenticator, IPolicyAuthorizer, Row } from "../base/core.js";
import { RZO, CONTEXT } from "../base/configuration.js";
import { TOASTER } from "./toaster.js";
import { IPanel, BasePanel, PanelData } from "./panel.js";

export class CreateLoginPanel extends BasePanel implements IPanel {
    authenticator: Cfg<IAuthenticator>;

    constructor() {
        super();
        this.prefix = "create-password";
        this.authenticator = new Cfg("auth");
    }

    get id(): string {
        return "create-password-panel";
    }

    initialize(): void {
        super.initialize();
        this.authenticator.v = RZO.getAuthenticator("auth").service;
        this.qForm("-form").addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
    }

    private onSubmit(evt: Event): void {
        const row = new Row({ "password": this.qInput("-txt").value });
        this.authenticator.v.createLogin(this.logger, CONTEXT.c, row)
        .then(() => {
            CONTEXT.reset();
            this.controller.v.show("login-panel");
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        this.qInput("-txt").value = "";
        this.qElement("-div").hidden = false;
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

export class OneTimeLoginPanel extends BasePanel implements IPanel {
    authenticator: Cfg<IAuthenticator>;

    constructor() {
        super();
        this.prefix = "onetimelogin";
        this.authenticator = new Cfg("auth");
    }

    get id(): string {
        return "onetimelogin-panel";
    }

    initialize(): void {
        super.initialize();
        this.authenticator.v = RZO.getAuthenticator("auth").service;
        this.qForm("-form").addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
    }

    private onSubmit(evt: Event): void {
        const row = new Row(
            {
                "username": this.qInput("-user-txt").value,
                "code": this.qInput("-code-txt").value
            }
        );
        this.authenticator.v.oneTimeLogin(this.logger, row)
        .then((context) => {
            CONTEXT.c = context;
            this.controller.v.show("create-password-panel");
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            const row = PanelData.rowOf(panelData);
            this.qInput("-user-txt").value = row.get("user");
            this.qInput("-email-txt").value = row.get("email");
        }
        this.qElement("-div").hidden = false;
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

export class PasswordResetPanel extends BasePanel implements IPanel {
    authenticator: Cfg<IAuthenticator>;

    constructor() {
        super();
        this.prefix = "password-reset";
        this.authenticator = new Cfg("auth");
    }

    get id(): string {
        return "password-reset-panel";
    }

    initialize(): void {
        super.initialize();
        this.authenticator.setIf(
            "Authenticator ", RZO.getAuthenticator("auth").service);
        this.qForm("-form").addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
        this.qButton("-back-btn").addEventListener("click", (evt) => {
            this.onBack(evt);
        });
    }

    private onBack(evt: Event): void {
        this.controller.v.show("login-panel");
    }

    private onSubmit(evt: Event): void {
        const row = new Row({ "user": this.qInput("-user-txt").value });
        this.authenticator.v.resetAuthentication(this.logger, row)
        .then((resultRow) => {
            this.controller.v.show(
                "onetimelogin-panel", new PanelData("Row", resultRow));
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    async show(panelData?: PanelData): Promise<void> {
        this.qInput("-user-txt").value = "";
        this.qElement("-div").hidden = false;
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

export class LoginPanel extends BasePanel implements IPanel {
    authenticator: Cfg<IAuthenticator>;
    policyAuthorizer: Cfg<IPolicyAuthorizer>;

    constructor() {
        super();
        this.prefix = "login";
        this.authenticator = new Cfg("auth");
        this.policyAuthorizer = new Cfg("policyquery");
    }

    get id(): string {
        return "login-panel";
    }

    initialize(): void {
        super.initialize();
        this.authenticator.v =
            RZO.getAuthenticator(this.authenticator.name).service;
        this.policyAuthorizer.v =
            RZO.getPolicyAuthorizer(this.policyAuthorizer.name).service;
        this.qForm("-form").addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
        this.qElement("welcome-heading").addEventListener("click", (evt) => {
            evt.preventDefault();
        });
        this.qButton("-reset-btn").addEventListener("click", (evt) => {
            this.onResetPassword(evt);
        });
    }

    private onResetPassword(evt: Event): void {
        this.controller.v.show("password-reset-panel");
    }

    private async fullLogin(creds: Row): Promise<Set<string>> {
        const context = await this.authenticator.v.login(this.logger, creds);
        const queries = this.controller.v.collectPolicyQueries();
        const policies = await this.policyAuthorizer.v.queryPolicies(
            this.logger, context, queries);
        CONTEXT.c = context;
        return policies;
    }

    private onSubmit(evt: Event): void {
        try {
            const targetUsername = this.qInput("-user-txt").value;
            const targetPassword = this.qInput("-password-txt").value;
            if (!targetUsername || !targetPassword) {
                TOASTER.error("You must enter a valid User and Password");
                throw new Error("No User or Password entered");
            }
            const credsRow = new Row(
                { "username": targetUsername, "password": targetPassword });
            this.fullLogin(credsRow)
            .then((policies) => {
                this.qElement("welcome-heading").innerText = targetUsername;
                // Broadcast the "logged-in" message
                this.controller.v.broadcast("logged-in");
                // Apply the policies
                this.controller.v.applyPolicies(policies);
                // Switch the icon on the nav bar
                this.qSVG("person-open-path").classList.toggle("invisible");
                this.qSVG("person-filled-path").classList.toggle("invisible");
                this.qSVG("person-check-path").classList.toggle("invisible");
                // Switch to the main 'Trips' panel
                this.controller.v.show("trips-panel");
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
        } catch (err) {
            TOASTER.exc(err);
        }
    }

    async show(panelData?: PanelData): Promise<void> {
        this.qInput("-user-txt").value = "";
        this.qInput("-password-txt").value = "";
        this.qElement("-div").hidden = false;
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

