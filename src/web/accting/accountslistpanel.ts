/*
    RZO - A Business Application Framework

    Copyright (C) 2025 Frank Vanderham

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

import { Modal } from "bootstrap";
import {
    Collection, Cfg, ServiceSource, Filter, Query, IResultSet, MemResultSet,
    Row, BooleanField, IService
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import {
    IPanel, BasePanel, PanelData, PanelButton, DynElement
} from "../panel.js";
import { EntityList } from "../list.js";

export class AccountDialog extends Modal {
    constructor(parent: HTMLElement) {
        super(parent);
    }
}

export type RowListener = (row: Row) => void;

export class AccountsList {
    collection: Cfg<Collection>;
    list: EntityList;
    defaultQuery: Query;
    lastResult: IResultSet | null;
    lastSelected: Row | null;
    service: Cfg<IService>;
    drillIntoListener: RowListener | null;
    showDetailListener: RowListener | null;

    constructor(element: HTMLElement, collection: string) {
        this.service = new Cfg("service");
        this.collection = new Cfg(collection);
        this.list = new EntityList(
            element,
            "name",
            "elementtype",
            "description",
            "holding");
        this.defaultQuery = new Query(
            [],
            new Filter("or")
                .op("name", "=", "C100")
                .op("name", "=", "C200")
                .op("name", "=", "C300")
                .op("name", "=", "C400")
                .op("name", "=", "C500"),
            [{field: "name", order: "asc"}]);
        this.lastResult = null;
        this.lastSelected = null;
        this.drillIntoListener = null;
        this.showDetailListener = null;
    }

    initialize(drillIntoListener: RowListener,
               showDetailListener: RowListener): void {
        this.collection.v = RZO.getCollection(this.collection.name);
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.drillIntoListener = drillIntoListener;
        this.showDetailListener = showDetailListener;
        this.list.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    private async drillDownOrIn(id: string): Promise<void> {
        if (this.lastResult) {
            const selectedRow = this.lastResult.find(
                (row) => row.get("_id") == id);
            if (selectedRow) {
                this.lastSelected = selectedRow;
                const query = new Query(
                    [],
                    new Filter().op(
                        "name", "~", `${selectedRow.get("name")}.*{1}`),
                        [{field: "name", order: "asc"}]);
                const resultSet = await this.collection.v.query(
                    CONTEXT.c, query);
                if (resultSet.rowCount > 0) {
                    this.lastResult = resultSet;
                    this.list.render(resultSet);
                    if (this.drillIntoListener) {
                        this.drillIntoListener(selectedRow);
                    }
                } else {
                    if (this.showDetailListener) {
                        this.showDetailListener(selectedRow);
                    }
                }
            }
        }
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.drillDownOrIn(id)
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    setList(rs: IResultSet, selectedId: string): void {
        this.lastResult = rs;
        this.drillDownOrIn(selectedId);
    }

    async branchInto(name: string): Promise<void> {
        const selectedRS = await this.collection.v.query(
            CONTEXT.c,
            new Query([], new Filter().op("name", "=", name))
        );
        if (selectedRS.next()) {
            this.lastSelected = selectedRS.getRow();
            const childrenRS = await this.collection.v.query(
                CONTEXT.c,
                new Query(
                    [],
                    new Filter().op("name", "~", `${name}.*{1}`),
                    [{field: "name", order: "asc"}]
                )
            );
            if (childrenRS.rowCount > 0) {
                this.lastResult = childrenRS;
                this.list.render(childrenRS);
            }
            if (this.drillIntoListener) {
                this.drillIntoListener(this.lastSelected);
            }
        } else {
            TOASTER.error(`Cannot find account: ${name}`);
        }
    }

    queryList(filter?: Filter): void {
        this.lastSelected = null;
        const query = filter ?
            new Query(
            [],
            filter,
            [{field: "name", order: "asc"}]) :
            this.defaultQuery;
        this.collection.v.query(CONTEXT.c, query)
        .then((resultSet) => {
            this.lastResult = resultSet;
            this.list.render(resultSet);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }
}

export class AccountsListPanel extends BasePanel implements IPanel {
    splitsBtn: PanelButton;
    transBtn: PanelButton;
    accountsList: AccountsList;
    crumbsClickedListener: EventListener;
    crumbsAbortController: AbortController | null;

    constructor() {
        super();
        this.prefix = "accounts";
        const btnDiv = this.qElement("-buttons-div");
        this.splitsBtn = new PanelButton(
            btnDiv, this.fqId("-splits-btn"),
            "View Splits...", "btn btn-primary me-2");
        this.transBtn = new PanelButton(
            btnDiv, this.fqId("-trans-btn"),
            "View Transactions...", "btn btn-primary me-2");
        this.accountsList = new AccountsList(
            this.qElement("-list-div"), "accounts");
        this.crumbsClickedListener = (evt) => {
            evt.preventDefault();
            this.onCrumbsClicked(evt);
        };
        this.crumbsAbortController = null;
    }

    get id(): string {
        return "accounts-panel";
    }

    initialize(): void {
        this.entity.v = RZO.getEntity("account");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        const drillIntoListener: RowListener =
            (row) => { this.onDrillDown(row); };
        const showDetailListener: RowListener =
            (row) => { this.onShowDetail(row); };
        this.accountsList.initialize(drillIntoListener, showDetailListener);
        this.qButton("-create-btn").addEventListener("click", (evt) => {
            this.onCreate(evt);
        });
        this.qButton("-edit-btn").addEventListener("click", (evt) => {
            this.onEdit(evt);
        });
        this.splitsBtn.initialize((evt) => {
            this.onSplits(evt);
        });
        this.transBtn.initialize((evt) => {
            this.onTransactions(evt);
        });
    }

    private onCreate(evt: Event): void {
        this.controller.v.stack("account-edit-panel");
    }

    private onEdit(evt: Event): void {
        if (this.accountsList.lastSelected) {
            this.controller.v.stack(
                "account-edit-panel",
                new PanelData(
                    "State",
                    this.entity.v.rowToState(this.accountsList.lastSelected)));
        }
    }

    private onSplits(evt: Event): void {
        if (this.accountsList.lastSelected) {
            this.navToAccountSplits(this.accountsList.lastSelected);
        }
    }

    private onTransactions(evt: Event): void {
    }

    private navToAccountSplits(row: Row): void {
        this.controller.v.stack(
            BooleanField.toBoolean(row.get("islogged")) ?
                "logsplits-panel" :
                "splits-panel",
            new PanelData("Row", row));
    }

    private setButtonsVisible(visible: boolean): void {
        if (visible) {
            this.splitsBtn.show();
            this.transBtn.show();
        } else {
            this.splitsBtn.hide();
            this.transBtn.hide();
        }
    }
    private onCrumbsClicked(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const name = target.dataset["name"];
        if (name) {
            this.accountsList.branchInto(name);
        }
    }

    private onShowDetail(row: Row): void {
        this.controller.v.stack(
            "account-view-panel",
            new PanelData("string", row.getString("_id")));
    }

    private onDrillDown(row: Row): void {
        this.clearBreadCrumbs();
        const name = row.getString("name");
        const path = name.split(".");
        const crumbs = this.qElement("-crumbs");
        this.crumbsAbortController = new AbortController();
        let fullPath = "";
        for (let i = 0; i < path.length; i++) {
            if (fullPath) {
                fullPath = `${fullPath}.${path[i]}`;
            } else {
                fullPath = path[i];
            }
            if (i < path.length - 1) {
                crumbs.appendChild(
                    new DynElement(
                    {
                        tag: "li",
                        css: "breadcrumb-item"
                    })
                    .append(
                        new DynElement(
                        {
                            tag: "a",
                            href: "#",
                            text: path[i],
                            data: { name: fullPath }
                        })
                        .addListener("click", this.crumbsClickedListener,
                                     this.crumbsAbortController)
                    )
                    .asElement()
                );
            } else {
                crumbs.appendChild(
                    new DynElement(
                    {
                        tag: "li",
                        css: "breadcrumb-item active",
                        text: path[i]
                    })
                    .asElement()
                );
            }
        }
        this.qElement("accounts-heading").innerText =
            `${name} - ${row.get("description")}`;
        this.setButtonsVisible(true);
    }

    private clearBreadCrumbs(): void {
        if (this.crumbsAbortController) {
            this.crumbsAbortController.abort();
        }
        this.qElement("-crumbs").innerHTML = "";
    }

    async show(panelData?: PanelData): Promise<void> {
        const nav = this.qElement("nav-accounts-a");
        nav.classList.add("active");
        nav.ariaCurrent = "page";
        this.qElement("-div").hidden = false;
        if (!PanelData.isParam("NoRefresh", panelData)) {
            if (PanelData.typeOf(panelData) == "Row") {
                const row = PanelData.rowOf(panelData);
                this.accountsList.setList(
                    MemResultSet.fromRow(row), row.get("_id"));
            } else {
                this.setButtonsVisible(false);
                this.clearBreadCrumbs();
                this.qElement("-heading").innerText = "Top-level Accounts";
                this.accountsList.queryList();
            }
        }
    }

    hide(): void {
        const nav = this.qElement("nav-accounts-a");
        nav.classList.remove("active");
        nav.ariaCurrent = "false";
        this.qElement("-div").hidden = true;
    }
}

