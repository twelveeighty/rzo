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
    Collection, Cfg, ServiceSource, Filter, Query, IResultSet,
    Row, IService
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { BasePanel, DynElement } from "../panel.js";
import { TOASTER } from "../toaster.js";
import { EntityList } from "../list.js";

export type RowListener = (row: Row, target?: HTMLElement) => void;

/* AccountDialog is a global dialog, so meant to only be created once, and
 * accessible via G_AccountDialog().
 */
class AccountDialog {
    private _initialized: boolean;
    static _instance: AccountDialog | null;
    modal: Modal;
    accountsList: AccountsList;
    selectListener: RowListener | null;
    target: HTMLElement | null;

    constructor() {
        this._initialized = false;
        this.accountsList = new AccountsList(
            BasePanel.queryElement("account-dlg-list"),
            BasePanel.queryElement("account-dlg-h5"),
            BasePanel.queryElement("account-dlg-crumbs"),
            "accounts");
        this.selectListener = null;
        this.target = null;
        this.modal = new Modal(BasePanel.queryElement("account-dlg-div"));
    }

    initialize(): void {
        if (this._initialized) {
            throw new Error(
                "You must not call AccountDialog.initialize() more than once");
        }
        this._initialized = true;
        const drillIntoListener: RowListener =
            (row) => { this.onDrillDown(row); };
        const ourSelectListener: RowListener =
            (row) => { this.onAccountSelect(row); };
        this.accountsList.initialize(drillIntoListener, ourSelectListener);
        BasePanel.queryElement("account-dlg-ok-btn")
        .addEventListener("click", (evt) => {
            this.dismiss(evt);
        });
        BasePanel.queryElement("account-dlg-cancel-btn")
        .addEventListener("click", (evt) => {
            this.dismiss(evt);
        });
    }

    private dismiss(evt?: Event): void {
        this.selectListener = null;
        this.target = null;
        this.modal.hide();
    }

    show(selectListener: RowListener, target?: HTMLElement, row?: Row): void {
        this.selectListener = selectListener;
        this.target = target || null;
        this.modal.show();
        if (!this.accountsList.lastResult) {
            this.showTopLevelAccounts();
        }
    }

    private onAccountSelect(row: Row): void {
        const listener = this.selectListener;
        const target = this.target;
        this.dismiss();
        if (listener) {
            if (target) {
                listener(row, target);
            } else {
                listener(row);
            }
        }
    }

    private onDrillDown(row: Row): void {
    }

    showTopLevelAccounts(): void {
        this.accountsList.showTopLevelAccounts();
    }
}

export function G_AccountDialog(): AccountDialog {
    if (AccountDialog._instance) {
        return AccountDialog._instance;
    } else {
        AccountDialog._instance = new AccountDialog();
        return AccountDialog._instance;
    }
}

export class AccountsList {
    collection: Cfg<Collection>;
    list: EntityList;
    defaultQuery: Query;
    lastResult: IResultSet | null;
    lastSelected: Row | null;
    service: Cfg<IService>;
    drillIntoListener: RowListener | null;
    selectListener: RowListener | null;
    crumbs: HTMLElement;
    header: HTMLElement;
    crumbsClickedListener: EventListener;
    crumbsAbortController: AbortController | null;

    constructor(parent: HTMLElement, header: HTMLElement, crumbs: HTMLElement,
                collection: string) {
        this.service = new Cfg("service");
        this.collection = new Cfg(collection);
        this.list = new EntityList(
            parent,
            "name",
            "elementtype",
            "description",
            "holding");
        this.header = header;
        this.crumbs = crumbs;
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
        this.selectListener = null;
        this.crumbsClickedListener = (evt) => {
            evt.preventDefault();
            this.onCrumbsClicked(evt);
        };
        this.crumbsAbortController = null;
    }

    initialize(drillIntoListener: RowListener,
               selectListener: RowListener): void {
        this.collection.v = RZO.getCollection(this.collection.name);
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.drillIntoListener = drillIntoListener;
        this.selectListener = selectListener;
        this.list.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
    }

    clearBreadCrumbs(): void {
        if (this.crumbsAbortController) {
            this.crumbsAbortController.abort();
            this.crumbsAbortController = null;
        }
        this.crumbs.innerHTML = "";
    }

    showTopLevelAccounts(): void {
        this.clearBreadCrumbs();
        this.header.innerText = "Top-level Accounts";
        this.queryList();
    }

    private onCrumbsClicked(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const name = target.dataset["name"];
        if (name) {
            this.branchInto(name);
        } else {
            this.showTopLevelAccounts();
        }
    }

    private drillInto(row: Row): void {
        this.clearBreadCrumbs();
        this.crumbsAbortController = new AbortController();
        this.crumbs.appendChild(
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
                    text: "Top"
                })
                .addListener("click", this.crumbsClickedListener,
                             this.crumbsAbortController)
            )
            .asElement()
        );
        const name = row.getString("name");
        const path = name.split(".");
        let fullPath = "";
        for (let i = 0; i < path.length; i++) {
            if (fullPath) {
                fullPath = `${fullPath}.${path[i]}`;
            } else {
                fullPath = path[i];
            }
            if (i < path.length - 1) {
                this.crumbs.appendChild(
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
                this.crumbs.appendChild(
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
        this.header.innerText = `${name} - ${row.get("description")}`;
        if (this.drillIntoListener) {
            this.drillIntoListener(row);
        }
    }

    private async drillDownOrSelect(id: string): Promise<void> {
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
                    this.drillInto(selectedRow);
                } else {
                    if (this.selectListener) {
                        this.selectListener(selectedRow);
                    }
                }
            }
        }
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.drillDownOrSelect(id)
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    setList(rs: IResultSet, selectedId: string): void {
        this.lastResult = rs;
        this.drillDownOrSelect(selectedId);
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

