/*
    RZO - A Business Application Frame
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
import { Modal } from "bootstrap";

import { ServiceSource, Entity, BigDecimal } from "../../base/core.js";
import { RZO } from "../../base/configuration.js";
import { IPanel, BasePanel, PanelData, DynElement } from "../panel.js";

const ROW_PREFIX = "tsc";
const DEL_BTN_PREFIX = "del";
const QTY_PREFIX = "qty";
const DR_PREFIX = "dr";
const CR_PREFIX = "cr";

export class TransCreatePanel extends BasePanel implements IPanel {
    rowControllers: Map<string, AbortController>;
    deleteRowListener: EventListener;
    qtyBlurListener: EventListener;
    amountBlurListener: EventListener;
    accountModal: Modal;

    constructor() {
        super();
        this.prefix = "doccreate";
        this.deleteRowListener = (evt) => { this.onDeleteRow(evt); };
        this.qtyBlurListener = (evt) => { this.onQtyBlur(evt); };
        this.amountBlurListener = (evt) => { this.onAmountBlur(evt); };
        this.rowControllers = new Map();
        this.accountModal = new Modal(this.qElement("account-dialog-div"));
    }

    get id(): string {
        return "transcreate-panel";
    }

    initialize(): void {
        super.initialize();
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.qButton("-newrow-btn").addEventListener("click", (evt) => {
            this.onNewRow(evt);
        });
        this.qButton("-cancel-btn").addEventListener("click", (evt) => {
            this.onCancel(evt);
        });
        this.qElement("-memo-txt").addEventListener("blur", (evt) => {
            this.onMemoBlur(evt);
        });
        this.qElement("-caccount-btn").addEventListener("click", (evt) => {
            this.onAccountSelect(evt);
        });
        this.qElement("-dr-txt").addEventListener(
            "blur", this.amountBlurListener);
        this.qElement("-cr-txt").addEventListener(
            "blur", this.amountBlurListener);
        this.qElement("-cdr-txt").addEventListener(
            "blur", this.amountBlurListener);
        this.qElement("-cdr-txt").addEventListener(
            "blur", this.amountBlurListener);
    }

    private addTotal(total: BigDecimal, id: string): BigDecimal {
        const input = this.qInput(id);
        if (input.value) {
            return total.add(new BigDecimal(input.value));
        }
        return total;
    }

    private checkBalance(): void {
        let drTotal = this.addTotal(new BigDecimal("0"), "-dr-txt");
        drTotal = this.addTotal(drTotal, "-cdr-txt");
        let crTotal = this.addTotal(new BigDecimal("0"), "-cr-txt");
        crTotal = this.addTotal(crTotal, "-ccr-txt");
        for (const id of this.rowControllers.keys()) {
            drTotal = this.addTotal(drTotal, `${DR_PREFIX}-${id}`);
            crTotal = this.addTotal(crTotal, `${CR_PREFIX}-${id}`);
        }
        const drOutput = this.qElement("-total-dr");
        const crOutput = this.qElement("-total-cr");
        drOutput.innerText = drTotal.toNumeric(10, 2);
        crOutput.innerText = crTotal.toNumeric(10, 2);
        if (drTotal.equals(crTotal)) {
            drOutput.classList.replace("table-danger", "table-success");
            crOutput.classList.replace("table-danger", "table-success");
        } else {
            drOutput.classList.replace("table-success", "table-danger");
            crOutput.classList.replace("table-success", "table-danger");
        }
    }

    private siblingAmountInput(id: string): HTMLInputElement | null {
        if (id.startsWith("dr-")) {
            return this.qInput(`cr-${id.slice(3)}`);
        } else if (id.startsWith("cr-")) {
            return this.qInput(`dr-${id.slice(3)}`);
        } else {
            const parts = id.split("-");
            if (parts.length != 3) {
                return null;
            }
            switch (parts[1]) {
                case "dr":
                    return this.qInput("-cr-txt");
                case "cr":
                    return this.qInput("-dr-txt");
                case "cdr":
                    return this.qInput("-ccr-txt");
                case "ccr":
                    return this.qInput("-cdr-txt");
                default:
                    return null;
            }
        }
    }

    private counterAmountInput(id: string): HTMLInputElement | null {
        if (id.startsWith("dr-") || id.startsWith("cr-")) {
            return null;
        }
        const parts = id.split("-");
        if (parts.length != 3) {
            return null;
        }
        switch (parts[1]) {
            case "dr":
                return this.qInput("-ccr-txt");
            case "cr":
                return this.qInput("-cdr-txt");
            case "cdr":
                return this.qInput("-cr-txt");
            case "ccr":
                return this.qInput("-dr-txt");
            default:
                return null;
        }
    }

    private onAccountSelect(evt: Event): void {
        this.accountModal.show();
    }

    private onAmountBlur(evt: Event): void {
        const blurred = evt.currentTarget as HTMLInputElement;
        const blurredValue = blurred.value;
        if (blurredValue) {
            const counterInput = this.counterAmountInput(blurred.id);
            if (counterInput && !counterInput.value) {
                counterInput.value = blurredValue;
            }
            const siblingInput = this.siblingAmountInput(blurred.id);
            if (siblingInput && siblingInput.value) {
                siblingInput.value = "";
            }
        }
        this.checkBalance();
    }

    private onMemoBlur(evt: Event): void {
        const target = evt.currentTarget as HTMLInputElement;
        const firstValue = target.value;
        const secondSplit = this.qInput("-cmemo-txt");
        const secondValue = secondSplit.value;
        if (firstValue && !secondValue) {
            secondSplit.value = firstValue;
        }
    }

    private onNewRow(evt: Event): void {
        const rowId = Entity.generateId();
        const rowAbortController = new AbortController();
        this.rowControllers.set(rowId, rowAbortController);
        const tr = new DynElement(
            {
                tag: "tr",
                id: `${ROW_PREFIX}-${rowId}`
            });
        tr.append(
            new DynElement({ tag: "td" })
            .append(// Delete button
                new DynElement(
                {
                    tag: "button",
                    id: `${DEL_BTN_PREFIX}-${rowId}`,
                    type: "button",
                    css: "btn btn-secondary",
                    text: "Delete"
                })
                .addListener("click", this.deleteRowListener,
                             rowAbortController)
            )
        );
        tr.append(
            new DynElement({ tag: "td" })
            .append(// Account input group
                new DynElement( { tag: "div", css: "input-group mb-3" })
                .append(// Account input
                    new DynElement( { tag: "input", css: "form-control" })
                )
                .append(// Account button
                    new DynElement(
                    {
                        tag: "button",
                        type: "button",
                        css: "btn btn-secondary",
                        text: "..."
                    })
                )
            )
        );
        tr.append(
            new DynElement({ tag: "td" })
            .append(// Memo
                new DynElement( { tag: "input", css: "form-control" })
            )
        );
        tr.append(
            new DynElement({ tag: "td" })
            .append(// Qty
                new DynElement(
                {
                    tag: "input",
                    id: `${QTY_PREFIX}-${rowId}`,
                    css: "form-control text-end"
                })
                .addListener("blur", this.qtyBlurListener, rowAbortController)
            )
        );
        tr.append(
            new DynElement({ tag: "td" })
            .append(// Price
                new DynElement(
                {
                    tag: "input",
                    css: "form-control text-end"
                })
            )
        );
        tr.append(
            new DynElement({ tag: "td" })
            .append(// Dr
                new DynElement(
                {
                    tag: "input",
                    id: `${DR_PREFIX}-${rowId}`,
                    css: "form-control text-end"
                })
                .addListener("blur", this.amountBlurListener,
                             rowAbortController)
            )
        );
        tr.append(
            new DynElement({ tag: "td" })
            .append(// Cr
                new DynElement(
                {
                    tag: "input",
                    id: `${CR_PREFIX}-${rowId}`,
                    css: "form-control text-end"
                })
                .addListener("blur", this.amountBlurListener,
                             rowAbortController)
            )
        );
        const tbody = this.qElement("-splits");
        tbody.appendChild(tr.asElement());
    }

    private onQtyBlur(evt: Event): void {
        console.log("OnBlur called");
    }

    private onDeleteRow(evt: Event): void {
        const target = evt.currentTarget as Element;
        if (target && target.id && target.id.length > 4) {
            const id = target.id.slice(4);
            const controller = this.rowControllers.get(id);
            if (controller) {
                controller.abort();
                this.rowControllers.delete(id);
            } else {
                console.error(`Cannot find controller: ${id}`);
            }
            const rowId = `${ROW_PREFIX}-${id}`;
            const tr = document.getElementById(rowId);
            if (tr) {
                tr.remove();
                this.checkBalance();
            } else {
                console.error(`Cannot find row: ${rowId}`);
            }
        }
    }

    private onCancel(evt: Event): void {
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }

    private reset(): void {
        for (const entry of this.rowControllers) {
            entry[1].abort();
            const tr = document.getElementById(`${ROW_PREFIX}-${entry[0]}`);
            if (tr) {
                tr.remove();
            }
        }
        this.rowControllers = new Map();
        this.qInput("-memo-txt").value = "";
        this.qInput("-qty-txt").value = "";
        this.qInput("-price-txt").value = "";
        this.qInput("-dr-txt").value = "";
        this.qInput("-cr-txt").value = "";
        this.qInput("-cmemo-txt").value = "";
        this.qInput("-cqty-txt").value = "";
        this.qInput("-cprice-txt").value = "";
        this.qInput("-cdr-txt").value = "";
        this.qInput("-ccr-txt").value = "";
        this.qInput("-doc-date").valueAsDate = new Date();
        this.checkBalance();
    }

    async show(panelData?: PanelData): Promise<void> {
        this.qElement("-div").hidden = false;
        if (PanelData.typeOf(panelData) == "Row") {
            this.reset();
            const row = PanelData.rowOf(panelData);
            this.qInput("-doc-account").value =
                `${row.get("name")} - ${row.get("description")}`;
            this.qElement("-account-td").innerText = row.get("name");
        }
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

