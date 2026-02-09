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
import {
         ServiceSource, Entity, BigDecimal, Row, Cfg, BizTrans, SideEffects
} from "../../base/core.js";
import { MaterializedCollection } from "../../base/collection.js";
import { Txn, FinDoc } from "../../accting/acc-core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { IPanel, BasePanel, PanelData, DynElement } from "../panel.js";
import { G_AccountDialog, RowListener } from "./accountlist.js";
import { TOASTER } from "../toaster.js";

const ROW_PREFIX = "tsc";
const DEL_BTN_PREFIX = "del";
const QTY_PREFIX = "qty";
const PRICE_PREFIX = "price";
const DR_PREFIX = "dr";
const CR_PREFIX = "cr";
const SEL_BTN_PREFIX = "sel";
const ACCT_PREFIX = "acct";
const DESC_ROW_PREFIX = "tscd";
const DESC_PREFIX = "desc";
const MEMO_PREFIX = "memo";

class FormInputError extends Error {
    element: HTMLInputElement;

    constructor(element: HTMLInputElement, message: string,
                options?: ErrorOptions) {
        super(message, options);
        this.element = element;
    }
}

export class DocCreatePanel extends BasePanel implements IPanel {
    rowControllers: Map<string, AbortController>;
    deleteRowListener: EventListener;
    qtyBlurListener: EventListener;
    accountBlurListener: EventListener;
    amountBlurListener: EventListener;
    accountBtnClickListener: EventListener;
    accountModalListener: RowListener;
    accountsCache: Cfg<MaterializedCollection>;
    findocEntity: Cfg<FinDoc>;
    account: Row | null;

    constructor() {
        super();
        this.prefix = "doccreate";
        this.deleteRowListener = (evt) => { this.onDeleteRow(evt); };
        this.qtyBlurListener = (evt) => { this.onQtyBlur(evt); };
        this.amountBlurListener = (evt) => { this.onAmountBlur(evt); };
        this.accountBlurListener = (evt) => { this.onAccountBlur(evt); };
        this.accountBtnClickListener = (evt) => { this.onAccountSelect(evt); };
        this.accountModalListener =
            (row, target) => { this.onAccountModalSelect(row, target); };
        this.rowControllers = new Map();
        this.accountsCache = new Cfg("accountscache");
        this.findocEntity = new Cfg("findoc");
        this.account = null;
    }

    get id(): string {
        return "doccreate-panel";
    }

    initialize(): void {
        super.initialize();
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.accountsCache.setIfCast(
            "DocCreatePanel",
            RZO.getCollection(this.accountsCache.name),
            MaterializedCollection
        );
        this.findocEntity.setIfCast(
            "DocCreatePanel",
            RZO.getEntity(this.findocEntity.name),
            FinDoc
        );
        this.qButton("-newrow-btn").addEventListener("click", (evt) => {
            this.onNewRow(evt);
        });
        this.qButton("-save-btn").addEventListener("click", (evt) => {
            this.onSave(evt);
        });
        this.qButton("-cancel-btn").addEventListener("click", (evt) => {
            this.onCancel(evt);
        });
        this.qElement("-memo-txt").addEventListener("blur", (evt) => {
            this.onMemoBlur(evt);
        });
        this.qElement("sel-caccount").addEventListener(
            "click", this.accountBtnClickListener);
        this.qElement("acct-caccount").addEventListener(
            "blur", this.accountBlurListener);
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

    private checkBalance(): boolean {
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
            return true;
        } else {
            drOutput.classList.replace("table-success", "table-danger");
            crOutput.classList.replace("table-success", "table-danger");
            return false;
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

    private onAccountModalSelect(row: Row, target?: HTMLElement): void {
        if (target && target.id && target.id.startsWith(`${SEL_BTN_PREFIX}-`)) {
            /* target is the button: sel-XYZ therefore:
             * acct-XYZ is the account input
             * desc-XYZ is the account description td
             */
            const suffix = target.id.slice(SEL_BTN_PREFIX.length + 1);
            this.qInput(`${ACCT_PREFIX}-${suffix}`).value = row.get("name");
            this.qElement(`${DESC_PREFIX}-${suffix}`).innerText =
                row.get("description");
        }
    }

    private onAccountSelect(evt: Event): void {
        G_AccountDialog().show(
            this.accountModalListener, evt.currentTarget as HTMLElement);
    }

    private onAccountBlur(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        if (target && target.id && target.id.startsWith(`${ACCT_PREFIX}-`)) {
            /* target is the input: acct-XYZ therefore:
             * desc-XYZ is the account description td
             */
            const suffix = target.id.slice(ACCT_PREFIX.length + 1);
            const value = this.qInput(`${ACCT_PREFIX}-${suffix}`).value;
            if (value) {
                const desc = this.accountsCache.v.find("name", value);
                if (!desc.empty) {
                    this.qElement(`${DESC_PREFIX}-${suffix}`).innerText =
                        desc.get("description");
                } else {
                    this.qElement(`${DESC_PREFIX}-${suffix}`).innerText =
                        `Invalid account: ${value}`;
                }
            } else {
                this.qElement(`${DESC_PREFIX}-${suffix}`).innerText = "";
            }
        }
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
            new DynElement({ tag: "td" }).addAttribute("rowspan", "2")
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
                    new DynElement(
                    {
                        tag: "input",
                        id: `${ACCT_PREFIX}-${rowId}`,
                        css: "form-control"
                    })
                    .addListener("blur", this.accountBlurListener,
                                 rowAbortController)
                )
                .append(// Account button
                    new DynElement(
                    {
                        tag: "button",
                        id: `${SEL_BTN_PREFIX}-${rowId}`,
                        type: "button",
                        css: "btn btn-secondary",
                        text: "..."
                    })
                    .addListener("click", this.accountBtnClickListener,
                                 rowAbortController)
                )
            )
        );
        tr.append(
            new DynElement({ tag: "td" }).addAttribute("rowspan", "2")
            .append(// Memo
                new DynElement(
                {
                    tag: "input",
                    id: `${MEMO_PREFIX}-${rowId}`,
                    css: "form-control"
                })
            )
        );
        tr.append(
            new DynElement({ tag: "td" }).addAttribute("rowspan", "2")
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
            new DynElement({ tag: "td" }).addAttribute("rowspan", "2")
            .append(// Price
                new DynElement(
                {
                    tag: "input",
                    id: `${PRICE_PREFIX}-${rowId}`,
                    css: "form-control text-end"
                })
            )
        );
        tr.append(
            new DynElement({ tag: "td" }).addAttribute("rowspan", "2")
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
            new DynElement({ tag: "td" }).addAttribute("rowspan", "2")
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
        const trDesc = new DynElement(
            {
                tag: "tr",
                id: `${DESC_ROW_PREFIX}-${rowId}`
            });
        trDesc.append(
            new DynElement(
            {
                tag: "td",
                id: `${DESC_PREFIX}-${rowId}`,
                css: "text-small"
            })
        );
        const tbody = this.qElement("-splits");
        tbody.appendChild(tr.asElement());
        tbody.appendChild(trDesc.asElement());
    }

    private onQtyBlur(evt: Event): void {
    }

    private deleteRow(prefix: string, id: string): void {
        const fullId = `${prefix}-${id}`;
        const tr = document.getElementById(fullId);
        if (tr) {
            tr.remove();
        } else {
            console.error(`Cannot find row: ${fullId}`);
        }
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
            this.deleteRow(DESC_ROW_PREFIX, id);
            this.deleteRow(ROW_PREFIX, id);
            this.checkBalance();
        }
    }

    private required(id: string): string {
        const input = this.qInput(id);
        const value = input.value;
        if (!value) {
            throw new FormInputError(input, "This field requires a value");
        }
        return value;
    }

    private amountCharged(drId: string, crId: string): Row {
        const drInput = this.qInput(drId);
        const crInput = this.qInput(crId);
        const drValue = drInput.value;
        const crValue = crInput.value;
        if (drValue && !crValue) {
            return new Row({ change: "Dr", amount: new BigDecimal(drValue) });
        } else if (crValue && !drValue) {
            return new Row({ change: "Cr", amount: new BigDecimal(crValue) });
        } else if (drValue) {
            throw new FormInputError(
                drInput, "Must specify either Dr or Cr, not both");
        } else {
            throw new FormInputError(drInput, "One of Dr or Cr is required");
        }
    }

    private quantPrice(qId: string, pId: string, amount: BigDecimal): Row {
        const quantInput = this.qInput(qId);
        const priceInput = this.qInput(pId);
        const quant = quantInput.value;
        const price = priceInput.value;
        if (quant || price) {
            if (!quant) {
                throw new FormInputError(
                    quantInput, "Must specify Quantity if Price is specified");
            }
            if (!price) {
                throw new FormInputError(
                    priceInput, "Must specify Price if Quantity is specified");
            }
            const quantBD = new BigDecimal(quant);
            const priceBD = new BigDecimal(price);
            const result = quantBD.multiply(price);
            if (result.notEquals(amount)) {
                throw new FormInputError(
                    quantInput,
                    `Amount value does not match quantity * price: ` +
                    `${amount.toString()} vs ${result.toString()}`);
            }
            return new Row({ quantity: quantBD, price: priceBD });
        } else {
            return new Row();
        }
    }

    private async save(): Promise<void> {
        try {
            const targetAccount = Row.must(this.account);
            const now = new Date();
            // FinDoc
            const fnt = this.findocEntity.v;
            let state = await fnt.create(CONTEXT.c, this.service.v);
            let se: Promise<SideEffects>[] = [];
            const targetAccountName = targetAccount.get("name");
            fnt.cpSetValue(state, "account", targetAccountName, CONTEXT.c, se);
            //TODO: Add a doc specific memo field
            fnt.cpSetValue(state, "memo", this.required("-memo-txt"),
                          CONTEXT.c, se);
            fnt.cpSetValue(state, "posted", now, CONTEXT.c, se);
            fnt.cpSetValue(state, "created", now, CONTEXT.c, se);
            await Promise.all(se);
            const docnum = state.field("docnum").value;
            // Txn
            const txn = new Txn(fnt, state);
            // First AccSplit
            let chargeRow = this.amountCharged("-dr-txt", "-cr-txt");
            const snt = fnt.splitEntity.v;
            state = await snt.create(CONTEXT.c, this.service.v);
            se = [];
            // Set findoc without validation, since it doesn't exist yet.
            state.field("findoc").value = docnum;
            snt.cpSetValue(state, "account", targetAccountName, CONTEXT.c, se);
            snt.cpSetValue(state, "change", chargeRow.get("change"),
                           CONTEXT.c, se);
            let qpRow = this.quantPrice("-qty-txt", "-price-txt",
                BigDecimal.ensure(chargeRow.get("amount")));
            if (!qpRow.empty) {
                snt.cpSetValue(state, "quantity", qpRow.get("quantity"),
                               CONTEXT.c, se);
                snt.cpSetValue(state, "price", qpRow.get("price"),
                               CONTEXT.c, se);
            }
            snt.cpSetValue(state, "amount", chargeRow.get("amount"),
                           CONTEXT.c, se);
            snt.cpSetValue(state, "created", now, CONTEXT.c, se);
            snt.cpSetValue(state, "posted", now, CONTEXT.c, se);
            snt.cpSetValue(state, "memo", this.required("-memo-txt"),
                           CONTEXT.c, se);
            await Promise.all(se);
            txn.splits.push(state);
            // Second AccSplit
            chargeRow = this.amountCharged("-cdr-txt", "-ccr-txt");
            state = await snt.create(CONTEXT.c, this.service.v);
            se = [];
            // Set findoc without validation, since it doesn't exist yet.
            state.field("findoc").value = docnum;
            snt.cpSetValue(state, "account", this.required("acct-caccount"),
                           CONTEXT.c, se);
            snt.cpSetValue(state, "change", chargeRow.get("change"),
                           CONTEXT.c, se);
            qpRow = this.quantPrice("-cqty-txt", "-cprice-txt",
                BigDecimal.ensure(chargeRow.get("amount")));
            if (!qpRow.empty) {
                snt.cpSetValue(state, "quantity", qpRow.get("quantity"),
                               CONTEXT.c, se);
                snt.cpSetValue(state, "price", qpRow.get("price"),
                               CONTEXT.c, se);
            }
            snt.cpSetValue(state, "amount", chargeRow.get("amount"),
                           CONTEXT.c, se);
            snt.cpSetValue(state, "created", now, CONTEXT.c, se);
            snt.cpSetValue(state, "posted", now, CONTEXT.c, se);
            snt.cpSetValue(state, "memo", this.required("-cmemo-txt"),
                           CONTEXT.c, se);
            await Promise.all(se);
            txn.splits.push(state);
            for (const uuid of this.rowControllers.keys()) {
                chargeRow = this.amountCharged(
                    `${DR_PREFIX}-${uuid}`, `${CR_PREFIX}-${uuid}`);
                state = await snt.create(CONTEXT.c, this.service.v);
                se = [];
                // Set findoc without validation, since it doesn't exist yet.
                state.field("findoc").value = docnum;
                snt.cpSetValue(state, "account",
                               this.required(`${ACCT_PREFIX}-${uuid}`),
                               CONTEXT.c, se);
                snt.cpSetValue(state, "change", chargeRow.get("change"),
                               CONTEXT.c, se);
                qpRow = this.quantPrice(
                    `${QTY_PREFIX}-${uuid}`,
                    `${PRICE_PREFIX}-${uuid}`,
                    BigDecimal.ensure(chargeRow.get("amount")));
                if (!qpRow.empty) {
                    snt.cpSetValue(state, "quantity", qpRow.get("quantity"),
                                   CONTEXT.c, se);
                    snt.cpSetValue(state, "price", qpRow.get("price"),
                                   CONTEXT.c, se);
                }
                snt.cpSetValue(state, "amount", chargeRow.get("amount"),
                               CONTEXT.c, se);
                snt.cpSetValue(state, "created", now, CONTEXT.c, se);
                snt.cpSetValue(state, "posted", now, CONTEXT.c, se);
                snt.cpSetValue(state, "memo",
                               this.required(`${MEMO_PREFIX}-${uuid}`),
                               CONTEXT.c, se);
                await Promise.all(se);
                txn.splits.push(state);
            }
            const bt = new BizTrans();
            await txn.toBizTrans(bt, this.logger, CONTEXT.c, this.service.v);
            await this.service.v.processBizTrans(this.logger, bt);
            this.controller.v.show(
                "account-view-panel",
                new PanelData("string", targetAccount.getString("_id")));
        } catch (err: any) {
            if (err instanceof FormInputError) {
                const input = (<FormInputError>err).element;
                input.setCustomValidity(`${err}`);
                input.reportValidity();
            } else {
                TOASTER.error(`ERROR: ${err}`);
            }
        }
    }

    private onSave(evt: Event): void {
        this.save();
    }

    private onCancel(evt: Event): void {
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }

    private resetInput(id: string): void {
        const input = this.qInput(id);
        input.setCustomValidity("");
        input.value = "";
    }

    private reset(): void {
        for (const entry of this.rowControllers) {
            entry[1].abort();
            this.deleteRow(DESC_ROW_PREFIX, entry[0]);
            this.deleteRow(ROW_PREFIX, entry[0]);
        }
        this.rowControllers = new Map();
        this.resetInput("acct-caccount");
        this.qElement("desc-caccount").innerText = "";
        this.resetInput("-doc-account");
        this.resetInput("-account-txt");
        this.qElement("-account-desc").innerText = "";
        this.resetInput("-memo-txt");
        this.resetInput("-qty-txt");
        this.resetInput("-price-txt");
        this.resetInput("-dr-txt");
        this.resetInput("-cr-txt");
        this.resetInput("-cmemo-txt");
        this.resetInput("-cqty-txt");
        this.resetInput("-cprice-txt");
        this.resetInput("-cdr-txt");
        this.resetInput("-ccr-txt");
        this.qInput("-doc-date").value =
            BasePanel.dateTimeLocalString(new Date());
        this.checkBalance();
        this.account = null;
    }

    async show(panelData?: PanelData): Promise<void> {
        this.qElement("-div").hidden = false;
        this.reset();
        if (PanelData.typeOf(panelData) == "Row") {
            if (this.accountsCache.v.cache.rowCount == 0) {
                await this.accountsCache.v.build(CONTEXT.c);
            }
            const row = PanelData.rowOf(panelData);
            this.qInput("-doc-account").value =
                `${row.get("name")} - ${row.get("description")}`;
            this.qInput("-account-txt").value = row.get("name");
            this.qElement("-account-desc").innerText = row.get("description");
            this.account = row;
        }
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

