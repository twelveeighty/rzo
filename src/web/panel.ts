/*
    RZO - A Business Application Framework

    Copyright (C) 2024-2026 Frank Vanderham

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
    State, Row, Entity, IService, Cfg, IContext, SideEffects, StringField,
    Logger, MemResultSet, JsonObject
} from "../base/core.js";
import { RZO, CONTEXT } from "../base/configuration.js";
import { TOASTER } from "./toaster.js";

type PanelDataType = "None" | "State" | "Row" | "string" | "Parameter";
type PanelParameter = "Refresh" | "NoRefresh";
type RenderAs = "asHTML" | "asText";
type ControlType = HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;

export class PanelData {

    static isParam(value: PanelParameter, panelData?: PanelData): boolean {
        return !!panelData && panelData.dataType == "Parameter" &&
            panelData.parameter == value;
    }

    static typeOf(panelData?: PanelData): PanelDataType {
        if (!panelData) {
            return "None";
        }
        return panelData.dataType;
    }

    static stateOf(panelData?: PanelData): State {
        if (panelData) {
            return panelData.state;
        }
        throw new Error("panelData is undefined");
    }

    static rowOf(panelData?: PanelData): Row {
        if (panelData) {
            return panelData.row;
        }
        throw new Error("panelData is undefined");
    }

    static stringOf(panelData?: PanelData): string {
        if (panelData) {
            return panelData.asString;
        }
        throw new Error("panelData is undefined");
    }

    static parameterOf(panelData?: PanelData): PanelParameter {
        if (panelData) {
            return panelData.parameter;
        }
        throw new Error("panelData is undefined");
    }


    constructor(public dataType: PanelDataType,
                public data: State | Row | string) {
    }

    get state(): State {
        if (this.dataType != "State") {
            throw new Error(`Data type ${this.dataType} is not State`);
        }
        return this.data as State;
    }

    get row(): Row {
        if (this.dataType != "Row") {
            throw new Error(`Data type ${this.dataType} is not Row`);
        }
        return this.data as Row;
    }

    get asString(): string {
        if (this.dataType != "string") {
            throw new Error(`Data type ${this.dataType} is not string`);
        }
        return this.data as string;
    }

    get parameter(): PanelParameter {
        if (this.dataType != "Parameter") {
            throw new Error(`Data type ${this.dataType} is not Parameter`);
        }
        return this.data as PanelParameter;
    }
}

export type PanelMessage =
    "driver-update" | "logged-in" | "logged-out";

export interface IPanel {
    get id(): string;
    initialize(): void;
    register(controller: PanelController): void;
    show(panelData?: PanelData): Promise<void>;
    canHide(): boolean;
    hide(): void;
    onMessage(message: PanelMessage): Promise<void>;
}

export class AttributeJoiner {
    joiner: string[];

    constructor() {
        this.joiner = [];
    }

    add(prefix: string, value?: string): AttributeJoiner {
        if (value) {
            const header = prefix ? `${prefix}: ` : "";
            this.joiner.push(`${header}${value}`);
        }
        return this;
    }

    toText(): string {
        return this.joiner.join(`\n`);
    }
}

export class NavMenuItem {
    parent: HTMLElement;
    title: string;
    id: string;
    listener: EventListener | null;
    li: HTMLElement | null;
    anchor: HTMLAnchorElement | null;
    abortController: AbortController | null;

    constructor(parent: HTMLElement, id: string, title: string) {
        this.parent = parent;
        this.id = id;
        this.title = title;
        this.listener = null;
        this.li = null;
        this.anchor = null;
        this.abortController = null;
    }

    initialize(listener: EventListener): void {
        this.listener = listener;
    }

    set enabled(value: boolean) {
        if (this.anchor) {
            if (value) {
                this.anchor.classList.remove("disabled");
                this.anchor.ariaDisabled = "false";
            } else {
                this.anchor.classList.add("disabled");
                this.anchor.ariaDisabled = "true";
            }
        }
    }

    show(): void {
        if (!this.li && !this.anchor) {
            if (this.listener) {
                this.abortController = new AbortController();
            }
            const anchor = new DynElement(
                {
                    tag: "a",
                    id: this.id,
                    css: "nav-link",
                    href: "#",
                    text: this.title
                })
            .addAttribute("ariaDisabled", "false")
            .addOptionalListener("click", this.listener, this.abortController);
            const li = new DynElement(
                {
                    tag: "li",
                    css: "nav-item"
                }
            )
            .append(anchor);
            this.li = li.asElement();
            this.anchor = anchor.asAnchorElement();
            this.parent.appendChild(this.li);
        }
    }

    hide(): void {
        if (this.li && this.anchor) {
            if (this.abortController != null) {
                this.abortController.abort();
                this.abortController = null;
            }
            this.li.removeChild(this.anchor);
            this.anchor = null;
            this.parent.removeChild(this.li);
            this.li = null;
        }
    }
}

export class PanelButton {
    parent: HTMLElement;
    title: string;
    id: string;
    listener: EventListener | null;
    btn: HTMLButtonElement | null;
    css: string | null;

    constructor(parent: HTMLElement, id: string, title: string, css?: string) {
        this.parent = parent;
        this.id = id;
        this.title = title;
        this.listener = null;
        this.btn = null;
        if (css) {
            this.css = css;
        } else {
            this.css = null;
        }
    }

    initialize(listener: EventListener): void {
        this.listener = listener;
    }

    set enabled(value: boolean) {
        if (this.btn) {
            this.btn.disabled = !value;
        }
    }

    show(): void {
        if (!this.btn) {
            this.btn = new DynElement(
                {
                    tag: "button",
                    id: this.id,
                    type: "button",
                    css: this.css || "btn btn-primary",
                    html: this.title
                }
            ).asButtonElement();
            if (this.listener) {
                this.btn.addEventListener("click", this.listener, false);
            }
            this.parent.appendChild(this.btn);
        }
    }

    hide(): void {
        if (this.btn) {
            if (this.listener) {
                this.btn.removeEventListener("click", this.listener, false);
            }
            this.parent.removeChild(this.btn);
            this.btn = null;
        }
    }
}

export class BasePanel {
    entity: Cfg<Entity>;
    service: Cfg<IService>;
    controller: Cfg<PanelController>;
    logger: Logger;
    prefix: string;

    static queryElement(id: string): HTMLElement {
        const element = document.getElementById(id);
        if (element) {
            return element;
        } else {
            throw new Error(`No such element: ${id}`);
        }
    }

    static dateTimeLocalString(date: Date): string {
        const month = `${date.getMonth() + 1}`.padStart(2, "0");
        const day = `${date.getDate()}`.padStart(2, "0");
        const hours = `${date.getHours()}`.padStart(2, "0");
        const minutes = `${date.getMinutes()}`.padStart(2, "0");
        const seconds = `${date.getSeconds()}`.padStart(2, "0");
        return `${date.getFullYear()}-${month}-${day}` +
            `T` +
            `${hours}:${minutes}:${seconds}`;
    }

    constructor() {
        this.logger = new Logger("client");
        this.entity = new Cfg("entity");
        this.service = new Cfg("service");
        this.controller = new Cfg("controller");
        this.prefix = "";
    }

    initialize(): void {
        this.logger.configure(RZO);
    }

    register(controller: PanelController): void {
        this.controller.v = controller;
    }

    canHide(): boolean {
        return true;
    }

    async onMessage(message: PanelMessage): Promise<void> {
    }

    ensure(obj: unknown, targetType: Function): unknown {
        if (!(obj instanceof targetType)) {
            throw new Error(
                `${obj} is not an instance of ${targetType.name}`);
        }
        return obj;
    }

    ensureDate(obj: unknown): Date {
        this.ensure(obj, Date);
        return obj as Date;
    }

    fqId(id: string): string {
        return (id[0] == "-" && this.prefix) ? `${this.prefix}${id}` : id;
    }

    qElement(id: string): HTMLElement {
        const fullId = this.fqId(id);
        return BasePanel.queryElement(fullId);
    }

    qSVG(id: string): SVGElement {
        const element = this.qElement(id);
        if (element instanceof SVGElement) {
            return element;
        } else {
            throw new Error(
                `Element ${element.id} is not an SVGElement`);
        }
    }

    qInput(id: string): HTMLInputElement {
        const element = this.qElement(id);
        if (element instanceof HTMLInputElement) {
            return element;
        } else {
            throw new Error(
                `Element ${element.id} is not an HTMLInputElement`);
        }
    }

    qButton(id: string): HTMLButtonElement {
        const element = this.qElement(id);
        if (element instanceof HTMLButtonElement) {
            return element;
        } else {
            throw new Error(
                `Element ${element.id} is not an HTMLButtonElement`);
        }
    }

    qTableSection(id: string): HTMLTableSectionElement {
        const element = this.qElement(id);
        if (element instanceof HTMLTableSectionElement) {
            return element;
        } else {
            throw new Error(
                `Element ${element.id} is not an HTMLTableSectionElement`);
        }
    }

    qSelect(id: string): HTMLSelectElement {
        const element = this.qElement(id);
        if (element instanceof HTMLSelectElement) {
            return element;
        } else {
            throw new Error(
                `Element ${element.id} is not an HTMLSelectElement`);
        }
    }

    qForm(id: string): HTMLFormElement {
        const element = this.qElement(id);
        if (element instanceof HTMLFormElement) {
            return element;
        } else {
            throw new Error(
                `Element ${element.id} is not an HTMLFormElement`);
        }
    }

    addTableRowElement(tbody: HTMLElement, header: string,
                       value: HTMLElement): void {
        if (value) {
            const tr = new DynElement( { tag: "tr" })
            .append(
                new DynElement(
                {
                    tag: "th",
                    text: header
                })
                .addAttribute("scope", "row")
            )
            .append(
                new DynElement( { tag: "td" })
                .appendElement(value)
            );
            tbody.appendChild(tr.asElement());
        }
    }

    addTableRowText(tbody: HTMLElement, header: string, value: string,
                    renderAs?: RenderAs, tdClass?: string): void {
        if (value) {
            const tr = new DynElement( { tag: "tr" })
            .append(
                new DynElement(
                {
                    tag: "th",
                    text: header
                })
                .addAttribute("scope", "row")
            );
            if (renderAs == "asHTML") {
                if (tdClass) {
                    tr.append(
                        new DynElement(
                        {
                            tag: "td",
                            css: tdClass,
                            html: value
                        })
                    );
                } else {
                    tr.append(
                        new DynElement(
                        {
                            tag: "td",
                            html: value
                        })
                    );
                }
            } else {
                if (tdClass) {
                    tr.append(
                        new DynElement(
                        {
                            tag: "td",
                            css: tdClass,
                            text: value
                        })
                    );
                } else {
                    tr.append(
                        new DynElement(
                        {
                            tag: "td",
                            text: value
                        })
                    );
                }
            }
            tbody.appendChild(tr.asElement());
        }
    }

    mapLink(input: string): string {
        if (input.startsWith("q=")) {
            return input.slice(2);
        }
        return encodeURIComponent(input);
    }

    addressMapAnchor(address: string, mapvalue: string): HTMLElement {
        const anchor = new DynElement(
            {
                tag: "a",
                href:
                    `https://maps.google.com/maps?q=${this.mapLink(mapvalue)}`,
                text: address
            })
        .addAttribute("target", "new");
        return anchor.asElement();
    }
}

export interface IBoundControl extends HTMLElement {
    value: string;
    required: boolean;
    setCustomValidity(msg: string): void;
    reportValidity(): boolean;
}

/* Control is a data-bound, i.e. State-aware UI element.
 */
export class Control {
    id: string;
    attribute: string;
    required: boolean;
    element: IBoundControl;

    static instantiate(id: string): ControlType {
        const element = document.getElementById(id);
        if (element) {
            const parts = id.split("-");
            if (parts.length > 1) {
                const suffix = parts.slice(-1)[0];
                switch(suffix) {
                    case "sel":
                        if (element instanceof HTMLSelectElement) {
                            return element as HTMLSelectElement;
                        } else {
                            throw new Error(
                                `Element ${id} is not an HTMLSelectElement`);
                        }
                    case "txt":
                        if (element instanceof HTMLInputElement) {
                            return element as HTMLInputElement;
                        } else {
                            throw new Error(
                                `Element ${id} is not an HTMLInputElement`);
                        }
                    case "tarea":
                        if (element instanceof HTMLTextAreaElement) {
                            return element as HTMLTextAreaElement;
                        } else {
                            throw new Error(
                                `Element ${id} is not an HTMLTextAreaElement`);
                        }
                    default:
                        throw new Error(
                            `Element id ${id} is not a control type`);
                }
            } else {
                throw new Error(`Cannot parse element id: ${id}`);
            }
        } else {
            throw new Error(`No such element: ${id}`);
        }
    }

    constructor(id: string, attribute: string, required?: boolean) {
        this.id = id;
        this.attribute = attribute;
        this.required = !!required;
        this.element = Control.instantiate(id);
    }

    fromState(state: State): void {
        this.element.value = state.asString(this.attribute);
    }

    setValue(entity: Entity, state: State,
             context: IContext): Promise<SideEffects> {
        return entity.setValue(
            state, this.attribute, this.element.value, context)
        .catch((err) => {
            this.element.setCustomValidity(`${err}`);
            throw err;
        });
    }
}

/* DynElement is a "short-hand" way of creating HTML Elements.
 * It is meant purely as a way to create more clean/readable code when
 * dynamically creating elements.
 * The constructor takes a plain object with the following fields:
 *
 * tag: (string) - what element to create; input, button, etc. (required)
 * ns: (string) - if specified createElementNS will be used (optional)
 * id: (string) - the element id (optional)
 * css: (string) - the space separated css class(es) (optional)
 * text: (string) - the element's innerText will be set to this value (optional)
 * html: (string) - the element's innerHTML will be set to this value (optional)
 * type: (string) - used for <input> and <button> type (optional)
 * href: (string) - sets the element's href (optional)
 * data: (object) - sets each key/value pair as 'data-{key} = value' (optional)
 *
 */
export class DynElement {
    scaffold: Row;
    private _element: HTMLElement | null;

    constructor(scaffold: JsonObject) {
        this.scaffold = new Row(scaffold);
        this._element = null;
    }

    protected build(): HTMLElement {
        const tag = this.scaffold.get("tag");
        const newElement = this.scaffold.has("ns") ?
            document.createElementNS(this.scaffold.get("ns"), tag) :
            document.createElement(tag);
        if (this.scaffold.has("id")) {
            newElement.id = this.scaffold.get("id");
        }
        if (this.scaffold.has("css")) {
            newElement.className = this.scaffold.get("css");
        }
        if (this.scaffold.has("text")) {
            newElement.innerText = this.scaffold.get("text");
        }
        if (this.scaffold.has("html")) {
            newElement.innerHTML = this.scaffold.get("html");
        }
        if (this.scaffold.has("type")) {
            newElement.setAttribute("type", this.scaffold.get("type"));
        }
        if (this.scaffold.has("href")) {
            newElement.setAttribute("href", this.scaffold.get("href"));
        }
        if (this.scaffold.has("data")) {
            const data = new Row(this.scaffold.get("data"));
            for (const key of data.columns) {
                newElement.setAttribute(`data-${key}`, data.get(key));
            }
        }
        this._element = newElement;
        return newElement;
    }

    addOptionalListener(type: string, listener?: EventListener | null,
                        controller?: AbortController | null): DynElement {
        if (listener) {
            return this.addListener(type, listener, controller);
        }
        return this;
    }

    addListener(type: string, listener: EventListener,
                controller?: AbortController | null): DynElement {

        const element = this.asElement();
        if (controller) {
            element.addEventListener(
                type, listener, { signal: controller.signal });
        } else {
            element.addEventListener(type, listener);
        }
        return this;
    }

    addAttribute(key: string, value: string): DynElement {
        this.asElement().setAttribute(key, value);
        return this;
    }

    addOptionalAttribute(key: string, value?: string): DynElement {
        if (value) {
            this.asElement().setAttribute(key, value);
        }
        return this;
    }

    asElement(): HTMLElement {
        if (this._element) {
            return this._element;
        }
        return this.build();
    }

    asButtonElement(): HTMLButtonElement {
        const check = this.asElement();
        if (check instanceof HTMLButtonElement) {
            return check;
        } else {
            throw new Error(
                `Element ${check.localName} is not an HTMLButtonElement`);
        }
    }

    asAnchorElement(): HTMLAnchorElement {
        const check = this.asElement();
        if (check instanceof HTMLAnchorElement) {
            return check;
        } else {
            throw new Error(
                `Element ${check.localName} is not an HTMLAnchorElement`);
        }
    }

    asInputElement(): HTMLInputElement {
        const check = this.asElement();
        if (check instanceof HTMLInputElement) {
            return check;
        } else {
            throw new Error(
                `Element ${check.localName} is not an HTMLInputElement`);
        }
    }

    asOptionElement(): HTMLOptionElement {
        const check = this.asElement();
        if (check instanceof HTMLOptionElement) {
            return check;
        } else {
            throw new Error(
                `Element ${check.localName} is not an HTMLOptionElement`);
        }
    }

    append(child: DynElement): DynElement {
        const element = this.asElement();
        const childElement = child.asElement();
        element.appendChild(childElement);
        return this;
    }

    appendElement(child: HTMLElement): DynElement {
        const element = this.asElement();
        element.appendChild(child);
        return this;
    }

    appendChild(child: HTMLElement): HTMLElement {
        const element = this.asElement();
        element.appendChild(child);
        return element;
    }

    appendTextNode(text: string): DynElement {
        const element = this.asElement();
        element.appendChild(document.createTextNode(text));
        return this;
    }

    appendSVG(useRef: string): DynElement {
        const svg = new DynElement(
        {
            ns: "http://www.w3.org/2000/svg",
            tag: "svg"
        })
        .addAttribute("class", "bi me-1")
        .addAttribute("width", "32px")
        .addAttribute("height", "32px")
        .addAttribute("role", "img")
        .addAttribute("aria-label", useRef)
        .append(
            new DynElement(
            {
                ns: "http://www.w3.org/2000/svg",
                tag: "use",
                href: `#${useRef}`
            })
        );
        const element = this.asElement();
        element.appendChild(svg.asElement());
        return this;
    }
}

export class LocalDateControl extends Control {
    localTimeFormat: Intl.DateTimeFormat;
    localMonthFormat: Intl.DateTimeFormat;
    localDayFormat: Intl.DateTimeFormat;

    constructor(id: string, attribute: string, required?: boolean) {
        super(id, attribute, required);
        this.localTimeFormat = new Intl.DateTimeFormat("en-US", {
            hour: "numeric",
            minute: "numeric",
            hour12: false
        });
        this.localMonthFormat = new Intl.DateTimeFormat("en-US", {
            month: "2-digit"
        });
        this.localDayFormat = new Intl.DateTimeFormat("en-US", {
            day: "2-digit"
        });
    }

    fromState(state: State): void {
        const value = state.field(this.attribute).value;
        if (StringField.isNullish(value)) {
            this.element.value = "";
        } else if (value instanceof Date) {
            const result =
                `${(<Date>value).getFullYear()}` +
                `-${this.localMonthFormat.format(value)}` +
                `-${this.localDayFormat.format(value)}` +
                `T${this.localTimeFormat.format(value)}`;
            this.element.value = result;
        } else {
            throw new Error(`Control ${this.id}: invalid value: '${value}'`);
        }
    }
}

export class ViewPanel extends BasePanel {
    div: HTMLElement;
    backBtn: HTMLButtonElement;
    editBtn: HTMLButtonElement;
    editPanelId: string;
    tbody: HTMLTableSectionElement;
    state: State | null;
    dirty: boolean;

    constructor(prefix: string, divId: string, tableId: string,
                backBtnId: string, editBtnId: string, editPanelId: string) {
        super();
        this.prefix = prefix;
        this.editPanelId = editPanelId;
        this.div = this.qElement(divId);
        this.tbody = this.qTableSection(tableId);
        this.backBtn = this.qButton(backBtnId);
        this.editBtn = this.qButton(editBtnId);
        this.dirty = false;
        this.state = null;
    }

    initialize(): void {
        this.backBtn.addEventListener("click", (evt) => {
            this.onBack(evt);
        });
        this.editBtn.addEventListener("click", (evt) => {
            this.onEdit(evt);
        });
    }

    private onBack(evt: Event): void {
        if (!this.dirty) {
            this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
        } else {
            this.dirty = false;
            this.controller.v.pop();
        }
    }

    private onEdit(evt: Event): void {
        // Stack on the 'edit' panel
        if (this.state) {
            this.controller.v.stack(
                this.editPanelId, new PanelData("State", this.state));
        }
    }

    protected stateToUI(state: State): void {
    }

    addRowText(header: string, value: string, renderAs?: RenderAs,
               tdClass?: string): void {
        this.addTableRowText(this.tbody, header, value, renderAs, tdClass);
    }

    async show(panelData?: PanelData): Promise<void> {
        if (PanelData.typeOf(panelData) == "Row") {
            this.dirty = true;
            const rs = MemResultSet.fromRow(PanelData.rowOf(panelData));
            rs.next();
            this.state = this.entity.v.from(rs);
            this.stateToUI(this.state);
            this.div.hidden = false;
        } else if (PanelData.typeOf(panelData) == "string") {
            this.entity.v.load(
                this.service.v, CONTEXT.c, PanelData.stringOf(panelData))
            .then((state) => {
                this.dirty = false;
                this.state = state;
                this.stateToUI(this.state);
                this.div.hidden = false;
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        } else if (!PanelData.isParam("NoRefresh", panelData) && this.state) {
            this.dirty = false;
            this.stateToUI(this.state);
            this.div.hidden = false;
        } else {
            this.div.hidden = false;
        }
    }

    hide(): void {
        this.div.hidden = true;
    }
}



export class FormPanel extends BasePanel {
    state: State | null;
    owner: HTMLElement;
    form: HTMLFormElement;
    submitBtn: HTMLButtonElement;
    cancelBtn: HTMLButtonElement;
    controls: Map<string, Control>;

    constructor(prefix: string, ownerId: string, formId: string,
                submitBtnId: string, cancelBtnId: string, controls: Control[]) {
        super();
        this.prefix = prefix;
        this.owner = this.qElement(ownerId);
        this.form = this.qForm(formId);
        this.submitBtn = this.qButton(submitBtnId);
        this.cancelBtn = this.qButton(cancelBtnId);
        this.controls = new Map();
        for (const control of controls) {
            this.controls.set(control.id, control);
        }
        this.state = null;
    }

    getControl(id: string): Control {
        const fullId = this.fqId(id);
        const control = this.controls.get(fullId);
        if (control) {
            return control;
        }
        throw new Error(`${fullId} is not part of this form`);
    }

    getSelect(id: string): HTMLSelectElement {
        const control = this.getControl(id);
        if (control.element instanceof HTMLSelectElement) {
            return control.element as unknown as HTMLSelectElement;
        } else {
            throw new Error(`${id} is not a Select element`);
        }
    }

    getInput(id: string): HTMLInputElement {
        const control = this.getControl(id);
        if (control.element instanceof HTMLInputElement) {
            return control.element as unknown as HTMLInputElement;
        } else {
            throw new Error(`${id} is not a Select element`);
        }
    }

    protected loadDropdown(selectId: string, collection: string,
                           valueField: string, labelField?: string): void {
        const sel = this.getSelect(selectId);
        this.service.v.queryCollection(
            this.logger, CONTEXT.c, RZO.getCollection(collection))
        .then((resultSet) => {
            while (sel.options.length > 1) {
                sel.remove(1);
            }
            while (resultSet.next()) {
                const opt = document.createElement("option");
                const value = resultSet.getString(valueField);
                const label = (labelField ?
                               resultSet.getString(labelField) : value);
                opt.value = value;
                opt.text = label;
                sel.add(opt);
            }
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    hide(): void {
        this.state = null;
        this.toggleUI(false);
    }

    protected initUI(): void {
        this.form.addEventListener("submit", (evt) => {
            evt.preventDefault();
            this.onSubmit(evt);
        });
        this.cancelBtn.addEventListener("click", (evt) => {
            this.onCancel(evt);
        });
        for (const control of this.controls.values()) {
            control.element.addEventListener("blur", (evt) => {
                this.onBlur(control, evt);
            });
        }
    }

    protected applySideEffect(field: string): void {
        // Find the control that matches this attribute
        const match = Array.from(this.controls.values()).find(
            (control) => control.attribute == field);
        if (match && this.state) {
            match.fromState(this.state);
        }
    }

    protected onBlur(control: Control, evt?: Event): void {
        if (this.state) {
            control.element.setCustomValidity("");
            control.setValue(this.entity.v, this.state!, CONTEXT.c)
            .then((sideEffects) => {
                if (sideEffects) {
                    for (const field of sideEffects) {
                        this.applySideEffect(field);
                    }
                }
            })
            .catch((err) => {
                control.element.reportValidity();
            });
        }
    }

    protected toggleReadOnly(element: HTMLInputElement, isReadOnly: boolean) {
        if (isReadOnly) {
            element.classList.replace("form-control", "form-control-plaintext");
        } else {
            element.classList.replace("form-control-plaintext", "form-control");
        }
    }

    protected fromState(): void {
        if (this.state) {
            for (const control of this.controls.values()) {
                control.fromState(this.state);
            }
        }
    }

    protected toggleUI(visible: boolean): void {
        for (const control of this.controls.values()) {
            if (control.required) {
                control.element.required = visible;
            }
        }
        this.owner.hidden = !visible;
    }

    protected validate(): Promise<SideEffects[]> {
        for (const control of this.controls.values()) {
            control.element.setCustomValidity("");
        }
        const validations: Promise<SideEffects>[] = [];
        for (const control of this.controls.values()) {
            validations.push(
                control.setValue(this.entity.v, this.state!, CONTEXT.c));
        }
        return Promise.all(validations);
    }

    protected reset(): void {
        for (const control of this.controls.values()) {
            control.element.value = "";
            control.element.setCustomValidity("");
        }
    }

    protected async save(): Promise<Row> {
        if (this.state) {
            const row = this.state.hasId() ?
                await this.entity.v.put(
                    this.service.v, this.state, CONTEXT.c) :
                await this.entity.v.post(
                    this.service.v, this.state, CONTEXT.c);
            return row;
        } else {
            throw new Error("this.state must be defined at this point");
        }
    }

    protected onSubmit(evt: Event): void {
        if (this.state) {
            this.validate()
            .then(() => {
                this.save().then((row) => {
                    this.controller.v.pop(new PanelData("Row", row));
                })
                .catch((err) => {
                    TOASTER.error(`ERROR: ${err}`);
                });
            })
            .catch((err) => {
                this.form.reportValidity();
            });
        }
    }

    protected onCancel(evt: Event): void {
        this.reset();
        this.controller.v.pop(new PanelData("Parameter", "NoRefresh"));
    }
}

export class PanelController {
    private panels: Map<string, IPanel>;
    private current: string[];
    private rootId: string;

    constructor(rootId: string) {
        this.rootId = rootId;
        this.current = [];
        this.panels = new Map();
    }

    add(panel: IPanel): IPanel {
        panel.register(this);
        this.panels.set(panel.id, panel);
        return panel;
    }

    initialize(): void {
        for (const panel of this.panels.values()) {
            panel.initialize();
        }
    }

    get(id?: string): IPanel {
        if (!id) {
            throw new Error("empty id passed to get()");
        }
        const panel = this.panels.get(id);
        if (!panel) {
            throw new Error(`Panel ${id} does not exist`);
        }
        return panel;
    }

    async show(id: string, panelData?: PanelData): Promise<void> {
        if (this.current.length > 0) {
            const old = this.current.pop();
            this.get(old).hide();
        }
        while (this.current.length > 1)  {
            this.current.pop();
        }
        this.current.push(id);
        return this.get(id).show(panelData);
    }

    async stack(id: string, panelData?: PanelData): Promise<void> {
        if (this.current.length >= 1) {
            this.get(this.current[this.current.length - 1]).hide();
            this.current.push(id);
            return this.get(id).show(panelData);
        }
        return Promise.resolve();
    }

    async pop(panelData?: PanelData): Promise<void> {
        if (this.current.length > 1) {
            const target = this.current[this.current.length - 2];
            const old = this.current.pop();
            this.get(old).hide();
            return this.get(target).show(panelData);
        } else {
            return this.show(this.rootId, panelData);
        }
    }

    async broadcast(message: PanelMessage): Promise<void> {
        for (const panel of this.panels.values()) {
            panel.onMessage(message);
        }
    }
}

