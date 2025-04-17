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

import {
    State, Row, Entity, IService, Cfg, IContext, SideEffects, StringField,
    Logger
} from "../base/core.js";
import { RZO, CONTEXT } from "../base/configuration.js";

import { TOASTER } from "./toaster.js";
import * as X from "./common.js";

type PanelDataType = "None" | "State" | "Row" | "string" | "Parameter";
type PanelParameter = "Refresh" | "NoRefresh";

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
    parent: HTMLUListElement;
    title: string;
    id: string;
    listener: EventListener | null;
    li: HTMLLIElement | null;
    anchor: HTMLAnchorElement | null;

    constructor(parent: HTMLUListElement, id: string, title: string) {
        this.parent = parent;
        this.id = id;
        this.title = title;
        this.listener = null;
        this.li = null;
        this.anchor = null;
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
            this.li = document.createElement("li") as HTMLLIElement;
            this.li.className = "nav-item";

            this.anchor = document.createElement("a") as HTMLAnchorElement;
            this.anchor.id = this.id;
            this.anchor.className = "nav-link";
            this.anchor.ariaDisabled = "false";
            this.anchor.href = "#";
            this.anchor.innerHTML = this.title;

            if (this.listener) {
                this.anchor.addEventListener("click", this.listener, false);
            }
            this.li.appendChild(this.anchor);
            this.parent.appendChild(this.li);
        }
    }

    hide(): void {
        if (this.li && this.anchor) {
            if (this.listener) {
                this.anchor.removeEventListener("click", this.listener, false);
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

    constructor(parent: HTMLElement, id: string, title: string) {
        this.parent = parent;
        this.id = id;
        this.title = title;
        this.listener = null;
        this.btn = null;
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
            this.btn = document.createElement("button") as HTMLButtonElement;
            this.btn.id = this.id;
            this.btn.type = "button";
            this.btn.className = "btn btn-primary";
            this.btn.innerHTML = this.title;
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

    constructor() {
        this.logger = new Logger("client");
        this.entity = new Cfg("entity");
        this.service = new Cfg("service");
        this.controller = new Cfg("controller");
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
}

export interface IBoundControl extends HTMLElement {
    value: string;
    required: boolean;
    setCustomValidity(msg: string): void;
    reportValidity(): boolean;
}

export class Control {
    id: string;
    attribute: string;
    required: boolean;
    element: IBoundControl;

    constructor(id: string, attribute: string, required?: boolean) {
        this.id = id;
        this.attribute = attribute;
        this.required = !!required;
        this.element = X.control(id);
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

export class FormPanel extends BasePanel {
    state: State | null;
    owner: HTMLElement;
    form: HTMLFormElement;
    submitBtn: HTMLButtonElement;
    cancelBtn: HTMLButtonElement;
    controls: Map<string, Control>;

    constructor(ownerId: string, formId: string, submitBtnId: string,
                cancelBtnId: string, controls: Control[]) {

        super();

        this.owner = X.div(ownerId);
        this.form = X.form(formId);
        this.submitBtn = X.btn(submitBtnId);
        this.cancelBtn = X.btn(cancelBtnId);

        this.controls = new Map();

        for (const control of controls) {
            this.controls.set(control.id, control);
        }

        this.state = null;
    }

    getControl(id: string): Control {
        const control = this.controls.get(id);
        if (control) {
            return control;
        }
        throw new Error(`${id} is not part of this form`);
    }

    getSelect(id: string): HTMLSelectElement {
        if (X.suffix(id) == "sel") {
            const control = this.controls.get(id);
            if (control) {
                return control.element as unknown as HTMLSelectElement;
            }
            throw new Error(`${id} is not part of this form`);
        }
        throw new Error(`${id} does not have a select suffix`);
    }

    getInput(id: string): HTMLInputElement {
        if (X.suffix(id) == "txt") {
            const control = this.controls.get(id);
            if (control) {
                return control.element as unknown as HTMLInputElement;
            }
            throw new Error(`${id} is not part of this form`);
        }
        throw new Error(`${id} does not have an input suffix`);
    }

    getCheckbox(id: string): HTMLInputElement {
        if (X.suffix(id) == "cbox") {
            const control = this.controls.get(id);
            if (control) {
                return control.element as unknown as HTMLInputElement;
            }
            throw new Error(`${id} is not part of this form`);
        }
        throw new Error(`${id} does not have an input suffix`);
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

    protected onSubmit(evt: Event): void {
        if (this.state) {
            this.validate()
            .then(() => {
                const action = this.state?.hasId() ?
                    this.entity.v.put(
                        this.service.v, this.state!, CONTEXT.c) :
                    this.entity.v.post(
                        this.service.v, this.state!, CONTEXT.c);
                action.then((row) => {
                    TOASTER.info(`Saved: ${row.getString("_id")}`);
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

