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


function checkElement(suffix: string, id: string): HTMLElement {
    const element = document.getElementById(id);
    if (element) {
        const parts = id.split("-");
        if (parts.length > 1) {
            if (parts.slice(-1)[0] != suffix) {
                throw new Error(
                    `Element id: ${id} mismatches suffix ${suffix}`);
            }
            return element;
        } else {
            throw new Error(`${suffix} - cannot parse element id: ${id}`);
        }
    } else {
        throw new Error(`No such element: ${id}`);
    }
}

export function suffix(id: string): string {
    const parts = id.split("-");
    if (parts.length > 1) {
        return parts.slice(-1)[0];
    } else {
        throw new Error(`Cannot parse element id: ${id}`);
    }
}

type ControlType = HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;

export function control(id: string): ControlType {
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
                    throw new Error(`Element id ${id} is not a control type`);
            }
        } else {
            throw new Error(`Cannot parse element id: ${id}`);
        }
    } else {
        throw new Error(`No such element: ${id}`);
    }
}

export function form(id: string): HTMLFormElement {
    const element = checkElement("form", id);
    if (element instanceof HTMLFormElement) {
        return element as HTMLFormElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLFormElement`);
    }
}

export function div(id: string): HTMLElement {
    const element = checkElement("div", id);
    return element as HTMLElement;
}

export function a(id: string): HTMLAnchorElement {
    const element = checkElement("a", id);
    if (element instanceof HTMLAnchorElement) {
        return element as HTMLAnchorElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLAnchorElement`);
    }
}

export function btn(id: string): HTMLButtonElement {
    const element = checkElement("btn", id);
    if (element instanceof HTMLButtonElement) {
        return element as HTMLButtonElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLButtonElement`);
    }
}

export function tbody(id: string): HTMLTableSectionElement {
    const element = checkElement("tbody", id);
    if (element instanceof HTMLTableSectionElement) {
        return element as HTMLTableSectionElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLTableSectionElement`);
    }
}

export function cell(id: string): HTMLTableCellElement {
    const element = checkElement("cell", id);
    if (element instanceof HTMLTableCellElement) {
        return element as HTMLTableCellElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLTableCellElement`);
    }
}

export function p(id: string): HTMLElement {
    const element = checkElement("p", id);
    return element as HTMLElement;
}

export function pre(id: string): HTMLPreElement {
    const element = checkElement("pre", id);
    return element as HTMLPreElement;
}

export function txt(id: string): HTMLInputElement {
    const element = checkElement("txt", id);
    if (element instanceof HTMLInputElement) {
        return element as HTMLInputElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLInputElement`);
    }
}

export function cbox(id: string): HTMLInputElement {
    const element = checkElement("cbox", id);
    if (element instanceof HTMLInputElement) {
        return element as HTMLInputElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLInputElement`);
    }
}

export function tarea(id: string): HTMLTextAreaElement {
    const element = checkElement("tarea", id);
    if (element instanceof HTMLTextAreaElement) {
        return element as HTMLTextAreaElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLTextAreaElement`);
    }
}

export function sel(id: string): HTMLSelectElement {
    const element = checkElement("sel", id);
    if (element instanceof HTMLSelectElement) {
        return element as HTMLSelectElement;
    } else {
        throw new Error(`Element ${id} is not an HTMLSelectElement`);
    }
}

export function heading(id: string): HTMLElement {
    const element = checkElement("heading", id);
    return element as HTMLElement;
}

export function path(id: string): SVGPathElement {
    const element = checkElement("path", id);
    if (element instanceof SVGPathElement) {
        return element as SVGPathElement;
    } else {
        throw new Error(`Element ${id} is not an SVGPathElement`);
    }
}

