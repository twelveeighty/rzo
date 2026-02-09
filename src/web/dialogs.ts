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

import { Modal } from "bootstrap";
import { Row } from "../base/core.js";
import { DynElement } from "./panel.js";

export class OkCancelDialog {
    modal: Modal;
    titleElement: HTMLElement;
    promptElement: HTMLElement;
    okButton: HTMLElement;
    okButtonInnerElement: HTMLElement;
    cancelButton: HTMLElement;
    row: Row;
    okListener: EventListener;

    constructor(title: string, prompt: string, okButtonText: string,
                cancelButtonText: string, okListener: EventListener) {
        this.okListener = okListener;
        this.row = new Row();
        this.titleElement = new DynElement(
            {
                tag: "h5",
                css: "mb-0",
                text: title
            }
        ).asElement();
        this.promptElement = new DynElement(
            {
                tag: "p",
                css: "mb-0",
                text: prompt
            }
        ).asElement();
        this.okButtonInnerElement = new DynElement(
            {
                tag: "strong",
                text: okButtonText
            }
        ).asElement();
        this.okButton = new DynElement(
            {
                tag: "button",
                type: "button",
                css: "btn btn-lg btn-link fs-6 text-decoration-none col-6 " +
                     "py-3 m-0 rounded-0 border-end"
            }
        )
        .addListener("click", (evt) => {
            this.onOKClicked(evt);
        })
        .appendElement(this.okButtonInnerElement)
        .asElement();
        this.cancelButton = new DynElement(
            {
                tag: "button",
                type: "button",
                css: "btn btn-lg btn-link fs-6 text-decoration-none col-6 " +
                     "py-3 m-0 rounded-0",
                text: cancelButtonText,
                data: {
                    "bs-dismiss": "modal"
                }
            }
        )
        .asElement();
        const div = new DynElement(
            {
                tag: "div",
                css: "modal fade",
                data: {
                    "bs-backdrop": "static",
                    "bs-keyboard": "false"
                }
            }
        )
        .addAttribute("tabindex", "-1")
        .addAttribute("aria-labelledby", "staticBackdropLabel")
        .addAttribute("aria-hidden", "true")
        .append(
            new DynElement(
            {
                tag: "div",
                css: "modal-dialog"
            })
            .addAttribute("role", "document")
            .append(
                new DynElement(
                {
                    tag: "div",
                    css: "modal-content rounded-3 shadow"
                })
                .append(
                    new DynElement(
                    {
                        tag: "div",
                        css: "modal-body p-4 text-center"
                    })
                    .appendElement(this.titleElement)
                    .appendElement(this.promptElement)
                )
                .append(
                    new DynElement(
                    {
                        tag: "div",
                        css: "modal-footer flex-nowrap p-0"
                    })
                    .appendElement(this.okButton)
                    .appendElement(this.cancelButton)
                )
            )
        );
        this.modal = new Modal(div.asElement());
    }

    private onOKClicked(evt: Event): void {
        this.modal.hide();
        this.okListener(evt);
    }

    show(row?: Row): void {
        this.row = row || new Row();
        this.modal.show();
    }

    hide(): void {
        this.modal.hide();
    }

    set title(text: string) {
        this.titleElement.innerText = text;
    }

    set prompt(text: string) {
        this.promptElement.innerText = text;
    }

    set okButtonText(text: string) {
        this.okButtonInnerElement.innerText = text;
    }

    set cancelButtonText(text: string) {
        this.cancelButton.innerText = text;
    }
}

