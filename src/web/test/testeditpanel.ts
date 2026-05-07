/*
    RZO - A Business Application Framework

    Copyright (C) 2026 Frank Vanderham

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

import { ServiceSource } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import {
         IPanel, FormPanel, Control, PanelData, DateControl, LocalDateControl,
         CheckBoxControl
} from "../panel.js";

export class TestEditPanel extends FormPanel implements IPanel {

    constructor() {
        super("test", "-edit-div", "-edit-form",
              "-edit-btn", "-edit-cancel-btn", [
                  new Control(
                      "test-keyfield-txt", "keyfield", true),
                  new Control(
                      "test-stringfield-txt", "stringfield", true),
                  new Control(
                      "test-uniquefield-txt", "uniquefield", true),
                  new Control(
                      "test-integerfield-txt", "integerfield", true),
                  new Control(
                      "test-aliasvaluelist-sel", "aliasvaluelist", true),
                  new Control(
                      "test-numberfield-txt", "numberfield", true),
                  new Control(
                      "test-amountfield-txt", "amountfield", true),
                  new CheckBoxControl(
                      "test-booleanfield-txt", "booleanfield"),
                  new DateControl(
                      "test-datefield-txt", "datefield", true),
                  new LocalDateControl(
                      "test-datetimefield-txt", "datetimefield", true),
                  new Control(
                      "test-uuidfield-txt", "uuidfield", true),
                  new Control(
                      "test-pathfield-txt", "pathfield", true),
              ]);
    }

    get id(): string {
        return "test-edit-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("testversioned");
        this.service.v =
            (<ServiceSource>RZO.getSource("pouchdb")
             .ensure(ServiceSource)).service;
        this.initUI();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            this.state = panelData.state;
        } else {
            this.state = await this.entity.v.create(CONTEXT.c, this.service.v);
        }
        this.fromState();
        this.toggleUI(true);
    }
}

