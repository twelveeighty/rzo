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

import { IPanel, BasePanel, PanelData } from "./panel.js";

export class OpenIDPanel extends BasePanel implements IPanel {
    constructor() {
        super();
        this.prefix = "openid";
    }

    get id(): string {
        return "openid-panel";
    }

    initialize(): void {
    }

    async show(panelData?: PanelData): Promise<void> {
        if (PanelData.typeOf(panelData) == "string") {
            this.qElement("-error-pre").innerText =
                PanelData.stringOf(panelData);
        } else {
            this.qElement("-error-pre").innerText = "";
        }
        this.qElement("-div").hidden = false;
    }

    hide(): void {
        this.qElement("-div").hidden = true;
    }
}

