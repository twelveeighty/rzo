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

import { Cfg, Entity } from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import { TOASTER } from "../toaster.js";
import * as X from "../common.js";
import {
    IPanel, FormPanel, Control, LocalDateControl, PanelData
} from "../panel.js";

export class TripEditPanel extends FormPanel implements IPanel {

    ooverrideCheckbox: HTMLInputElement;
    doverrideCheckbox: HTMLInputElement;
    omaplinkManual: HTMLInputElement;
    dmaplinkManual: HTMLInputElement;
    riderNum: HTMLInputElement;
    riderName: HTMLInputElement;

    riderEntity: Cfg<Entity>;

    constructor() {
        super("trip-edit-div", "trip-edit-form",
              "trip-edit-btn", "trip-edit-cancel-btn", [
                  new Control("trip-tripnum-txt", "tripnum", false),
                  new Control("trip-zone-sel", "zone", true),
                  new Control("trip-status-sel", "status", true),
                  new Control("trip-description-txt", "description", false),
                  new Control("trip-triptype-sel", "triptype", true),
                  new LocalDateControl("trip-appointmentts-txt",
                                       "appointmentts", true),
                  new LocalDateControl("trip-returnts-txt", "returnts", false),
                  new Control("trip-oaddress1-txt", "oaddress1", true),
                  new Control("trip-oaddress2-txt", "oaddress2", false),
                  new Control("trip-ocity-txt", "ocity", false),
                  new Control("trip-ostateprov-txt", "ostateprov", false),
                  new Control("trip-opostalcode-txt", "opostalcode", false),
                  new Control("trip-omaplink-txt", "omaplink", false),
                  new Control("trip-omaplinkmanual-txt", "omaplinkmanual",
                              false),
                  new Control("trip-ophone-txt", "ophone", true),
                  new Control("trip-daddress1-txt", "daddress1", true),
                  new Control("trip-daddress2-txt", "daddress2", false),
                  new Control("trip-dcity-txt", "dcity", false),
                  new Control("trip-dstateprov-txt", "dstateprov", false),
                  new Control("trip-dpostalcode-txt", "dpostalcode", false),
                  new Control("trip-dmaplink-txt", "dmaplink", false),
                  new Control("trip-dmaplinkmanual-txt", "dmaplinkmanual",
                              false),
                  new Control("trip-dphone-txt", "dphone", true),
                  new Control("trip-comments-tarea", "comments", false),
              ]);

        this.ooverrideCheckbox = X.cbox("trip-ooverride-cbox");
        this.doverrideCheckbox = X.cbox("trip-doverride-cbox");
        this.omaplinkManual = this.getInput("trip-omaplinkmanual-txt");
        this.dmaplinkManual = this.getInput("trip-dmaplinkmanual-txt");
        this.riderNum = X.txt("trip-ridernum-txt");
        this.riderName = X.txt("trip-ridername-txt");

        this.riderEntity = new Cfg("riderEntity");
    }

    get id(): string {
        return "trip-edit-panel";
    }

    private loadZones(): void {
        const tripZoneSel = this.getSelect("trip-zone-sel");
        this.service.v.queryCollection(RZO.getCollection("zones"), CONTEXT)
        .then((resultSet) => {
            while (tripZoneSel.options.length > 1) {
                tripZoneSel.remove(1);
            }
            while (resultSet.next()) {
                const opt = document.createElement("option");
                const zone = resultSet.getString("zone");
                opt.value = zone;
                opt.text = zone;
                tripZoneSel.add(opt);
            }
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    protected initUI(): void {
        super.initUI();

        this.loadZones();

        this.ooverrideCheckbox.addEventListener("change", (evt) => {
            this.toggleOriginOverride();
        });
        this.doverrideCheckbox.addEventListener("change", (evt) => {
            this.toggleDestOverride();
        });
    }

    initialize(): void {
        this.entity.v = RZO.getEntity("trip");
        this.service.v = RZO.getSource("db").service;
        this.riderEntity.v = RZO.getEntity("rider");
        this.initUI();
    }

    async show(panelData?: PanelData): Promise<void> {
        if (panelData) {
            if (panelData.dataType == "string") {
                this.state = await this.entity.v.load(
                    this.service.v, CONTEXT, panelData.asString);
            } else if (panelData.dataType == "State") {
                this.state = panelData.state;
            }
        } else {
            this.state = await this.entity.v.create(this.service.v);
        }
        this.fromState();
        if (this.state) {
            const ridernum_id = this.state.findField("ridernum_id");
            if (ridernum_id?.isNotNull) {
                await this.showRider(ridernum_id.asString);
            }
        }
        this.toggleUI(true);
    }

    private async showRider(ridernum_id: string): Promise<void> {
        const riderState = await this.riderEntity.v.load(
            this.service.v, CONTEXT, ridernum_id);
        this.riderNum.value = riderState.value("ridernum");
        this.riderName.value = riderState.value("name");
    }

    private toggleOriginOverride(): void {
        if (!this.ooverrideCheckbox.checked) {
            this.omaplinkManual.value = "";
        }
        this.toggleReadOnly(
            this.omaplinkManual, !this.ooverrideCheckbox.checked);
    }

    private toggleDestOverride(): void {
        if (!this.doverrideCheckbox.checked) {
            this.dmaplinkManual.value = "";
        }
        this.toggleReadOnly(
            this.dmaplinkManual, !this.doverrideCheckbox.checked);
    }

    protected fromState(): void {
        super.fromState();

        if (this.state) {
            const oIsChecked = !!this.state.value("omaplinkmanual");
            this.ooverrideCheckbox.checked = oIsChecked;
            this.toggleReadOnly(this.omaplinkManual, !oIsChecked);

            const dIsChecked = !!this.state.value("dmaplinkmanual");
            this.doverrideCheckbox.checked = dIsChecked;
            this.toggleReadOnly(this.dmaplinkManual, !dIsChecked);
        }
    }

    protected onSubmit(evt: Event): void {
        if (this.state) {
            this.validate()
            .then(() => {
                const action = this.state?.hasId() ?
                    this.entity.v.put(this.service.v, this.state!, CONTEXT) :
                    this.entity.v.post(this.service.v, this.state!, CONTEXT);
                action.then((row) => {
                    TOASTER.info(`Saved: ${row.getString("_id")}`);
                    this.controller.v.pop();
                })
                .catch((err) => {
                    console.error(err);
                    TOASTER.error(`ERROR: ${err}`);
                });
            })
            .catch((err) => {
                this.form.reportValidity();
            });
        }
    }

}

