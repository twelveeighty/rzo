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

import { Modal } from "bootstrap";
import {
         Entity, State, Filter, Query, Collection, Cfg, ServiceSource, Row,
         IResultSet, BizTrans, BigDecimal
} from "../../base/core.js";
import { DocumentBuilder, FinDoc } from "../../accting/acc-core.js";
import { Driver } from "../../scheduler/trip.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { TOASTER } from "../toaster.js";
import {
         IPanel, ViewPanel, PanelData, DynElement, PanelButton
} from "../panel.js";
import { TripList } from "../trip/triplist.js";

const ACC_CHEQ_ACCOUNT = "C100.200.100";
const ACC_TICKET_PRICE = new BigDecimal("10.00");

export class DriverViewPanel extends ViewPanel implements IPanel {
    tripEntity: Cfg<Entity>;
    tripsCollection: Cfg<Collection>;
    accBalanceCollection: Cfg<Collection>;
    findocEntity: Cfg<FinDoc>;
    tripList: TripList;
    owedAccBal: Row | null;
    expensedAccBal: Row | null;
    createAcctsBtn: PanelButton;
    reimburseBtn: PanelButton;
    reimburseModal: Modal;

    constructor() {
        super("driver", "-view-div", "-view-tsec", "-view-back-btn",
              "-view-edit-btn", "driver-edit-panel");
        this.tripList = new TripList(this.qElement("-view-trips-div"));
        this.tripEntity = new Cfg("tripEntity");
        this.tripsCollection = new Cfg("tripsCollection");
        const btnDiv = this.qElement("-acct-buttons-div");
        this.createAcctsBtn = new PanelButton(
            btnDiv, this.fqId("-view-createaccts-btn"), "Create Accounts");
        this.reimburseBtn = new PanelButton(
            btnDiv, this.fqId("-view-reimburse-btn"), "Reimburse Mileage...");
        this.accBalanceCollection = new Cfg("accountbalances");
        this.findocEntity = new Cfg("findoc");
        this.reimburseModal = new Modal(this.qElement("-reimburse-div"));
        this.owedAccBal = null;
        this.expensedAccBal = null;
    }

    get id(): string {
        return "driver-view-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("driver");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.tripEntity.v = RZO.getEntity("trip");
        this.tripsCollection.v = RZO.getCollection("trips");
        this.accBalanceCollection.v =
            RZO.getCollection(this.accBalanceCollection.name);
        this.findocEntity.setIfCast("driverviewpanel",
            RZO.entities.get(this.findocEntity.name), FinDoc);
        this.createAcctsBtn.initialize((evt) => {
            this.onCreateAccts(evt);
        });
        this.tripList.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
        this.reimburseBtn.initialize((evt) => {
            this.onReimbursement(evt);
        });
        this.qInput("-reimburse-qty-txt").addEventListener("blur", (evt) => {
            this.onQtyBlur(evt);
        });
        this.qButton("-reimburse-confirm-btn")
        .addEventListener("click", (evt) => {
            this.onReimbursementOk(evt);
        });
    }

    private async createAllAccounts(state: State): Promise<void> {
        const driver = this.entity.v.stateToRow(state);
        const bt = new BizTrans();
        await (<Driver>(this.entity.v)).createAccounts(
            bt, this.service.v, CONTEXT.c, driver);
        await this.service.v.processBizTrans(this.logger, bt);
    }

    private onQtyBlur(evt: Event): void {
        const input = this.qInput("-reimburse-qty-txt");
        input.setCustomValidity("");
        try {
            this.setQuantity();
        } catch (err) {
            input.reportValidity();
        }
    }

    private calculateReimbursedTotal(): BigDecimal {
        const qtyStr = this.qInput("-reimburse-qty-txt").value;
        if (qtyStr) {
            const qty = new BigDecimal(qtyStr);
            const price = new BigDecimal(
                this.qInput("-reimburse-price-txt").value);
            const amount = qty.multiply(price);
            this.qInput("-reimburse-total-txt").value =
                amount.toNumeric(12,2);
            return amount;
        } else {
            this.qInput("-reimburse-total-txt").value = "0.00";
            return BigDecimal.zero();
        }
    }

    private setQuantity(): void {
        try {
            if (this.calculateReimbursedTotal().lte(BigDecimal.ZERO)) {
                throw new Error("Total amount must be larger than zero");
            }
        } catch (err) {
            this.qInput("-reimburse-qty-txt").setCustomValidity(`${err}`);
            throw err;
        }
    }

    private async processReimbursement(driverId: string, driverNum: string,
                                       owedAcct: string, posted: Date,
                                       qty: BigDecimal, price: BigDecimal,
                                       amount: BigDecimal): Promise<void> {
        const now = new Date();
        const bt = new BizTrans();
        const dr = owedAcct;
        const cr = ACC_CHEQ_ACCOUNT;
        const memo = `Driver reimbursement for ${driverNum}`;
        const builder = new DocumentBuilder(
            this.findocEntity.v, bt, CONTEXT.c, this.service.v);
        await builder.document(new Row(
            {
                account: owedAcct,
                entityid: driverId,
                memo: memo,
                posted: posted,
                created: now
            }
        ));
        await builder.split(new Row(
            {
                account: dr,
                change: "Dr",
                quantity: qty,
                price: price,
                amount: amount,
                memo: memo
            }
        ));
        await builder.split(new Row(
            {
                account: cr,
                change: "Cr",
                quantity: qty,
                price: price,
                amount: amount,
                memo: memo
            }
        ));
        await builder.postBizTrans();
        await this.service.v.processBizTrans(this.logger, bt);
    }

    private onReimbursementOk(evt: Event): void {
        if (this.closeDialog() && this.state && this.owedAccBal) {
            const posted = new Date(this.qInput("-reimburse-posted-txt").value);
            const qty = new BigDecimal(this.qInput("-reimburse-qty-txt").value);
            const amount = qty.multiply(ACC_TICKET_PRICE);
            if (amount.gt(BigDecimal.ZERO)) {
                this.processReimbursement(
                    this.state.id, this.state.asString("drivernum"),
                    this.owedAccBal.get("account"), posted, qty,
                    ACC_TICKET_PRICE, amount)
                .then(() => {
                    this.queryAccting();
                })
                .catch((err) => {
                    TOASTER.exc(`ERROR: ${err}`);
                });
            } else {
                TOASTER.error("Total reimbursement must be larger than zero");
            }
        }
    }

    private closeDialog(): boolean {
        try {
            this.setQuantity();
            this.reimburseModal.hide();
            return true;
        } catch (err) {
            this.qForm("-reimburse-form").reportValidity();
        }
        return false;
    }

    private onReimbursement(evt: Event): void {
        if (this.owedAccBal) {
            // Re-query AP accountbalance for current balance
            const filter = new Filter()
                .op("_id", "=", this.owedAccBal.get("_id"));
            this.accBalanceCollection.v.query(CONTEXT.c, new Query([], filter))
            .then((rs) => {
                this.qInput("-reimburse-price-txt").value =
                    ACC_TICKET_PRICE.toNumeric(12, 2);
                this.qInput("-reimburse-posted-txt").value =
                    (new Date()).toISOString().slice(0, 10);
                rs.next();
                this.qInput("-reimburse-qty-txt").value = rs.get("balance");
                this.reimburseModal.show();
                this.calculateReimbursedTotal();
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
        }
    }

    private onCreateAccts(evt: Event): void {
        this.createAcctsBtn.enabled = false;
        this.createAllAccounts(State.must(this.state))
        .then(() => {
            this.queryAccting();
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as HTMLElement;
        const id = target.dataset["id"];
        if (id) {
            this.controller.v.stack(
                "trip-view-panel", new PanelData("string", id));
        }
    }

    private handleAccData(rs: IResultSet): void {
        const tbody = this.qElement("-view-acct-tsec");
        tbody.innerHTML = "";
        // We expect two account balances to be returned
        if (rs.rowCount == 2) {
            this.owedAccBal = rs.find(
                (row) => "OWED" == row.get("entitytag")) || null;
            this.expensedAccBal = rs.find(
                (row) => "EXPENSED" == row.get("entitytag")) || null;
            if (this.owedAccBal && this.expensedAccBal) {
                this.addTableRowText(
                    tbody, "Owed Mileage",
                    this.owedAccBal.get("balance"), "asText", "text-end");
                this.addTableRowText(
                    tbody, "Expensed Mileage",
                    this.expensedAccBal.get("balance"), "asText", "text-end");
                this.createAcctsBtn.hide();
                this.reimburseBtn.show();
            } else {
                TOASTER.error(
                    "ERROR: Missing one of the expected account balances");
            }
        } else {
            this.addTableRowText(
                tbody, "NOTE", "Accounts not yet set up for this driver");
            this.createAcctsBtn.show();
            this.createAcctsBtn.enabled = true;
            this.reimburseBtn.hide();
        }
    }

    private queryAccting(): void {
        this.owedAccBal = null;
        this.expensedAccBal = null;
        // Query all account balances for this rider
        const filter = new Filter()
            .op("entityid", "=", State.must(this.state).id);
        this.accBalanceCollection.v.query(CONTEXT.c, new Query([], filter))
        .then((rs) => {
            this.handleAccData(rs);
        })
        .catch((err) => {
            TOASTER.exc(err);
        });
    }

    private queryTrips(): void {
        if (!this.state) {
            return;
        }
        try {
            const query = new Query(
                [],
                new Filter().op(
                    "drivernum_id", "=", this.state.id),
                [ { field: "appointmentts", order: "desc" } ]
            );
            this.tripsCollection.v.query(CONTEXT.c, query)
            .then((resultSet) => {
                this.tripList.render(resultSet);
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
        } catch (err) {
            TOASTER.exc(err);
        }
    }

    protected stateToUI(state: State): void {
        this.tbody.innerHTML = "";
        this.addRowText("Driver", state.asString("name"));
        this.addRowText("Driver Num",
                     state.asString("drivernum"));
        this.addRowText("Status", state.asString("status"));
        this.addTableRowElement(
            this.tbody, "Address",
            this.addressMapAnchor(state.asString("address1"),
                                  state.asString("maplink")));
        this.addRowText("Address2", state.asString("address2"));
        this.addRowText("City", state.asString("city"));
        this.addRowText("Prov/State", state.asString("stateprov"));
        this.addRowText("Zip", state.asString("postalcode"));
        this.addRowText(state.asString("phone1label"),
                        state.asString("phone1"));
        this.addRowText(state.asString("phone2label"),
                        state.asString("phone2"));
        this.addRowText(state.asString("phone3label"),
                        state.asString("phone3"));
        this.addTableRowElement(this.tbody, "DB Id",
            new DynElement({ tag: "samp", text: state.id }).asElement());
        this.addTableRowElement(this.tbody, "DB Version",
            new DynElement({ tag: "samp", text: state.rev }).asElement());
        this.queryTrips();
        this.queryAccting();
    }
}

