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
         Entity, Field, State, Filter, Query, Collection, Cfg, ServiceSource,
         IResultSet, BigDecimal, Row, BizTrans
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { DocumentBuilder, FinDoc } from "../../accting/acc-core.js";
import { Rider } from "../../scheduler/trip.js";
import { TOASTER } from "../toaster.js";
import {
         IPanel, ViewPanel, PanelData, PanelButton, DynElement, BasePanel
} from "../panel.js";
import { TripList } from "../trip/triplist.js";


const ACC_CHEQ_ACCOUNT = "C100.200.100";
const ACC_TICKET_PRICE = new BigDecimal("10.00");

export class RiderViewPanel extends ViewPanel implements IPanel {
    tripEntity: Cfg<Entity>;
    findocEntity: Cfg<FinDoc>;
    tripRidernumField: Cfg<Field>;
    tripsCollection: Cfg<Collection>;
    accBalanceCollection: Cfg<Collection>;
    tripList: TripList;
    createAcctsBtn: PanelButton;
    orderBtn: PanelButton;
    orderModal: Modal;
    paymentBtn: PanelButton;
    paymentModal: Modal;
    arAccBal: Row | null;
    ticketAccBal: Row | null;
    rTicketAccBal: Row | null;

    constructor() {
        super("rider", "-view-div", "-view-tsec", "-view-back-btn",
              "-view-edit-btn", "rider-edit-panel");
        this.tripList = new TripList(this.qElement("-view-trips-div"));
        this.tripEntity = new Cfg("trip");
        this.findocEntity = new Cfg("findoc");
        this.tripRidernumField = new Cfg("tripRidernumField");
        this.tripsCollection = new Cfg("trips");
        this.accBalanceCollection = new Cfg("accountbalances");
        const btnDiv = this.qElement("-acct-buttons-div");
        this.createAcctsBtn = new PanelButton(
            btnDiv, this.fqId("-view-createaccts-btn"), "Create Accounts");
        this.orderBtn = new PanelButton(
            btnDiv, this.fqId("-view-order-btn"), "Order Tickets...");
        this.paymentBtn = new PanelButton(
            btnDiv, this.fqId("-view-payment-btn"), "Process Payment...");
        this.orderModal = new Modal(this.qElement("-order-div"));
        this.paymentModal = new Modal(this.qElement("-payment-div"));
        this.arAccBal = null;
        this.ticketAccBal = null;
        this.rTicketAccBal = null;
    }

    get id(): string {
        return "rider-view-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.setIfCast(
            `'configuration error: 'Rider'`, RZO.entities.get("rider"), Rider);
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.tripEntity.v = RZO.getEntity(this.tripEntity.name);
        this.findocEntity.setIfCast("riderviewpanel",
            RZO.entities.get(this.findocEntity.name), FinDoc);
        this.tripRidernumField.v = this.tripEntity.v.getField("ridernum");
        this.tripsCollection.v = RZO.getCollection(this.tripsCollection.name);
        this.accBalanceCollection.v =
            RZO.getCollection(this.accBalanceCollection.name);
        this.qButton("-view-create-btn").addEventListener("click", (evt) => {
            this.onCreateTrip(evt);
        });
        this.tripList.initialize((evt) => {
            evt.preventDefault();
            this.onAnchorClick(evt);
        });
        this.createAcctsBtn.initialize((evt) => {
            this.onCreateAccts(evt);
        });
        this.orderBtn.initialize((evt) => {
            this.onOrder(evt);
        });
        this.qButton("-order-confirm-btn").addEventListener("click", (evt) => {
            this.onOrderOk(evt);
        });
        this.paymentBtn.initialize((evt) => {
            this.onPayment(evt);
        });
        this.qButton("-payment-confirm-btn")
        .addEventListener("click", (evt) => {
            this.onPaymentOk(evt);
        });
    }

    private onCreateTrip(evt: Event): void {
        const ridernum = State.must(this.state).value("ridernum");
        this.tripEntity.v.create(CONTEXT.c, this.service.v)
        .then((newTrip) => {
            this.tripRidernumField.v.setValue(
                newTrip, ridernum, CONTEXT.c)
            .then(() => {
                this.controller.v.stack(
                    "trip-edit-panel", new PanelData("State", newTrip));
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
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

    private async createAllAccounts(state: State): Promise<void> {
        const rider = this.entity.v.stateToRow(state);
        const bt = new BizTrans();
        await (<Rider>(this.entity.v)).createAccounts(
            bt, this.service.v, CONTEXT.c, rider);
        await this.service.v.processBizTrans(this.logger, bt);
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

    private onOrder(evt: Event): void {
        this.orderModal.show();
    }

    private async processTicketOrder(qty: string,
                                     paid: boolean): Promise<void> {
        if (this.state && this.ticketAccBal && this.arAccBal) {
            const quantity = new BigDecimal(qty);
            const amount = quantity.multiply(ACC_TICKET_PRICE);
            const now = new Date();
            const creditAccName = this.ticketAccBal.getString("account");
            const debitAccName = paid ? ACC_CHEQ_ACCOUNT :
                this.arAccBal.getString("account");
            const memo =
                `Ticket purchase for ${this.state.asString("ridernum")}`;
            const bt = new BizTrans();
            const builder = new DocumentBuilder(
                this.findocEntity.v, bt, CONTEXT.c, this.service.v);
            await builder.document(new Row(
                {
                    account: creditAccName,
                    entityid: State.must(this.state).id,
                    memo: memo,
                    posted: now,
                    created: now
                }
            ));
            await builder.split(new Row(
                {
                    account: creditAccName,
                    change: "Cr",
                    quantity: quantity,
                    price: ACC_TICKET_PRICE,
                    amount: amount,
                    memo: memo
                }
            ));
            await builder.split(new Row(
                {
                    account: debitAccName,
                    change: "Dr",
                    quantity: null,
                    price: null,
                    amount: amount,
                    memo: memo
                }
            ));
            await builder.postBizTrans();
            await this.service.v.processBizTrans(this.logger, bt);
        }
    }

    private onOrderOk(evt: Event): void {
        this.orderModal.hide();
        const qty = this.qInput("-order-qty-txt").value;
        if (qty) {
            this.processTicketOrder(
                qty, this.qInput("-order-paid-cbox").checked)
            .then(() => {
                this.queryAccting();
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
        } else {
            TOASTER.error("You must specify a valid quantity");
        }
    }

    private async processPayment(ridernum: string, accountBalance: Row,
                                 posted: Date,
                                 amount: BigDecimal): Promise<void> {
        const now = new Date();
        const creditAccName = accountBalance.getString("account");
        const debitAccName = ACC_CHEQ_ACCOUNT;
        const memo = `Payment received for ${ridernum}`;
        const bt = new BizTrans();
        const builder = new DocumentBuilder(
            this.findocEntity.v, bt, CONTEXT.c, this.service.v);
        await builder.document(new Row(
            {
                account: creditAccName,
                entityid: State.must(this.state).id,
                memo: memo,
                posted: now,
                created: now
            }
        ));
        await builder.split(new Row(
            {
                account: creditAccName,
                change: "Cr",
                quantity: null,
                price: null,
                amount: amount,
                memo: memo
            }
        ));
        await builder.split(new Row(
            {
                account: debitAccName,
                change: "Dr",
                quantity: null,
                price: null,
                amount: amount,
                memo: memo
            }
        ));
        await builder.postBizTrans();
        await this.service.v.processBizTrans(this.logger, bt);
    }

    private onPayment(evt: Event): void {
        if (this.arAccBal) {
            // Re-query AR accountbalance for current balance
            const filter = new Filter()
                .op("_id", "=", this.arAccBal.get("_id"));
            this.accBalanceCollection.v.query(CONTEXT.c, new Query([], filter))
            .then((resultSet) => {
                resultSet.next();
                this.qInput("-payment-posted-txt").value =
                    BasePanel.dateTimeLocalString(new Date());
                this.qInput("-payment-owing-txt").value =
                    resultSet.getString("balance");
                this.qInput("-payment-recvd-txt").value = "";
                this.paymentModal.show();
            })
            .catch((err) => {
                TOASTER.exc(err);
            });
        }
    }

    private onPaymentOk(evt: Event): void {
        this.paymentModal.hide();
        const posted = this.qInput("-payment-posted-txt").value;
        const payment = this.qInput("-payment-recvd-txt").value;
        if (posted && payment) {
            if (this.state && this.arAccBal) {
                const amount = new BigDecimal(payment);
                const postedDate = new Date(posted);
                if (!amount.equals(new BigDecimal(0))) {
                    this.processPayment(
                        this.state.asString("ridernum"), this.arAccBal,
                        postedDate, amount)
                    .then(() => {
                        this.queryAccting();
                    })
                    .catch((err) => {
                        TOASTER.exc(err);
                    });
                } else {
                    TOASTER.error(`Invalid payment: ${amount}`);
                }
            }
        } else {
            TOASTER.error("You must specify a valid Post Date and Payment");
        }
    }

    private queryTrips(): void {
        try {
            const query = new Query(
                [],
                new Filter().op(
                    "ridernum_id", "=", State.must(this.state).id),
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

    private handleAccData(rs: IResultSet): void {
        const tbody = this.qElement("-view-acct-tsec");
        tbody.innerHTML = "";
        // We expect three account balances to be returned
        if (rs.rowCount == 3) {
            this.arAccBal = rs.find(
                (row) => "AR" == row.get("entitytag")) || null;
            this.ticketAccBal = rs.find(
                (row) => "AVAILABLE" == row.get("entitytag")) || null;
            this.rTicketAccBal = rs.find(
                (row) => "RESERVED" == row.get("entitytag")) || null;
            if (this.arAccBal && this.ticketAccBal && this.rTicketAccBal) {
                this.addTableRowText(
                    tbody, "Available Tickets",
                    this.ticketAccBal.get("balance"), "asText", "text-end");
                this.addTableRowText(
                    tbody, "Reserved Tickets",
                    this.rTicketAccBal.get("balance"), "asText", "text-end");
                this.addTableRowText(
                    tbody, "Balance owing",
                    this.arAccBal.get("balance"), "asText", "text-end");
                this.createAcctsBtn.hide();
                this.orderBtn.show();
                this.paymentBtn.show();
            } else {
                TOASTER.error(
                    "ERROR: Missing one of the expected account balances");
            }
        } else {
            this.addTableRowText(
                tbody, "NOTE", "Accounts not yet set up for this rider");
            this.createAcctsBtn.show();
            this.createAcctsBtn.enabled = true;
            this.orderBtn.hide();
            this.paymentBtn.hide();
        }
    }

    private queryAccting(): void {
        this.arAccBal = null;
        this.ticketAccBal = null;
        this.rTicketAccBal = null;
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

    protected stateToUI(state: State): void {
        this.tbody.innerHTML = "";
        this.addRowText("Rider", state.value("name"));
        this.addRowText("Rider Num", state.value("ridernum"));
        this.addRowText("Status", state.value("status"));
        this.addRowText("Zone", state.value("zone"));
        this.addTableRowElement(
            this.tbody, "Address",
            this.addressMapAnchor(state.value("address1"),
                                  state.value("maplink")));
        this.addRowText("Address2", state.asString("address2"));
        this.addRowText("City", state.asString("city"));
        this.addRowText("Prov/State", state.asString("stateprov"));
        this.addRowText("Zip", state.asString("postalcode"));
        this.addRowText(state.asString("phone1label"),
                        state.value("phone1"));
        this.addRowText(state.asString("phone2label"),
                        state.asString("phone2"));
        this.addRowText(state.asString("phone3label"),
                        state.asString("phone3"));
        this.addRowText("Comments", state.asString("comments"), "asHTML");
        this.addRowText("Trip Comments", state.asString("tripinfo"), "asHTML");
        this.addTableRowElement(this.tbody, "DB Id",
            new DynElement({ tag: "samp", text: state.id }).asElement());
        this.addTableRowElement(this.tbody, "DB Version",
            new DynElement({ tag: "samp", text: state.rev }).asElement());
        this.queryTrips();
        this.queryAccting();
    }
}

