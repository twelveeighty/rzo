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
    IResultSet, SideEffects, BigDecimal, Row, BizTrans
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";
import { Txn, AccTrans } from "../../accting/acc-core.js";
import { TOASTER } from "../toaster.js";
import {
    IPanel, ViewPanel, PanelData, PanelButton, DynElement
} from "../panel.js";
import { TripList } from "../trip/triplist.js";


const ACC_LEDGER = "CAN";
const ACC_STATUS = "ACTIVE";
const ACC_CUR_HOLDING = "CAN.CAD";
const ACC_TICKET_HOLDING = "TICKET";
const ACC_AR_PREFIX = "C100.100.";
const ACC_TICKET_PREFIX = "C300.100.";
const ACC_RTICKET_PREFIX = "C300.200.";
const ACC_CHEQ_ACCOUNT = "C100.200.100";
const ACC_TICKET_PRICE = new BigDecimal("10.00");

export class RiderViewPanel extends ViewPanel implements IPanel {
    tripEntity: Cfg<Entity>;
    accountEntity: Cfg<Entity>;
    accBalanceEntity: Cfg<Entity>;
    acctransEntity: Cfg<AccTrans>;
    accsplitEntity: Cfg<Entity>;
    tripRidernumField: Cfg<Field>;
    tripsCollection: Cfg<Collection>;
    accBalanceCollection: Cfg<Collection>;
    tripList: TripList;
    acctbody: HTMLElement;
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
        this.accountEntity = new Cfg("account");
        this.accBalanceEntity = new Cfg("accountbalance");
        this.acctransEntity = new Cfg("acctrans");
        this.accsplitEntity = new Cfg("accsplit");
        this.tripRidernumField = new Cfg("tripRidernumField");
        this.tripsCollection = new Cfg("trips");
        this.accBalanceCollection = new Cfg("accountbalances");
        this.acctbody = this.qElement("-view-acct-tsec");
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
        this.entity.v = RZO.getEntity("rider");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        this.tripEntity.v = RZO.getEntity(this.tripEntity.name);
        this.accountEntity.v = RZO.getEntity(this.accountEntity.name);
        this.accBalanceEntity.v = RZO.getEntity(this.accBalanceEntity.name);
        this.acctransEntity.setIfCast(
            "riderviewpanel",
            RZO.entities.get(this.acctransEntity.name),
            AccTrans);
        this.accsplitEntity.v = RZO.getEntity(this.accsplitEntity.name);
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
                TOASTER.error(`ERROR: ${err}`);
            });
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
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

    private async createAccount(entityId: string, name: string,
                                elementType: string, description: string,
                                holding: string)
                                    : Promise<State> {
        const state = await this.accountEntity.v.create(
            CONTEXT.c, this.service.v);
        const validations: Promise<SideEffects>[] = [];
        validations.push(this.accountEntity.v.setValue(
            state, "entityid", entityId, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "ledger", ACC_LEDGER, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "name", name, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "elementtype", elementType, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "description", description, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "holding", holding, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "status", ACC_STATUS, CONTEXT.c));
        await Promise.all(validations);
        await this.accountEntity.v.post(this.service.v, state, CONTEXT.c);
        return state;
    }

    private async createAccountBalance(name: string): Promise<State> {
        const state = await this.accBalanceEntity.v.create(
            CONTEXT.c, this.service.v);
        await this.accBalanceEntity.v.setValue(
            state, "account", name, CONTEXT.c);
        await this.accBalanceEntity.v.post(this.service.v, state, CONTEXT.c);
        return state;
    }

    private async createAccountWithBalance(entityId: string, name: string,
                                           elementType: string,
                                           description: string, holding: string)
                                               : Promise<void> {
        await this.createAccount(
            entityId, name, elementType, description, holding);
        await this.createAccountBalance(name);
    }


    private async createAllAccounts(entityId: string,
                                    ridernum: string): Promise<void> {
        const name = ridernum.replaceAll(".", "_");
        await this.createAccountWithBalance(
            entityId, ACC_AR_PREFIX + name, "ASSET",
            `${ridernum} Accounts Receivable`, ACC_CUR_HOLDING);
        await this.createAccountWithBalance(
            entityId, ACC_TICKET_PREFIX + name, "LIABILITY",
            `${ridernum} Available Tickets`, ACC_TICKET_HOLDING);
        await this.createAccountWithBalance(
            entityId, ACC_RTICKET_PREFIX + name, "LIABILITY",
            `${ridernum} Reserved Tickets`, ACC_TICKET_HOLDING);
    }

    private onCreateAccts(evt: Event): void {
        const rider = State.must(this.state);
        this.createAcctsBtn.enabled = false;
        this.createAllAccounts(rider.id, rider.asString("ridernum"))
        .then(() => {
            this.queryAccting();
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    private onOrder(evt: Event): void {
        this.orderModal.show();
    }

    private async createSplit(change: string, transnum: string,
                              account: string, quantity: BigDecimal | null,
                              amount: BigDecimal, posted: Date,
                              created: Date, memo?: string): Promise<State> {
        const split = await this.accsplitEntity.v.create(
            CONTEXT.c, this.service.v);
        const validations: Promise<SideEffects>[] = [];
        // Set acctrans without validation, since it doesn't exist yet.
        split.field("acctrans").value = transnum;
        validations.push(this.accsplitEntity.v.setValue(
            split, "account", account, CONTEXT.c));
        validations.push(this.accsplitEntity.v.setValue(
            split, "change", change, CONTEXT.c));
        if (quantity !== null) {
            validations.push(this.accsplitEntity.v.setValue(
                split, "quantity", quantity, CONTEXT.c));
            validations.push(this.accsplitEntity.v.setValue(
                split, "price", ACC_TICKET_PRICE, CONTEXT.c));
        }
        validations.push(this.accsplitEntity.v.setValue(
            split, "amount", amount, CONTEXT.c));
        validations.push(this.accsplitEntity.v.setValue(
            split, "created", created, CONTEXT.c));
        validations.push(this.accsplitEntity.v.setValue(
            split, "posted", posted, CONTEXT.c));
        if (memo) {
            validations.push(this.accsplitEntity.v.setValue(
                split, "memo", memo, CONTEXT.c));
        }
        await Promise.all(validations);
        return split;
    }

    private async createTransaction(account: string, memo: string, posted: Date,
                                    created: Date): Promise<State> {
        const state = await this.acctransEntity.v.create(
            CONTEXT.c, this.service.v);
        const validations: Promise<SideEffects>[] = [];
        validations.push(this.acctransEntity.v.setValue(
            state, "account", account, CONTEXT.c));
        validations.push(this.acctransEntity.v.setValue(
            state, "entityid", State.must(this.state).id, CONTEXT.c));
        validations.push(this.acctransEntity.v.setValue(
            state, "memo", memo, CONTEXT.c));
        validations.push(this.acctransEntity.v.setValue(
            state, "posted", posted, CONTEXT.c));
        validations.push(this.acctransEntity.v.setValue(
            state, "created", created, CONTEXT.c));
        await Promise.all(validations);
        return state;
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
            const transaction = await this.createTransaction(
                creditAccName, memo, now, now);
            const transnum = transaction.field("transnum").value;
            const txn = new Txn(this.acctransEntity.v, transaction);
            const crSplit = await this.createSplit(
                "Cr", transnum, creditAccName, quantity, amount, now, now,
                memo);
            txn.splits.push(crSplit);
            const drSplit = await this.createSplit(
                "Dr", transnum, debitAccName, null, amount, now, now, memo);
            txn.splits.push(drSplit);
            const bizTrans = new BizTrans();
            await txn.toBizTrans(
                bizTrans, this.logger, CONTEXT.c, this.service.v);
            await this.service.v.processBizTrans(this.logger, bizTrans);
        }
    }

    private onOrderOk(evt: Event): void {
        this.orderModal.hide();
        const qty = this.qInput("-order-qty-txt").value;
        if (qty) {
            this.processTicketOrder(qty,
                                    this.qInput("-order-paid-cbox").checked)
            .then(() => {
                this.queryAccting();
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
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
        const transaction = await this.createTransaction(
            creditAccName, memo, posted, now);
        const transnum = transaction.field("transnum").value;
        const txn = new Txn(this.acctransEntity.v, transaction);
        const crSplit = await this.createSplit(
            "Cr", transnum, creditAccName, null, amount, posted, now, memo);
        txn.splits.push(crSplit);
        const drSplit = await this.createSplit(
            "Dr", transnum, debitAccName, null, amount, posted, now, memo);
        txn.splits.push(drSplit);
        const bizTrans = new BizTrans();
        await txn.toBizTrans(
            bizTrans, this.logger, CONTEXT.c, this.service.v);
        await this.service.v.processBizTrans(this.logger, bizTrans);
    }

    private onPayment(evt: Event): void {
        if (this.arAccBal) {
            // Re-query AR accountbalance for current balance
            const filter = new Filter()
                .op("_id", "=", this.arAccBal.get("_id"));
            this.accBalanceCollection.v.query(CONTEXT.c, new Query([], filter))
            .then((resultSet) => {
                this.qInput("-payment-posted-txt").value =
                    (new Date()).toISOString().slice(0, 10);
                resultSet.next();
                this.qInput("-payment-owing-txt").value =
                    resultSet.getString("balance");
                this.qInput("-payment-recvd-txt").value = "";
                this.paymentModal.show();
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
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
                        TOASTER.error(`ERROR: ${err}`);
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
                TOASTER.error(`ERROR: ${err}`);
            });
        } catch (err) {
            TOASTER.error(`ERROR: ${err}`);
        }
    }

    private handleAccData(resultSet: IResultSet): void {
        // We expect three account balances to be returned
        if (resultSet.rowCount == 3) {
            this.arAccBal = resultSet.find(
                (row) => row.getString("account").startsWith(
                    ACC_AR_PREFIX)) || null;
            this.ticketAccBal = resultSet.find(
                (row) => row.getString("account").startsWith(
                    ACC_TICKET_PREFIX)) || null;
            this.rTicketAccBal = resultSet.find(
                (row) => row.getString("account").startsWith(
                    ACC_RTICKET_PREFIX)) || null;
            if (this.arAccBal && this.ticketAccBal && this.rTicketAccBal) {
                this.addTableRowText(
                    this.acctbody, "Available Tickets",
                    this.ticketAccBal.get("balance"), "asText", "text-end");
                this.addTableRowText(
                    this.acctbody, "Reserved Tickets",
                    this.rTicketAccBal.get("balance"), "asText", "text-end");
                this.addTableRowText(
                    this.acctbody, "Balance owing",
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
                this.acctbody, "NOTE",
                "Accounts not yet set up for this rider");
            this.createAcctsBtn.show();
            this.createAcctsBtn.enabled = true;
            this.orderBtn.hide();
            this.paymentBtn.hide();
        }
    }

    private queryAccting(): void {
        this.acctbody.innerHTML = "";
        this.arAccBal = null;
        this.ticketAccBal = null;
        this.rTicketAccBal = null;
        // Query all account balances for this rider
        const filter = new Filter()
            .op("entityid", "=", State.must(this.state).id);
        this.accBalanceCollection.v.query(CONTEXT.c, new Query([], filter))
        .then((resultSet) => {
            this.handleAccData(resultSet);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    protected stateToUI(state: State): void {
        this.tbody.innerHTML = "";
        this.addRowText("Rider", state.asString("name"));
        this.addRowText("Rider Num", state.asString("ridernum"));
        this.addRowText("Status", state.asString("status"));
        this.addRowText("Zone", state.asString("zone"));
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

