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
    IResultSet, MemResultSet, SideEffects, BigDecimal
} from "../../base/core.js";
import { RZO, CONTEXT } from "../../base/configuration.js";

import {
    IAcctingService, AcctingServiceSource, Txn
} from "../../accting/acc-core.js";

import * as X from "../common.js";
import { TOASTER } from "../toaster.js";

import {
    IPanel, ViewPanel, PanelData, PanelButton
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
    accTransEntity: Cfg<Entity>;
    accSplitEntity: Cfg<Entity>;
    tripRidernumField: Cfg<Field>;
    tripsCollection: Cfg<Collection>;
    accBalanceCollection: Cfg<Collection>;
    tripList: TripList;
    acctTBody: HTMLTableSectionElement;
    createBtn: HTMLButtonElement;
    createAcctsBtn: PanelButton;
    orderBtn: PanelButton;
    orderModal: Modal;
    orderOkBtn: HTMLButtonElement;
    orderQty: HTMLInputElement;
    orderPaid: HTMLInputElement;
    paymentBtn: PanelButton;
    paymentModal: Modal;
    paymentOkBtn: HTMLButtonElement;
    paymentDate: HTMLInputElement;
    paymentOwing: HTMLInputElement;
    paymentRecvd: HTMLInputElement;
    txnService: Cfg<IAcctingService>;

    constructor() {
        super("rider-view-div", "rider-view-tsec", "rider-view-status-sm",
             "rider-view-back-btn", "rider-view-edit-btn", "rider-edit-panel");
        this.tripList = new TripList(X.div("rider-view-trips-div"), "vtl");
        this.tripEntity = new Cfg("trip");
        this.accountEntity = new Cfg("account");
        this.accBalanceEntity = new Cfg("accountbalance");
        this.accTransEntity = new Cfg("acctrans");
        this.accSplitEntity = new Cfg("accsplit");
        this.tripRidernumField = new Cfg("tripRidernumField");
        this.tripsCollection = new Cfg("trips");
        this.accBalanceCollection = new Cfg("accountbalances");
        this.createBtn = X.btn("rider-view-create-btn");
        this.acctTBody = X.tsec("rider-view-acct-tsec");
        const btnDiv = X.div("rider-acct-buttons-div");
        this.createAcctsBtn = new PanelButton(
            btnDiv, "rider-view-createaccts-btn", "Create Accounts");
        this.orderBtn = new PanelButton(
            btnDiv, "rider-view-order-btn", "Order Tickets...");
        this.orderModal = new Modal(X.div("rider-order-div"));
        this.orderOkBtn = X.btn("rider-order-confirm-btn");
        this.orderQty = X.txt("rider-order-qty-txt");
        this.orderPaid = X.cbox("rider-order-paid-cbox");
        this.txnService = new Cfg("txndb");
        this.paymentBtn = new PanelButton(
            btnDiv, "rider-view-payment-btn", "Process Payment...");
        this.paymentModal = new Modal(X.div("rider-payment-div"));
        this.paymentOkBtn = X.btn("rider-payment-confirm-btn");
        this.paymentDate = X.txt("rider-payment-posted-txt");
        this.paymentOwing = X.txt("rider-payment-owing-txt");
        this.paymentRecvd = X.txt("rider-payment-recvd-txt");
    }

    get id(): string {
        return "rider-view-panel";
    }

    initialize(): void {
        super.initialize();
        this.entity.v = RZO.getEntity("rider");
        this.service.v =
            (<ServiceSource>RZO.getSource("db").ensure(ServiceSource)).service;
        const txnSource = RZO.getSource(this.txnService.name);
        this.txnService.v = (<AcctingServiceSource>txnSource.ensure(
                AcctingServiceSource)).service;
        this.tripEntity.v = RZO.getEntity(this.tripEntity.name);
        this.accountEntity.v = RZO.getEntity(this.accountEntity.name);
        this.accBalanceEntity.v = RZO.getEntity(this.accBalanceEntity.name);
        this.accTransEntity.v = RZO.getEntity(this.accTransEntity.name);
        this.accSplitEntity.v = RZO.getEntity(this.accSplitEntity.name);
        this.tripRidernumField.v = this.tripEntity.v.getField("ridernum");
        this.tripsCollection.v = RZO.getCollection(this.tripsCollection.name);
        this.accBalanceCollection.v =
            RZO.getCollection(this.accBalanceCollection.name);
        this.createBtn.addEventListener("click", (evt) => {
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
        this.orderOkBtn.addEventListener("click", (evt) => {
            this.onOrderOk(evt);
        });
        this.paymentBtn.initialize((evt) => {
            this.onPayment(evt);
        });
        this.paymentOkBtn.addEventListener("click", (evt) => {
            this.onPaymentOk(evt);
        });
    }

    private onCreateTrip(evt: Event): void {
        if (this.state) {
            const ridernum = this.state.value("ridernum");
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
    }

    private onAnchorClick(evt: Event): void {
        const target = evt.currentTarget as Element;
        if (target && target.id && target.id.length > 4) {
            const _id = target.id.slice(4);
            this.controller.v.stack(
                "trip-view-panel", new PanelData("string", _id));
        }
    }

    private async createAccount(name: string, elementType: string,
                                description: string, holding: string)
                                    : Promise<State> {
        const state = await this.accountEntity.v.create(
            CONTEXT.c, this.service.v);
        const validations: Promise<SideEffects>[] = [];
        validations.push(this.accountEntity.v.setValue(
            state, "ledger", ACC_LEDGER, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "name", name, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "elementtype", elementType, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "description", description, CONTEXT.c));
        validations.push(this.accountEntity.v.setValue(
            state, "holding", ACC_CUR_HOLDING, CONTEXT.c));
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

    private async createAccountWithBalance(name: string, elementType: string,
                                           description: string, holding: string)
                                               : Promise<void> {
        await this.createAccount(name, elementType, description, holding);
        await this.createAccountBalance(name);
    }


    private async createAllAccounts(ridernum: string): Promise<void> {
        await this.createAccountWithBalance(
            ACC_AR_PREFIX + ridernum, "ASSET",
            `${ridernum} Accounts Receivable`, ACC_CUR_HOLDING);
        await this.createAccountWithBalance(
            ACC_TICKET_PREFIX + ridernum, "LIABILITY",
            `${ridernum} Available Tickets`, ACC_TICKET_HOLDING);
        await this.createAccountWithBalance(
            ACC_RTICKET_PREFIX + ridernum, "LIABILITY",
            `${ridernum} Reserved Tickets`, ACC_TICKET_HOLDING);
    }

    private onCreateAccts(evt: Event): void {
        if (this.state) {
            this.createAcctsBtn.enabled = false;
            const ridernum = this.state.asString("ridernum");
            this.createAllAccounts(ridernum)
            .then(() => {
                this.queryAccting();
            })
            .catch((err) => {
                TOASTER.error(`ERROR: ${err}`);
            });
        }
    }

    private onOrder(evt: Event): void {
        this.orderModal.show();
    }

    private async createSplit(change: string, transnum: string,
                              account: string, quantity: BigDecimal | null,
                              amount: BigDecimal, posted: Date,
                              created: Date, memo?: string): Promise<State> {
        const split = await this.accSplitEntity.v.create(
            CONTEXT.c, this.service.v);
        const validations: Promise<SideEffects>[] = [];
        // Set acctrans without validation, since it doesn't exist yet.
        split.field("acctrans").value = transnum;
        validations.push(this.accSplitEntity.v.setValue(
            split, "account", account, CONTEXT.c));
        validations.push(this.accSplitEntity.v.setValue(
            split, "change", change, CONTEXT.c));
        if (quantity !== null) {
            validations.push(this.accSplitEntity.v.setValue(
                split, "quantity", quantity, CONTEXT.c));
            validations.push(this.accSplitEntity.v.setValue(
                split, "price", ACC_TICKET_PRICE, CONTEXT.c));
        }
        validations.push(this.accSplitEntity.v.setValue(
            split, "amount", amount, CONTEXT.c));
        validations.push(this.accSplitEntity.v.setValue(
            split, "created", created, CONTEXT.c));
        validations.push(this.accSplitEntity.v.setValue(
            split, "posted", posted, CONTEXT.c));
        if (memo) {
            validations.push(this.accSplitEntity.v.setValue(
                split, "memo", memo, CONTEXT.c));
        }
        await Promise.all(validations);
        return split;
    }

    private async createTransaction(account: string, memo: string, posted: Date,
                                    created: Date): Promise<State> {
        const state = await this.accTransEntity.v.create(
            CONTEXT.c, this.service.v);
        const validations: Promise<SideEffects>[] = [];
        validations.push(this.accTransEntity.v.setValue(
            state, "account", account, CONTEXT.c));
        validations.push(this.accTransEntity.v.setValue(
            state, "memo", memo, CONTEXT.c));
        validations.push(this.accTransEntity.v.setValue(
            state, "posted", posted, CONTEXT.c));
        validations.push(this.accTransEntity.v.setValue(
            state, "created", created, CONTEXT.c));
        await Promise.all(validations);
        return state;
    }

    private async processTicketOrder(ridernum: string, qty: string,
                                     paid: boolean): Promise<void> {
        const quantity = new BigDecimal(qty);
        const amount = quantity.multiply(ACC_TICKET_PRICE);
        const now = new Date();
        const creditAccName = ACC_TICKET_PREFIX + ridernum;
        const debitAccName = paid ? ACC_CHEQ_ACCOUNT : ACC_AR_PREFIX + ridernum;
        const memo = `Ticket purchase for ${ridernum}`;
        const transaction = await this.createTransaction(
            creditAccName, memo, now, now);
        const transnum = transaction.field("transnum").value;
        const splits = new MemResultSet();
        const txn: Txn = {
            transaction: this.accTransEntity.v.stateToRow(transaction),
            splits: splits
        };
        const crSplit = await this.createSplit(
            "Cr", transnum, creditAccName, quantity, amount, now, now, memo);
        splits.addRow(this.accSplitEntity.v.stateToRow(crSplit));
        const drSplit = await this.createSplit(
            "Dr", transnum, debitAccName, null, amount, now, now, memo);
        splits.addRow(this.accSplitEntity.v.stateToRow(drSplit));
        await this.txnService.v.postTxn(this.logger, CONTEXT.c, txn);
    }

    private onOrderOk(evt: Event): void {
        this.orderModal.hide();
        const qty = this.orderQty.value;
        if (qty) {
            if (!this.state) {
                return;
            }
            this.processTicketOrder(this.state.asString("ridernum"), qty,
                                   this.orderPaid.checked)
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

    private async processPayment(ridernum: string, posted: Date,
                                 amount: BigDecimal): Promise<void> {
        const now = new Date();
        const creditAccName = ACC_AR_PREFIX + ridernum;
        const debitAccName = ACC_CHEQ_ACCOUNT;
        const memo = `Payment received for ${ridernum}`;
        const transaction = await this.createTransaction(
            creditAccName, memo, posted, now);
        const transnum = transaction.field("transnum").value;
        const splits = new MemResultSet();
        const txn: Txn = {
            transaction: this.accTransEntity.v.stateToRow(transaction),
            splits: splits
        };
        const crSplit = await this.createSplit(
            "Cr", transnum, creditAccName, null, amount, posted, now, memo);
        splits.addRow(this.accSplitEntity.v.stateToRow(crSplit));
        const drSplit = await this.createSplit(
            "Dr", transnum, debitAccName, null, amount, posted, now, memo);
        splits.addRow(this.accSplitEntity.v.stateToRow(drSplit));
        await this.txnService.v.postTxn(this.logger, CONTEXT.c, txn);
    }

    private onPayment(evt: Event): void {
        if (!this.state) {
            return;
        }
        // Query AR accountbalance for current balance
        const ridernum = this.state.asString("ridernum");
        const filter = new Filter()
            .op("account", "=", ACC_AR_PREFIX + ridernum);
        this.accBalanceCollection.v.query(CONTEXT.c, new Query([], filter))
        .then((resultSet) => {
            this.paymentDate.value = (new Date()).toISOString().slice(0, 10);
            resultSet.next();
            this.paymentOwing.value = resultSet.getString("balance");
            this.paymentRecvd.value = "";
            this.paymentModal.show();
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    private onPaymentOk(evt: Event): void {
        this.paymentModal.hide();
        const posted = this.paymentDate.value;
        const payment = this.paymentRecvd.value;
        if (posted && payment) {
            if (!this.state) {
                return;
            }
            const amount = new BigDecimal(payment);
            const postedDate = new Date(posted);
            if (!amount.equals(new BigDecimal(0))) {
                this.processPayment(this.state.asString("ridernum"), postedDate,
                                    amount)
                .then(() => {
                    this.queryAccting();
                })
                .catch((err) => {
                    TOASTER.error(`ERROR: ${err}`);
                });
            } else {
                TOASTER.error(`Invalid payment: ${amount}`);
            }
        } else {
            TOASTER.error("You must specify a valid Post Date and Payment");
        }
    }

    private queryTrips(): void {
        if (!this.state) {
            return;
        }
        try {
            const query = new Query(
                [],
                new Filter().op(
                    "ridernum_id", "=", this.state.id),
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

    private handleAccData(resultSet: IResultSet, ridernum: string): void {
        // We expect three account balances to be returned
        if (resultSet.rowCount == 3) {
            const arRow = resultSet.find(
                (row) => row.get("account") == ACC_AR_PREFIX + ridernum);
            const ticketRow = resultSet.find(
                (row) => row.get("account") == ACC_TICKET_PREFIX + ridernum);
            const rTicketRow = resultSet.find(
                (row) => row.get("account") == ACC_RTICKET_PREFIX + ridernum);
            if (arRow && ticketRow && rTicketRow) {
                X.addRowText(
                    this.acctTBody, "Available Tickets",
                    ticketRow.getString("balance"));
                X.addRowText(
                    this.acctTBody, "Reserved Tickets",
                    rTicketRow.getString("balance"));
                X.addRowText(
                    this.acctTBody, "Balance owing",
                    arRow.getString("balance"));
                this.createAcctsBtn.hide();
                this.orderBtn.show();
                this.paymentBtn.show();
            } else {
                TOASTER.error(
                    "ERROR: Missing one of the expected account balances");
            }
        } else {
            X.addRowText(
                this.acctTBody, "NOTE",
                "Accounts not yet set up for this rider");
            this.createAcctsBtn.show();
            this.createAcctsBtn.enabled = true;
            this.orderBtn.hide();
            this.paymentBtn.hide();
        }
    }

    private queryAccting(): void {
        this.acctTBody.innerHTML = "";
        if (!this.state) {
            return;
        }
        // Query all three account balances
        const ridernum = this.state.asString("ridernum");
        const filter = new Filter("or")
            .op("account", "=", ACC_AR_PREFIX + ridernum)
            .op("account", "=", ACC_TICKET_PREFIX + ridernum)
            .op("account", "=", ACC_RTICKET_PREFIX + ridernum);
        this.accBalanceCollection.v.query(CONTEXT.c, new Query([], filter))
        .then((resultSet) => {
            this.handleAccData(resultSet, ridernum);
        })
        .catch((err) => {
            TOASTER.error(`ERROR: ${err}`);
        });
    }

    protected stateToUI(state: State): void {
        this.tableTBody.innerHTML = "";
        X.addRowText(this.tableTBody, "Rider", state.asString("name"));
        X.addRowText(this.tableTBody, "Rider Num", state.asString("ridernum"));
        X.addRowText(this.tableTBody, "Status", state.asString("status"));
        X.addRowText(this.tableTBody, "Zone", state.asString("zone"));
        X.addRowElement(this.tableTBody, "Address",
                    X.addressMapAnchor(state.asString("address1"),
                                      state.asString("maplink")));
        X.addRowText(this.tableTBody, "Address2", state.asString("address2"));
        X.addRowText(this.tableTBody, "City", state.asString("city"));
        X.addRowText(this.tableTBody, "Prov/State", state.asString("stateprov"));
        X.addRowText(this.tableTBody, "Zip", state.asString("postalcode"));

        X.addRowText(this.tableTBody, state.asString("phone1label"),
                     state.asString("phone1"));
        X.addRowText(this.tableTBody, state.asString("phone2label"),
                     state.asString("phone2"));
        X.addRowText(this.tableTBody, state.asString("phone3label"),
                     state.asString("phone3"));
        X.addRowText(this.tableTBody, "Comments", state.asString("comments"),
                           X.asHTML);
        X.addRowText(this.tableTBody, "Trip Comments",
                     state.asString("tripinfo"), X.asHTML);

        this.statusElement.innerText = `${state.id} / ${state.rev}`;

        this.queryTrips();
        this.queryAccting();
    }
}

