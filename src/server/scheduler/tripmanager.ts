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

import {
    IConfiguration, IService, DaemonWorker, TypeCfg, ClassSpec, _IError, Cfg,
    Logger, AliasValueList, Collection, Filter, IntegerField,
    ServiceSource, State, Query, BizTrans, BigDecimal
} from "../../base/core.js";
import { NOCONTEXT } from "../../base/configuration.js";
import { Trip } from "../../scheduler/trip.js";
import { LeaderElector } from "../election.js";

class TripMgrError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

type TripMgrWorkerSpec = ClassSpec & {
    source: string;
    leaderElector: string;
    /* Set extraMinutes to the number of minutes we wait after the trip's
     * 'appointmentts' to CANCEL or COMP the trip.
     */
    extraMinutes: number;
    checkFrequency: number;
}

export class TripMgrWorker extends DaemonWorker {
    readonly name: string;
    private _checkId: NodeJS.Timeout | null;
    private _leader: boolean;
    service: Cfg<IService>;
    tripEntity: Cfg<Trip>;
    statusField: Cfg<AliasValueList>;
    collection: Cfg<Collection>;
    checkFrequency: number;
    leaderElector: Cfg<LeaderElector>;
    logger: Logger;
    extraMinutes: number;
    baseFilter: Filter;

    constructor(config: TypeCfg<TripMgrWorkerSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this._leader = false;
        this.leaderElector = new Cfg(config.spec.leaderElector);
        this._checkId = null;
        this.logger = new Logger(`tripmgr/${this.name}`);
        this.extraMinutes = IntegerField.parseInteger(config.spec.extraMinutes);
        this.checkFrequency =
            IntegerField.parseInteger(config.spec.checkFrequency);
        if (this.checkFrequency < 1000) {
            throw new TripMgrError(
                `Invalid TripMgrWorker configuration ${this.name}, one ` +
                `or more time/frequency values is below 1000`);
        }
        this.service = new Cfg(config.spec.source);
        this.tripEntity = new Cfg("trip");
        this.statusField = new Cfg("trip.status");
        this.collection = new Cfg("trips");
        this.baseFilter = new Filter("or")
            .op("status", "=", "SCHED")
            .op("status", "=", "ACCEPT");
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.logger.configure(configuration);
        this.leaderElector.setIfCast(
            `Invalid TripMgrWorker: leaderElector `,
            configuration.workers.get(this.leaderElector.name),
            LeaderElector);
        this.leaderElector.v.onChange((leader) => {
            this._leader = leader;
        });
        const dbSource = configuration.getSource(
            this.service.name).ensure(ServiceSource) as ServiceSource;
        this.service.v = dbSource.service;
        this.tripEntity.setIfCast(
            `TripMgrWorker '${this.name}': `,
            configuration.entities.get(this.tripEntity.name),
            Trip);
        this.statusField.setIfCast(
            `TripMgrWorker '${this.name}': `,
            configuration.getField(this.statusField.name), AliasValueList);
        this.collection.setIf(
            `TripMgrWorker '${this.name}': `,
            configuration.collections.get(this.collection.name));
        configuration.registerAsyncTask(this);
    }

    private schedule(): void {
        this._checkId = setTimeout(() => {
            this.execute();
        }, this.checkFrequency);
    }

    private async completeReservation(bizTrans: BizTrans, now: Date,
                                      trip: State): Promise<void> {
        const row = this.tripEntity.v.stateToRow(trip);
        const id = trip.id;
        const quantity = BigDecimal.ensure(trip.value("price"));
        return this.tripEntity.v.completeReservation(
            bizTrans, this.service.v, NOCONTEXT, row, now, quantity, id);
    }

    private async run(): Promise<void> {
        const targetTS = new Date(Date.now() - this.extraMinutes*60*1000);
        const filter = this.baseFilter.and(
            new Filter().op("appointmentts", "<=", targetTS.toISOString())
        );
        const query = new Query(
            [],
            filter,
            [{field: "appointmentts", order: "asc"}]
        );
        const rs = await this.collection.v.query(NOCONTEXT, query);
        while (rs.next()) {
            const status = this.statusField.v.getInternalValue(
                rs.get("status"));
            if (this.logger.willLog("Debug")) {
                this.logger.debug(
                    `Examining Trip ${rs.get("tripnum")} with status: ` +
                    `${rs.get("status")} -> ${status}`);
            }
            if (status == "SCHED" || status == "ACCEPT") {
                const state = this.tripEntity.v.from(rs);
                const newStatus = status == "SCHED" ? "CANCEL" : "COMP";
                if (this.logger.willLog("Info")) {
                    this.logger.info(
                        `Changing status for Trip ${rs.get("tripnum")} to ` +
                        `${newStatus}`);
                }
                await this.tripEntity.v.setValue(
                    state, "status", newStatus, NOCONTEXT);
                const bt = new BizTrans();
                await this.tripEntity.v.putBizTrans(
                    bt, this.service.v, state, NOCONTEXT);
                if (newStatus == "COMP") {
                    await this.completeReservation(bt, new Date(), state);
                }
                await this.service.v.processBizTrans(this.logger, bt);
            }
        }
    }

    private execute(): void {
        if (this._leader) {
            this.run()
            .catch((err) => {
                this.logger.exc(err);
            })
            .finally(() => {
                this.schedule();
            });
        } else {
            this.schedule();
        }
    }

    async start(): Promise<any> {
        this.logger.log(
            `TripMgrWorker '${this.name}' running every ` +
            `${this.checkFrequency}`);
        super.start();
        this.schedule();
    }

    async stop(): Promise<any> {
        if (this._checkId) {
            clearInterval(this._checkId);
        }
        this.logger.log(`TripMgrWorker ${this.name} stopped`);
    }
}

