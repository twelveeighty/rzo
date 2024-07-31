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

import {
    Row, _IError, TypeCfg, ClassSpec, DaemonWorker, Cfg, IConfiguration,
    IService, Entity
} from "../base/core.js";

class ElectionError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export interface IElectorService extends IService {
    get isElectorService(): boolean;
    castBallot(serverId: string, rowId: number, interval: string): Promise<Row>;
    leaderPing(serverId: string, rowId: number): Promise<Row>;
}

type LeaderElectorSpec = ClassSpec & {
    source: string;
    leaderElectRowId: number;
    leaderCastVoteInterval: string;
    leaderCastVoteFrequency: number;
    leaderReelectFrequency: number;
}

type OnChangeCallback = (isLeader: boolean) => void;

export class LeaderElector extends DaemonWorker {
    service: Cfg<IElectorService>;
    readonly name: string;
    readonly serverId: string;
    private _spec: LeaderElectorSpec;
    private _castVoteTimerId: NodeJS.Timeout | null;
    private _reelectTimerId: NodeJS.Timeout | null;
    private _leader: boolean | null;
    private onChangeCallbacks: OnChangeCallback[];

    constructor(config: TypeCfg<LeaderElectorSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.serverId = Entity.generateId();
        this.name = config.metadata.name;
        this.onChangeCallbacks = [];
        this.service = new Cfg(config.spec.source);
        this._leader = null;
        this._spec = config.spec;
        if (this._spec.leaderCastVoteFrequency < 1000 ||
            this._spec.leaderReelectFrequency < 1000) {
            throw new ElectionError(
                `LeaderElector '${this.name}': configured frequencies must ` +
                `not be less than 1000`);
        }
        this._castVoteTimerId = null;
        this._reelectTimerId = null;
        console.log(`::Leader Election:: Server ID = ${this.serverId}`);
    }

    get leader(): boolean {
        return this._leader || false;
    }

    configure(configuration: IConfiguration): void {
        const dbSource = configuration.getSource(this.service.name);
        if (!(<any>dbSource.service).isElectorService) {
            throw new ElectionError(
                `LeaderElector '${this.name}' requires a Source that defines ` +
                `an ElectorService`);
        }
        this.service.v = dbSource.service as IElectorService;
        configuration.registerAsyncTask(this);
    }

    onChange(callback: OnChangeCallback): void {
        this.onChangeCallbacks.push(callback);
    }

    private async leaderCastVote(): Promise<void> {
        if (!this._leader) {
            const row = await this.service.v.castBallot(
                this.serverId, this._spec.leaderElectRowId,
                this._spec.leaderCastVoteInterval);
            if (row && !row.empty && row.getString("leader") == this.serverId) {
                this._leader = true;
                const lastping = row.get("lastping") as Date;
                console.log(
                    `::Leader Election:: Server ${this.serverId} has become ` +
                    `Leader at local time [${lastping.toString()}], ` +
                    `UTC: [${lastping.toUTCString()}]`);
                for (const callback of this.onChangeCallbacks) {
                    setTimeout(callback, 0, this._leader || false);
                }
            } else {
                console.log(
                    `::Leader Election:: Server ${this.serverId} is not the ` +
                    `Leader`);
            }
        }
    }

    private async leaderReelect(): Promise<void> {
        if (this._leader) {
            const row = await this.service.v.leaderPing(
                this.serverId, this._spec.leaderElectRowId);
            if (!row || row.empty || row.getString("leader") != this.serverId) {
                this._leader = false;
                const now = new Date();
                console.log(
                    `::Leader Election:: Server ${this.serverId} lost Leader ` +
                    `at local (server) time [${now.toString()}], ` +
                    `UTC: [${now.toUTCString()}]`);
                for (const callback of this.onChangeCallbacks) {
                    setTimeout(callback, 0, this._leader || false);
                }
            } else {
                const lastping = row.get("lastping") as Date;
                console.log(
                    `::Leader Election:: Server ${this.serverId} remains ` +
                    `Leader at local time [${lastping.toString()}], ` +
                    `UTC: [${lastping.toUTCString()}]`);
            }
        }
    }

    async start(): Promise<any> {
        /* We 'await' the first leaderCastVote(), since this is the likely
         * the first time we connect to the DB; and that could take a long
         * time in a hosted scenario (>10s).
         */
        await this.leaderCastVote();
        this._castVoteTimerId = setInterval(() => {
            this.leaderCastVote();
        }, this._spec.leaderCastVoteFrequency);
        console.log(
            `LeaderElection '${this.name}' CastVote loop started ` +
            `with frequency: ${this._spec.leaderCastVoteFrequency}`);
        this._reelectTimerId = setInterval(() => {
            this.leaderReelect();
        }, this._spec.leaderReelectFrequency);
        console.log(
            `LeaderElection '${this.name}' Reelect loop started ` +
            `with frequency: ${this._spec.leaderReelectFrequency}`);
    }

    async stop(): Promise<any> {
        if (this._castVoteTimerId) {
            clearInterval(this._castVoteTimerId);
            console.log(`LeaderElection '${this.name}' CastVote loop stopped`);
        }
        if (this._reelectTimerId) {
            clearInterval(this._reelectTimerId);
            console.log(`LeaderElection '${this.name}' Reelect loop stopped`);
        }
    }
}

