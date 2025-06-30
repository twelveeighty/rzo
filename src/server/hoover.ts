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
    ClassSpec, TypeCfg, IConfiguration, DaemonWorker, _IError, Cfg, Logger,
    ServiceSource, Entity
} from "../base/core.js";

import { LeaderElector } from "./election.js";

class HooverError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export interface IHooverService {
    get isHooverService(): boolean;
    hoover(logger: Logger, entity: Entity): Promise<void>;
}


type HooverSpec = ClassSpec & {
    source: string;
    leaderElector: string;
    checkFrequency: number;
}

export class HooverWorker extends DaemonWorker {
    readonly name: string;
    private _checkId: NodeJS.Timeout | null;
    private _leader: boolean;
    logger: Logger;
    source: Cfg<IHooverService>;
    leaderElector: Cfg<LeaderElector>;
    checkFrequency: number;
    targets: Entity[];

    constructor(config: TypeCfg<HooverSpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.checkFrequency = config.spec.checkFrequency;
        if (this.checkFrequency < 1000) {
            throw new HooverError(
                `Invalid HooverWorker configuration ${this.name}, one or more ` +
                `time/frequency values is below 1000 ms`);
        }
        this._checkId = null;
        this._leader = false;
        this.source = new Cfg(config.spec.source);
        this.leaderElector = new Cfg(config.spec.leaderElector);
        this.targets = [];
        this.logger = new Logger(`hoover/${this.name}`);
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.leaderElector.setIfCast(
            `Invalid Cache: leaderElector `,
            configuration.workers.get(this.leaderElector.name),
            LeaderElector);
        this.leaderElector.v.onChange((leader) => {
            this._leader = leader;
        });
        const hooverService: unknown =
            (<ServiceSource>configuration.getSource(
                this.source.name).ensure(ServiceSource)).service;
        if (!((<any>hooverService).isHooverService)) {
            throw new HooverError(
                `Invalid HooverWorker: ${this.name}: ` +
                `source ${this.source.name} is not an IHooverService`);
        }
        for (const entity of configuration.entities.values()) {
            if (!entity.immutable && entity.retention.style == "temporary") {
                this.targets.push(entity);
            }
        }
        this.source.v = <IHooverService>hooverService;
        configuration.registerAsyncTask(this);
    }

    private schedule(): void {
        this._checkId = setTimeout(() => {
            this.vacuum();
        }, this.checkFrequency);
    }

    private vacuum(): void {
        if (this._leader) {
            const results: Promise<void>[] = [];
            for (const entity of this.targets) {
                results.push(
                    this.source.v.hoover(this.logger, entity));
            }
            Promise.allSettled(results)
            .then((outcomes) => {
                for (const outcome of outcomes) {
                    if (outcome.status == "rejected") {
                        this.logger.exc(outcome.reason);
                    }
                }
                this.schedule();
            });
        } else {
            this.schedule();
        }
    }

    async start(): Promise<any> {
        this.logger.debug(
            `Hoover '${this.name}' monitoring ${this.targets.length} entities`);
        super.start();
        this.schedule();
        console.log(
            `Hoover ${this.name} checkFrequency: ${this.checkFrequency}`);
    }

    async stop(): Promise<any> {
        if (this._checkId) {
            clearTimeout(this._checkId);
        }
        super.stop();
    }
}

