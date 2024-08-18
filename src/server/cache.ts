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
    ClassSpec, TypeCfg, IConfiguration, DaemonWorker, _IError, Cfg, Logger
} from "../base/core.js";

import { ISessionBackendService } from "../base/session.js";

import { LeaderElector } from "./election.js";

class CacheError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export interface ICache {
    get isCache(): boolean;
    set(key: string, value: any): void;
    get(key: string): any | null;
    has(key: string): boolean;
    delete(key: string): void;
}

type APICacheWorkerSpec = ClassSpec & {
    ttl: number;
    cacheCheckFrequency: number;
}

type CacheWorkerSpec = APICacheWorkerSpec & {
    leaderElector: string;
    sessionBackendSource: string;
    backendCheckFrequency: number;
}

export type CacheEntry = {
    value: any;
    expires: Date;
}

export class APICacheWorker extends DaemonWorker implements ICache {
    readonly name: string;
    readonly ttl: number;
    readonly cacheCheckFrequency: number;
    private _cacheCheckId: NodeJS.Timeout | null;
    private _cache: Map<string, CacheEntry>;
    logger: Logger;

    constructor(config: TypeCfg<APICacheWorkerSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.ttl = config.spec.ttl;
        this.cacheCheckFrequency = config.spec.cacheCheckFrequency;
        if (this.ttl < 1000 || this.cacheCheckFrequency < 1000) {
            throw new CacheError(
                `Invalid CacheWorker configuration ${this.name}, one or more ` +
                `time/frequency values is below 1000`);
        }
        this._cacheCheckId = null;
        this._cache = new Map();
        this.logger = new Logger(`cache/${this.name}`);
    }

    get isCache(): boolean {
        return true;
    }

    configure(configuration: IConfiguration): void {
        this.logger.configure(configuration);
        configuration.registerAsyncTask(this);
    }

    protected updateCache(): void {
        const evicts: string[] = [];
        const now = new Date();
        for (const entry of this._cache.entries()) {
            if (entry[1].expires <= now) {
                evicts.push(entry[0]);
                this.logger.debug(
                    `Cache ${this.name} evicting key '${entry[0]}'`);
            }
        }
        for (const key of evicts) {
            this._cache.delete(key);
        }
    }

    async start(): Promise<any> {
        this._cacheCheckId = setInterval(() => {
            this.updateCache();
        }, this.cacheCheckFrequency);
        this.logger.log(
            `Cache ${this.name} started with cacheCheckFrequency: ` +
            `${this.cacheCheckFrequency}; ttl = ${this.ttl}`);
    }

    async stop(): Promise<any> {
        if (this._cacheCheckId) {
            clearInterval(this._cacheCheckId);
        }
        this.logger.log(`Cache ${this.name} stopped`);
    }

    set(key: string, value: any): CacheEntry {
        const entry =
            { value: value, expires: new Date(Date.now() + this.ttl) };
        this._cache.set(key, entry);
        return entry;
    }

    get(key: string): any | null {
        const entry = this.getEntry(key);
        if (entry) {
            return entry.value;
        }
        return null;
    }

    getEntry(key: string): CacheEntry | null {
        const entry = this._cache.get(key);
        if (entry) {
            return entry;
        }
        return null;
    }

    has(key: string): boolean {
        return this._cache.has(key);
    }

    delete(key: string): void {
        this._cache.delete(key);
    }
}

export class CacheWorker extends APICacheWorker implements ICache {
    readonly backendCheckFrequency: number;
    private _backendCheckId: NodeJS.Timeout | null;
    leaderElector: Cfg<LeaderElector>;
    sessionBackend: Cfg<ISessionBackendService>;
    private _leader: boolean;

    constructor(config: TypeCfg<CacheWorkerSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.backendCheckFrequency = config.spec.backendCheckFrequency;
        if (this.backendCheckFrequency < 1000) {
            throw new CacheError(
                `Invalid CacheWorker configuration ${this.name}, one or more ` +
                `time/frequency values is below 1000`);
        }
        this._backendCheckId = null;
        this._leader = false;
        this.leaderElector = new Cfg(config.spec.leaderElector);
        this.sessionBackend = new Cfg(config.spec.sessionBackendSource);
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
        const sessionBackendService: unknown =
            configuration.getSource(this.sessionBackend.name).service;
        if (!((<any>sessionBackendService).isSessionBackendService)) {
            throw new CacheError(
                `Invalid CacheWorker: ${this.name}: ` +
                `sessionBackendSource ${this.sessionBackend.name} is not an ` +
                `ISessionBackendService`);
        }
        this.sessionBackend.v = <ISessionBackendService>sessionBackendService;
        configuration.registerAsyncTask(this);
    }

    protected updateBackend(): void {
        if (this._leader) {
            this.sessionBackend.v.deleteSessionsUpTo(this.logger, new Date());
        }
    }

    async start(): Promise<any> {
        super.start();
        this._backendCheckId = setInterval(() => {
            this.updateBackend();
        }, this.backendCheckFrequency);
        console.log(
            `Cache ${this.name} backendCheckFrequency: ` +
            `${this.backendCheckFrequency}`);
    }

    async stop(): Promise<any> {
        if (this._backendCheckId) {
            clearInterval(this._backendCheckId);
        }
        super.stop();
    }
}

