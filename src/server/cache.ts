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
    ClassSpec, TypeCfg, IConfiguration, DaemonWorker, _IError, ICache, Cfg
} from "../base/core.js";

import { ISessionBackendService } from "../base/session.js";

import { LeaderElector } from "./election.js";

class CacheError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

type CacheWorkerSpec = ClassSpec & {
    leaderElector: string;
    ttl: number;
    sessionBackendSource: string;
    cacheCheckFrequency: number;
    backendCheckFrequency: number;
}

export type CacheEntry = {
    value: any;
    expires: Date;
}

export class CacheWorker extends DaemonWorker implements ICache {
    readonly name: string;
    readonly ttl: number;
    readonly cacheCheckFrequency: number;
    readonly backendCheckFrequency: number;
    private _cacheCheckId: NodeJS.Timeout | null;
    private _backendCheckId: NodeJS.Timeout | null;
    private _cache: Map<string, CacheEntry>;
    leaderElector: Cfg<LeaderElector>;
    sessionBackend: Cfg<ISessionBackendService>;
    private _leader: boolean;

    constructor(config: TypeCfg<CacheWorkerSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.ttl = config.spec.ttl;
        this.cacheCheckFrequency = config.spec.cacheCheckFrequency;
        this.backendCheckFrequency = config.spec.backendCheckFrequency;
        if (this.ttl < 1000 || this.cacheCheckFrequency < 1000 ||
            this.backendCheckFrequency < 1000) {
            throw new CacheError(
                `Invalid CacheWorker configuration ${this.name}, one or more ` +
                `time/frequency values is below 1000`);
        }
        this._cacheCheckId = null;
        this._backendCheckId = null;
        this._leader = false;
        this._cache = new Map();
        this.leaderElector = new Cfg(config.spec.leaderElector);
        this.sessionBackend = new Cfg(config.spec.sessionBackendSource);
    }

    get isCache(): boolean {
        return true;
    }

    configure(configuration: IConfiguration): void {
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

    protected updateCache(): void {
        const evicts: string[] = [];
        const now = new Date();
        for (const entry of this._cache.entries()) {
            if (entry[1].expires <= now) {
                evicts.push(entry[0]);
            }
        }
        for (const key of evicts) {
            this._cache.delete(key);
        }
    }

    protected updateBackend(): void {
        if (this._leader) {
            this.sessionBackend.v.deleteSessionsUpTo(new Date());
        }
    }

    async start(): Promise<any> {
        this._cacheCheckId = setInterval(() => {
            this.updateCache();
        }, this.cacheCheckFrequency);
        this._backendCheckId = setInterval(() => {
            this.updateBackend();
        }, this.backendCheckFrequency);
        console.log(
            `Cache ${this.name} started with cacheCheckFrequency: ` +
            `${this.cacheCheckFrequency}; ttl = ${this.ttl} and ` +
            `backendCheckFrequency: ${this.backendCheckFrequency}`);
    }

    async stop(): Promise<any> {
        if (this._cacheCheckId) {
            clearInterval(this._cacheCheckId);
        }
        if (this._backendCheckId) {
            clearInterval(this._backendCheckId);
        }
        console.log(`Cache ${this.name} stopped`);
    }

    set(key: string, value: any): void {
        this._cache.set(key, {
            value: value,
            expires: new Date(Date.now() + this.ttl)
        });
    }

    get(key: string): any | null {
        const entry = this._cache.get(key);
        if (entry) {
            return entry.value;
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

