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
    TypeCfg, ClassSpec, EntitySpec, Entity, Field, Source, Persona, Nobody,
    Collection, AsyncTask, DaemonWorker, IConfiguration, IContext,
    IPolicyConfiguration, Authenticator, Logger, LogLevel, LogThreshold
} from "./core.js";

import { ClassInfo, Reflection } from "./reflect.js";

class ConfigError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }
}

type LoggerConfigSpec = {
    name: string;
    level: LogLevel;
}

type LoggerSpec = {
    name: string;
    level: LogThreshold;
}

type LogConfigurationSpec = ClassSpec & {
    defaultLevel: LogLevel;
    loggers: LoggerConfigSpec[];
}

type ReflectableClass<Target> = { new(config: TypeCfg<ClassSpec>,
                         blueprints: Map<string, any>): Target; };

type EntityClass = { new(config: TypeCfg<ClassSpec>,
                         blueprints: Map<string, any>): Entity; };

type SourceClass = { new(config: TypeCfg<ClassSpec>,
                         blueprints: Map<string, any>): Source; };

type AuthenticatorClass = { new(config: TypeCfg<ClassSpec>,
                         blueprints: Map<string, any>): Authenticator; };

type PersonaClass = { new(config: TypeCfg<ClassSpec>,
                          blueprints: Map<string, any>): Persona; };

type CollectionClass = { new(config: TypeCfg<ClassSpec>,
                          blueprints: Map<string, any>): Collection; };

type DaemonWorkerClass = { new(config: TypeCfg<ClassSpec>,
                          blueprints: Map<string, any>): DaemonWorker; };

class LogConfiguration {
    name: string;
    defaultThreshold: LogThreshold;
    loggers: LoggerSpec[];

    constructor() {
        this.name = "";
        this.defaultThreshold = 0;
        this.loggers = [];
    }

    addLogger(spec: LoggerConfigSpec): void {
        this.loggers.push({
            name: spec.name, level: Logger.toThreshold(spec.level)
        });
    }

    thresholdFor(name: string): LogThreshold {
        for (const logger of this.loggers) {
            if (name.startsWith(logger.name)) {
                return logger.level;
            }
        }
        return this.defaultThreshold;
    }

    sort(): void {
        this.loggers.sort((a, b) => {
            if (a.name == b.name) {
                return 0;
            } else if (a.name < b.name) {
                return 1;
            } else {
                return -1;
            }
        });
    }
}

/* Bootstraps the metadata configuration.
 */
export class Configuration implements IConfiguration {
    entities: Map<string, Entity>;
    sources: Map<string, Source>;
    authenticators: Map<string, Authenticator>;
    personas: Map<string, Persona>;
    collections: Map<string, Collection>;
    workers: Map<string, DaemonWorker>;
    classes: Map<string, any>;
    reflection: Reflection;
    json_config: TypeCfg<ClassSpec>[];
    asyncTasks: AsyncTask[];
    _policyConfig?: IPolicyConfiguration;
    logConfiguration: LogConfiguration;

    constructor() {
        this.json_config = [];
        this.entities = new Map();
        this.sources = new Map();
        this.authenticators = new Map();
        this.personas = new Map();
        this.collections = new Map();
        this.workers = new Map();
        this.reflection = new Reflection();
        this.classes = new Map();
        this.asyncTasks = [];
        this.logConfiguration = new LogConfiguration();
    }

    private addClass(className: string, classNames: string[]): void {
        if (!classNames.includes(className)) {
            classNames.push(className);
        }
    }

    async reflectAllClasses() {
        const classNames:string[] = [];
        for (const config_j of this.json_config) {
            if (!config_j) {
                break;
            }
            if (config_j.kind == "LogConfiguration") {
                continue;
            }
            // console.log(`Object: ${config_j.metadata.name}`);
            this.addClass(config_j.spec.type, classNames);
            if (config_j.kind == "Entity") {
                const spec = <EntitySpec>config_j.spec;
                // find the Field classes
                for (const keyField of spec.keyFields) {
                    this.addClass(keyField.type, classNames);
                }
                for (const keyField of spec.coreFields) {
                    this.addClass(keyField.type, classNames);
                }
            }
        }
        console.log(`Reflecting ${classNames.length} classes...`);
        // Start reflection of all classes
        const classPromises: Promise<ClassInfo>[] = [];
        for (const className of classNames) {
            classPromises.push(this.reflection.reflect(className));
        }
        try {
            const allClasses = await Promise.all(classPromises);
            for (const resolvedClass of allClasses) {
                const fqn = resolvedClass.name;
                const clazz = resolvedClass.clazz;
                this.classes.set(fqn, clazz);
            }
            console.log("Reflection completed");
        } catch (error) {
            throw new ConfigError("Reflection error", { cause: error });
        }
    }

    instantiate<Target, ReflectClass extends ReflectableClass<Target>>
                             (config: TypeCfg<ClassSpec>,
                              collection: Map<string, Target>): void {
        const instanceName = config.metadata.name;
        // check dupes
        if (collection.has(instanceName)) {
            throw new ConfigError(`${instanceName} is duplicated`);
        }
        const className = config.spec.type;
        const clazz = this.classes.get(className);
        // Instantiate the Entity class.
        // console.log(`instantiating ${className}: ${instanceName}`);
        const instanceClazz = <ReflectClass>clazz;
        const instance = new instanceClazz(config, this.classes);
        collection.set(instanceName, instance);
    }

    setupLogging(config: TypeCfg<LogConfigurationSpec>): void {
        this.logConfiguration.name = config.metadata.name;
        this.logConfiguration.defaultThreshold = Logger.toThreshold(
            config.spec.defaultLevel);
        for (const loggerSpec of config.spec.loggers) {
            this.logConfiguration.addLogger(loggerSpec);
        }
    }

    getLogThreshold(name: string): LogThreshold {
        return this.logConfiguration.thresholdFor(name);
    }

    async parse() {
        await this.reflectAllClasses();
        // Create the Entities.
        for (const config_j of this.json_config) {
            if (!config_j) {
                break;
            }
            switch (config_j.kind) {
                case "Entity":
                    this.instantiate<Entity,EntityClass>(
                        config_j, this.entities);
                    break;
                case "Source":
                    this.instantiate<Source,SourceClass>(
                        config_j, this.sources);
                    break;
                case "Authenticator":
                    this.instantiate<Authenticator,AuthenticatorClass>(
                        config_j, this.authenticators);
                    break;
                case "Persona":
                    this.instantiate<Persona,PersonaClass>(
                        config_j, this.personas);
                    break;
                case "Collection":
                    this.instantiate<Collection,CollectionClass>(
                        config_j, this.collections);
                    break;
                case "Worker":
                    this.instantiate<DaemonWorker,DaemonWorkerClass>(
                        config_j, this.workers);
                    break;
                case "LogConfiguration":
                    this.setupLogging(
                        config_j as TypeCfg<LogConfigurationSpec>);
                    break;
            }
        }
    }

    getPersona(name: string): Persona {
        const persona = this.personas.get(name);
        if (persona) {
            return persona;
        }
        throw new ConfigError(`No such persona: ${name}`);
    }

    getCollection(name: string): Collection {
        const collection = this.collections.get(name);
        if (collection) {
            return collection;
        }
        throw new ConfigError(`No such collection: ${name}`);
    }

    getEntity(name: string): Entity {
        const entity = this.entities.get(name);
        if (entity) {
            return entity;
        }
        throw new ConfigError(`No such entity: ${name}`);
    }

    getField(fqName: string): Field {
        const split = fqName.split(".");
        if (split.length != 2) {
            throw new ConfigError(`Invalid qualified field name: ${fqName}`);
        }
        const entity = this.entities.get(split[0]);
        if (!entity) {
            throw new ConfigError(`No such entity: ${name}`);
        }
        return entity.getField(split[1]);
    }

    getSource(name: string): Source {
        const source = this.sources.get(name);
        if (source) {
            return source;
        }
        throw new ConfigError(`No such source: ${name}`);
    }

    getAuthenticator(name: string): Authenticator {
        const authenticator = this.authenticators.get(name);
        if (authenticator) {
            return authenticator;
        }
        throw new ConfigError(`No such authenticator: ${name}`);
    }

    registerAsyncTask(task: AsyncTask): void {
        this.asyncTasks.push(task);
    }

    async startAsyncTasks(): Promise<void> {
        // Start all async tasks
        const taskPromises: Promise<any>[] = [];
        for (const task of this.asyncTasks) {
            taskPromises.push(task.start());
        }
        try {
            await Promise.all(taskPromises);
        } catch (error) {
            throw new ConfigError("Async startup error", { cause: error });
        }
    }

    async stopAsyncTasks(): Promise<void> {
        // Stop all async tasks
        const taskPromises: Promise<any>[] = [];
        for (const task of this.asyncTasks) {
            taskPromises.push(task.stop());
        }
        try {
            await Promise.all(taskPromises);
        } catch (error) {
            throw new ConfigError("Async shutdown error", { cause: error });
        }
    }

    set policyConfig(policyConfig: IPolicyConfiguration) {
        this._policyConfig = policyConfig;
    }

    get policyConfig(): IPolicyConfiguration | undefined {
        return this._policyConfig;
    }

    private configure(): void {
        for (const entity of this.entities.values()) {
            entity.configure(this);
        }
        for (const source of this.sources.values()) {
            source.configure(this);
        }
        for (const authenticator of this.authenticators.values()) {
            authenticator.configure(this);
        }
        for (const persona of this.personas.values()) {
            persona.configure(this);
        }
        for (const collection of this.collections.values()) {
            collection.configure(this);
        }
        for (const worker of this.workers.values()) {
            worker.configure(this);
        }
    }

    private async performBootstrap() {
        await this.parse();
        this.configure();
        this.logConfiguration.sort();
        console.log(this.logConfiguration.loggers);
    }

    async bootstrap(config: TypeCfg<ClassSpec>[]) {
        this.json_config = this.json_config.concat(config);
        await this.performBootstrap();
    }

    async load(streams: string[]) {
        for (const stream of streams) {
            const configPart = JSON.parse(stream) as TypeCfg<ClassSpec>[];
            this.json_config = this.json_config.concat(configPart);
        }
        await this.performBootstrap();
    }

    save(): string {
        return JSON.stringify(this.json_config);
    }
}

class NobodyContext implements IContext {
    sessionId?: string;
    persona: Persona;
    userAccountId: string;

    constructor() {
        this.persona = Nobody.INSTANCE;
        this.userAccountId = Nobody.ID;
    }

    getSubject(key: string): string {
        return "";
    }
}

class ClientContext {
    session: IContext;

    constructor() {
        this.session = new NobodyContext();
    }

    reset(): void {
        this.session = new NobodyContext();
    }
}

export const VERSION = "1.0.0";
export const RZO = new Configuration();
export const NOCONTEXT = new NobodyContext();
export const CONTEXT = new ClientContext();

