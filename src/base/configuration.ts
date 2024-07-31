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
    Collection, AsyncTask, DaemonWorker, IConfiguration, IContext, ICache,
    IPolicyConfiguration
} from "./core.js";

import { ClassInfo, Reflection } from "./reflect.js";

class ConfigError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }
}

type ReflectableClass<Target> = { new(config: TypeCfg<ClassSpec>,
                         blueprints: Map<string, any>): Target; };

type EntityClass = { new(config: TypeCfg<ClassSpec>,
                         blueprints: Map<string, any>): Entity; };

type SourceClass = { new(config: TypeCfg<ClassSpec>,
                         blueprints: Map<string, any>): Source; };

type PersonaClass = { new(config: TypeCfg<ClassSpec>,
                          blueprints: Map<string, any>): Persona; };

type CollectionClass = { new(config: TypeCfg<ClassSpec>,
                          blueprints: Map<string, any>): Collection; };

type DaemonWorkerClass = { new(config: TypeCfg<ClassSpec>,
                          blueprints: Map<string, any>): DaemonWorker; };

/* Bootstraps the metadata configuration.
 */
export class Configuration implements IConfiguration {
    entities: Map<string, Entity>;
    sources: Map<string, Source>;
    personas: Map<string, Persona>;
    collections: Map<string, Collection>;
    workers: Map<string, DaemonWorker>;
    classes: Map<string, any>;
    caches: Map<string, ICache>;
    reflection: Reflection;
    json_config: TypeCfg<ClassSpec>[];
    asyncTasks: AsyncTask[];
    _policyConfig?: IPolicyConfiguration;

    constructor() {
        this.json_config = [];
        this.entities = new Map();
        this.sources = new Map();
        this.personas = new Map();
        this.collections = new Map();
        this.workers = new Map();
        this.caches = new Map();
        this.reflection = new Reflection();
        this.classes = new Map();
        this.asyncTasks = [];
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

    async parse() {
        await this.reflectAllClasses();
        // Create the Entities.
        for (const config_j of this.json_config) {
            if (!config_j) {
                break;
            }
            switch (config_j.kind) {
                case "Entity":
                    this.instantiate<Entity,EntityClass>(config_j,
                                                         this.entities);
                    break;
                case "Source":
                    this.instantiate<Source,SourceClass>(config_j,
                                                         this.sources);
                    break;
                case "Persona":
                    this.instantiate<Persona,PersonaClass>(config_j,
                                                           this.personas);
                    break;
                case "Collection":
                    this.instantiate<Collection,CollectionClass>(config_j,
                                                             this.collections);
                    break;
                case "Worker":
                    this.instantiate<DaemonWorker,DaemonWorkerClass>(config_j,
                                                           this.workers);
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

class ClientContext implements IContext {
    sessionId?: string;
    persona: Persona;
    userAccountId: string;
    private subjects: Map<string, string>;

    constructor(config: IConfiguration) {
        this.persona = Nobody.INSTANCE;
        this.userAccountId = Nobody.ID;
        this.subjects = new Map();
    }

    getSubject(key: string): string {
        return this.subjects.get(key) || "";
    }

    setSubject(key: string, subject: string) {
        this.subjects.set(key, subject);
    }
}

export const RZO = new Configuration();
export const CONTEXT = new ClientContext(RZO);

