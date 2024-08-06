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
    ClassSpec, TypeCfg, Row, IContext, IResultSet, _IError, IConfiguration,
    PolicyAction, IPolicyConfiguration
} from "../base/core.js";

import { ClassInfo, Reflection } from "../base/reflect.js";

class PolicyError extends _IError {
    constructor(message?: string, code?: number, options?: ErrorOptions) {
        super(code || 403,
              message || "Policy violation, contact administrator", options);
    }
}

type MatchCombineAs = "any" | "all";
type MatchOperand = "eq" | "ne" | "isNull" | "isNotNull";

type MatchCondition = {
    attr: string;
    op: MatchOperand;
    subject: string;
}

type PolicyWhere = {
    match: MatchCombineAs;
    conditions: MatchCondition[];
}

type PolicyEffect = "allow" | "deny";

type PolicyStatement = {
    resource: string;
    action: PolicyAction;
    effect: PolicyEffect;
    where?: PolicyWhere;
}

type PolicyResult = {
    result: boolean;
    doBreak: boolean;
}

export type PolicySpec = ClassSpec & {
    appliesTo: string;
    statements: PolicyStatement[];
}

export class Policy {
    readonly name: string;
    appliesTo: string;
    readonly statements: PolicyStatement[];

    constructor(config: TypeCfg<PolicySpec>, blueprints: Map<string, any>) {
        this.name = config.metadata.name;
        this.appliesTo = config.spec.appliesTo;
        this.statements = config.spec.statements;
    }

    configure(policyConfig: PolicyConfiguration, config: IConfiguration) {
        if (!config.personas.has(this.appliesTo)) {
            throw new PolicyError(
                `Policy '${this.name}' appliesTo references invalid ` +
                `Persona '${this.appliesTo}'`, 500);
        }
    }

    guardResource(context: IContext, resource: string,
                  action: PolicyAction): void {
        let allowed = false;
        for (const statement of this.statements) {
            if (statement.resource == resource &&
                    statement.action == action &&
                    statement.effect == "allow") {
                allowed = true;
                break;
            }
        }
        if (!allowed) {
            console.log(
                `Policy violation: guard ` +
                `policy: [${this.name}] ` +
                `resource: [${resource}] ` +
                `userAccountId: [${context.userAccountId}] ` +
                `persona: [${context.persona.name}] ` +
                `action: [${action}] ` +
                `reason: No [allow] rules found for resource`);
            throw new PolicyError();
        }
        for (const statement of this.statements) {
            if (statement.resource == resource &&
                    statement.action == action &&
                    statement.effect == "deny" &&
                    (!statement.where ||
                     statement.where.conditions.length == 0)) {
                console.log(
                    `Policy violation: guard ` +
                    `policy: [${this.name}] ` +
                    `resource: [${resource}] ` +
                    `userAccountId: [${context.userAccountId}] ` +
                    `persona: [${context.persona.name}] ` +
                    `action: [${action}] ` +
                    `reason: A specific [deny] rule prohibits access`);
                throw new PolicyError();
            }
        }
    }

    private personaSubject(context: IContext, subject: string,
                           cache: Map<string, string>): string {
        const cached = cache.get(subject);
        if (cached !== undefined) {
            return cached || "";
        }
        if (subject == "${userid}") {
            return context.userAccountId;
        }
        const subjectRegex = /\$\{my\.(\w+)}/;
        const match = subjectRegex.exec(subject);
        if (match) {
            const result = context.getSubject(match[1]);
            if (result) {
                cache.set(subject, result);
                return result;
            }
        }
        return "";
    }

    private checkNull(op: MatchOperand, statement: PolicyStatement,
                      value: string): PolicyResult {
        const valueNull = value.length == 0;
        const check = { result: false, doBreak: false };
        const conditionResult = (op == "isNull" ? valueNull : !valueNull);
        if (conditionResult) {
            check.result = true;
            if (statement.where!.match == "any") {
                check.doBreak = true;
            }
        } else {
            check.result = false;
            if (statement.where!.match == "all") {
                check.doBreak = true;
            }
        }
        return check;
    }

    private checkEquals(op: MatchOperand, context: IContext,
                        statement: PolicyStatement, condition: MatchCondition,
                        subjectCache: Map<string, string>, attrValue: string,
                        row: Row): PolicyResult {
        const attrValueNull = attrValue.length == 0;
        const check = { result: false, doBreak: false };
        if (!attrValueNull) {
            let subjectValue = "";
            if (condition.subject.startsWith("${")) {
                subjectValue = this.personaSubject(
                    context, condition.subject, subjectCache);
            } else {
                subjectValue = row.getString(condition.subject);
            }
            if (subjectValue.length == 0) {
                check.result = false;
                if (statement.where!.match == "all") {
                    check.doBreak = true;
                }
            } else {
                const conditionResult = (op == "eq" ?
                                         (attrValue == subjectValue) :
                                         (attrValue != subjectValue));
                check.result = conditionResult;
                if ((check.result && statement.where!.match == "any") ||
                    (!check.result && statement.where!.match == "all")) {
                    check.doBreak = true;
                }
            }
        } else {
            check.result = false;
            if (statement.where!.match == "all") {
                check.doBreak = true;
            }
        }
        return check;
    }

    guardResultSet(context: IContext, resource: string,
                   resultSet: IResultSet): IResultSet {
        const subjectCache: Map<string, string> = new Map();
        while (resultSet.next()) {
            this.guardRow(context, resource, "get", resultSet.getRow(),
                          subjectCache);
        }
        return resultSet;
    }

    guardRow(context: IContext, resource: string, action: PolicyAction,
             row: Row, cache?: Map<string, string>): Row {
        let allowed = false;
        const subjectCache: Map<string, string> = cache || new Map();
        for (const statement of this.statements) {
            if (statement.resource == resource &&
                    statement.action == action &&
                    statement.effect == "allow") {
                if (!statement.where) {
                    allowed = true;
                    break;
                }
                let conditionAllowed = false;
                for (const condition of statement.where.conditions) {
                    if (row.has(condition.attr)) {
                        const attrValue = row.getString(condition.attr);
                        if (condition.op == "isNull" ||
                            condition.op == "isNotNull") {
                            const check = this.checkNull(
                                condition.op, statement, attrValue);
                            conditionAllowed = check.result;
                            if (check.doBreak) {
                                break;
                            }
                        } else if (condition.op == "eq" ||
                                   condition.op == "ne") {
                            const check = this.checkEquals(
                                condition.op, context, statement, condition,
                                subjectCache, attrValue, row);
                            conditionAllowed = check.result;
                            if (check.doBreak) {
                                break;
                            }
                        }
                    } else {
                        console.log(
                            `Policy violation: row guard ` +
                            `policy: [${this.name}] ` +
                            `resource: [${resource}] ` +
                            `userAccountId: [${context.userAccountId}] ` +
                            `persona: [${context.persona.name}] ` +
                            `action: [${action}] ` +
                            `reason: Missing attr [${condition.attr}]`);
                        throw new PolicyError();
                    }
                }
                if (conditionAllowed) {
                    allowed = true;
                    break;
                }
            }
        }
        if (!allowed) {
            console.log(
                `Policy violation: row guard ` +
                `policy: [${this.name}] ` +
                `resource: [${resource}] ` +
                `userAccountId: [${context.userAccountId}] ` +
                `persona: [${context.persona.name}] ` +
                `action: [${action}] ` +
                `reason: No [allow] rule evaluated to true`);
            throw new PolicyError();
        }
        for (const statement of this.statements) {
            if (statement.resource == resource &&
                    statement.action == action &&
                    statement.effect == "deny") {
                if (!statement.where) {
                    console.log(
                        `Policy violation: row guard ` +
                        `policy: [${this.name}] ` +
                        `resource: [${resource}] ` +
                        `userAccountId: [${context.userAccountId}] ` +
                        `persona: [${context.persona.name}] ` +
                        `action: [${action}] ` +
                        `reason: A specific [deny] rule prohibits access`);
                    throw new PolicyError();
                }
                let conditionDenied = false;
                for (const condition of statement.where.conditions) {
                    if (row.has(condition.attr)) {
                        const attrValue = row.getString(condition.attr);
                        if (condition.op == "isNull" ||
                            condition.op == "isNotNull") {
                            const check = this.checkNull(
                                condition.op, statement, attrValue);
                            conditionDenied = check.result;
                            if (check.doBreak) {
                                console.log(
                                    `Policy violation: row guard ` +
                                    `policy: [${this.name}] ` +
                                    `resource: [${resource}] ` +
                                    `userAccountId: [${context.userAccountId}] ` +
                                    `persona: [${context.persona.name}] ` +
                                    `action: [${action}] ` +
                                    `attr: [${condition.attr}] ` +
                                    `op: [${condition.op}] ` +
                                    `subject: [${condition.subject}] ` +
                                    `reason: [deny] rule prohibits access`);
                                throw new PolicyError();
                            }
                        } else if (condition.op == "eq" ||
                                   condition.op == "ne") {
                            const check = this.checkEquals(
                                condition.op, context, statement, condition,
                                subjectCache, attrValue, row);
                            conditionDenied = check.result;
                            if (check.doBreak) {
                                console.log(
                                    `Policy violation: row guard ` +
                                    `policy: [${this.name}] ` +
                                    `resource: [${resource}] ` +
                                    `userAccountId: [${context.userAccountId}] ` +
                                    `persona: [${context.persona.name}] ` +
                                    `action: [${action}] ` +
                                    `attr: [${condition.attr}] ` +
                                    `op: [${condition.op}] ` +
                                    `subject: [${condition.subject}] ` +
                                    `reason: [deny] rule prohibits access`);
                                throw new PolicyError();
                            }
                        }
                    } else {
                        console.log(
                            `Policy violation: row guard ` +
                            `policy: [${this.name}] ` +
                            `resource: [${resource}] ` +
                            `userAccountId: [${context.userAccountId}] ` +
                            `persona: [${context.persona.name}] ` +
                            `action: [${action}] ` +
                            `reason: Missing attr [${condition.attr}]`);
                        throw new PolicyError();
                    }
                }
                if (conditionDenied) {
                    console.log(
                        `Policy violation: row guard ` +
                        `policy: [${this.name}] ` +
                        `resource: [${resource}] ` +
                        `userAccountId: [${context.userAccountId}] ` +
                        `persona: [${context.persona.name}] ` +
                        `action: [${action}] ` +
                        `reason: A specific [deny] rule prohibits access`);
                    throw new PolicyError();
                }
            }
        }
        return row;
    }
}

type PolicyClass = { new(config: TypeCfg<PolicySpec>,
                         blueprints: Map<string, any>): Policy; };

export class PolicyConfiguration implements IPolicyConfiguration {
    policies: Map<string, Policy>;
    policyByPersona: Map<string, Policy>;
    json_config: TypeCfg<PolicySpec>[];
    classes: Map<string, any>;

    constructor() {
        this.json_config = [];
        this.policies = new Map();
        this.policyByPersona = new Map();
        this.classes = new Map();
    }

    async reflectAllClasses() {
        const classNames:string[] = [];
        for (const config_j of this.json_config) {
            if (!config_j || config_j.kind != "Policy") {
                break;
            }
            if (!classNames.includes(config_j.spec.type)) {
                classNames.push(config_j.spec.type);
            }
        }
        console.log(`Reflecting ${classNames.length} Policy classes...`);
        const reflection = new Reflection();
        const classPromises: Promise<ClassInfo>[] = [];
        for (const className of classNames) {
            classPromises.push(reflection.reflect(className));
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
            throw new PolicyError("Reflection error", 500, { cause: error });
        }
    }

    instantiate(config: TypeCfg<PolicySpec>): void {
        const instanceName = config.metadata.name;
        // check dupes
        if (this.policies.has(instanceName)) {
            throw new PolicyError(`${instanceName} is duplicated`, 500);
        }
        if (this.policyByPersona.has(config.spec.appliesTo)) {
            throw new PolicyError(
                `Persona ${config.spec.appliesTo} has already been specified ` +
                `on a different Policy`, 500);
        }
        const className = config.spec.type;
        const clazz = this.classes.get(className);
        // Instantiate the Entity class.
        const instanceClazz = <PolicyClass>clazz;
        const instance = new instanceClazz(config, this.classes);
        this.policies.set(instanceName, instance);
        this.policyByPersona.set(config.spec.appliesTo, instance);
    }

    async parse() {
        await this.reflectAllClasses();
        // Create the Policies
        for (const config_j of this.json_config) {
            if (!config_j || config_j.kind != "Policy") {
                break;
            }
            this.instantiate(config_j);
        }
    }

    private configure(config: IConfiguration): void {
        for (const policy of this.policies.values()) {
            policy.configure(this, config);
        }
    }

    private async performBootstrap(config: IConfiguration) {
        await this.parse();
        this.configure(config);
    }

    async load(streams: string[], config: IConfiguration) {
        for (const stream of streams) {
            const configPart = JSON.parse(stream) as TypeCfg<PolicySpec>[];
            this.json_config = this.json_config.concat(configPart);
        }
        await this.performBootstrap(config);
    }

    save(): string {
        return JSON.stringify(this.json_config);
    }

    getPolicy(context: IContext, resource: string,
              action: PolicyAction): Policy {
        const policy = this.policyByPersona.get(context.persona.name);
        if (!policy) {
            console.log(
                `Policy violation: guard ` +
                `resource: [${resource}] ` +
                `userAccountId: [${context.userAccountId}] ` +
                `persona: [${context.persona.name}] ` +
                `action: [${action}] ` +
                `reason: No policy found for that Persona`);
            throw new PolicyError();
        }
        return policy;
    }

    guardResource(context: IContext, resource: string,
                  action: PolicyAction): void {
        this.getPolicy(context, resource, action).guardResource(
            context, resource, action);
    }

    guardRow(context: IContext, resource: string, action: PolicyAction,
             row: Row): Row {
        return this.getPolicy(context, resource, action).guardRow(
            context, resource, action, row);
    }

    guardResultSet(context: IContext, resource: string,
                   resultSet: IResultSet): IResultSet {
        return this.getPolicy(context, resource, "get").guardResultSet(
            context, resource, resultSet);
    }
}

