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

import { IContext, Persona, Row, Nobody, Logger } from "../base/core.js";

type SubjectType = {
    subject: string;
    id: string;
}

export interface ISessionBackendService {
    get isSessionBackendService(): boolean;
    getSession(logger: Logger, id: string): Promise<Row>;
    createSession(logger: Logger, userId: string, expiryOverride?: Date,
                  personaOverride?: Persona): Promise<Row>;
    deleteSession(logger: Logger, id: string): Promise<void>;
    deleteSessionsUpTo(logger: Logger, expiry: Date): Promise<void>;
}

export function serializeSubjectMap(map: Map<string, string>): string {
    const arr: SubjectType[] = [];
    map.forEach((value, key) => {
        arr.push({ subject: key, id: value });
    });
    return JSON.stringify(arr);
}

export function deserializeSubjectMap(subjects?: string): Map<string, string> {
    const result: Map<string, string> = new Map();
    if (subjects && subjects.startsWith("[") && subjects.endsWith("]")) {
        const arr = JSON.parse(subjects) as SubjectType[];
        for (const entry of arr) {
            result.set(entry.subject, entry.id);
        }
    }
    return result;
}

export class SessionContext implements IContext {
    static DEFAULT_TIMEOUT = 1000*60*60;

    sessionId: string;
    persona: Persona;
    userAccount: string;
    userAccountId: string;
    expiry: Date;
    subjects: Map<string, string>;

    constructor(row?: Row, persona?: Persona) {
        this.sessionId =  row?.get("_id") || Nobody.ID;
        this.userAccountId = row?.get("useraccountnum_id") || Nobody.ID;
        this.userAccount = row?.get("useraccountnum") || Nobody.NUM;
        this.persona = persona || Nobody.INSTANCE;
        this.expiry = row?.get("expiry") || new Date();
        this.subjects = deserializeSubjectMap(row?.get("subjects"));
    }

    getSubject(key: string): string {
        return this.subjects.get(key) || "";
    }

    setSubject(key: string, value: string) {
        this.subjects.set(key, value);
    }

    toRow(): Row {
        return new Row({
            "_id": this.sessionId,
            "useraccountnum_id": this.userAccountId,
            "useraccountnum": this.userAccount,
            "persona": this.persona.name,
            "expiry": this.expiry,
            "subjects": serializeSubjectMap(this.subjects)
        });
    }
}

