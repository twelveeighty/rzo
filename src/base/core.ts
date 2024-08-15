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

export class _IError extends Error {
    code: number;

    constructor(code: number, message: string, options?: ErrorOptions) {
        super(message, options);
        this.code = code;
    }
}

class CoreError extends _IError {
    constructor(message: string, options?: ErrorOptions) {
        super(500, message, options);
    }
}

export interface AsyncTask {
    start(): Promise<any>;
    stop(): Promise<any>;
}

export type PolicyAction = "get" | "put" | "post" | "delete";

export interface IPolicyConfiguration {
    guardResource(context: IContext, resource: string,
                  action: PolicyAction): void;
    guardRow(context: IContext, resource: string, action: PolicyAction,
             row: Row): Row;
    guardResultSet(context: IContext, resource: string,
                   resultSet: IResultSet): IResultSet;
}

export interface IConfiguration {
    entities: Map<string, Entity>;
    sources: Map<string, Source>;
    authenticators: Map<string, Authenticator>;
    personas: Map<string, Persona>;
    collections: Map<string, Collection>;
    workers: Map<string, DaemonWorker>;

    getCollection(name: string): Collection;
    getEntity(name: string): Entity;
    getField(fqName: string): Field;
    getSource(name: string): Source;
    getAuthenticator(name: string): Authenticator;
    getPersona(name: string): Persona;

    registerAsyncTask(task: AsyncTask): void;
    startAsyncTasks(): Promise<void>;
    stopAsyncTasks(): Promise<void>;

    set policyConfig(policyConfig: IPolicyConfiguration);
    get policyConfig(): IPolicyConfiguration | undefined;

    getLogThreshold(name: string): LogThreshold;
}

export interface IContext {
    sessionId?: string;
    persona: Persona;
    userAccountId: string;
    getSubject(key: string): string;
}

export type KeyValue = {
    k: string;
    v: any;
}

export type JsonObject = { [name: string]: any };

export type LogLevel = "Debug" | "Info" | "Error" | "Never";

//                         Never   Error   Info  Debug
export type LogThreshold =  0     |  9   |  99  | 999;

export class Logger {
    readonly name: string;
    threshold: LogThreshold;

    static toThreshold(level: LogLevel): LogThreshold {
        switch (level) {
            case "Error": return 9;
            case "Info": return 99;
            case "Debug": return 999;
            default: return 0;
        }
    }

    constructor(name: string) {
        this.name = name;
        this.threshold = 999;
    }

    configure(configuration: IConfiguration): void {
        this.threshold = configuration.getLogThreshold(this.name);
    }

    log(msg: any, level?: LogThreshold): void {
        if (level == undefined) {
            console.log(msg);
        } else {
            if (level <= this.threshold) {
                console.log(msg);
            }
        }
    }

    error(msg: any): void {
        this.log(msg, 9);
    }

    info(msg: any): void {
        this.log(msg, 99);
    }

    debug(msg: any): void {
        this.log(msg, 999);
    }

    willLog(level: LogLevel): boolean {
        return Logger.toThreshold(level) <= this.threshold;
    }
}

type FilterLogical = "and" | "or";
type FilterOperator = "=" | "!=" | "<>" | ">" | "<" | ">=" | "<=" | "<@";

/*
 * https://host.domain/e/asset?f=a,b,c&o=a+&q=a&w=b!,a='hello\, \\ \'\'a=b'
 *                               where: b is not null and a = 'hello, \ ''a=b'
 *
 * https://host.domain/e/asset?f=a,b,c&o=a+&q=o&w=b~,a>=3
 *                               where: b is null or a > 3
 */
export class Filter {
    chunks: string[];
    combinedAs: FilterLogical;
    private _sealedWhere?: string;

    static NullNotNullRegex = /^(\w+)([~!])$/;
    static ComparisonRegex = /^([\w\.]+)([<>=!@]+)(.+)/;
    static QueryAnd      = "q=a&w=";
    static QueryOr       = "q=o&w=";

    constructor(combinedAs?: FilterLogical) {
        this.combinedAs = combinedAs || "and";
        this.chunks = [];
    }

    parseParameters(parameters: string): void {
        if (parameters) {
            if (parameters.length <= (Filter.QueryAnd.length + 1)) {
                throw new CoreError(
                    `Invalid filter parameter: '${parameters}'`);
            }
            const firstChar = parameters[0];
            let toParse: string;
            switch (firstChar) {
                case "q":
                    toParse = parameters;
                    break;
                case "&":
                case "?":
                    toParse = parameters.substring(1);
                    break;
                default:
                    throw new CoreError(
                        `Invalid filter parameter: '${parameters}'`);

            }
            if (toParse.startsWith(Filter.QueryAnd)) {
                this.combinedAs = "and";
            } else if (toParse.startsWith(Filter.QueryOr)) {
                this.combinedAs = "or";
            } else {
                throw new CoreError(`Invalid filter parameter: '${parameters}'`);
            }
            this.chunks = [];
            this.split(toParse.substring(Filter.QueryAnd.length));
            this.createWhere();
        }
    }

    seal(where: string): void {
        this._sealedWhere = where;
    }

    get sealed(): boolean {
        return !!this._sealedWhere;
    }

    get notEmpty(): boolean {
        return (!!this._sealedWhere) || this.chunks.length > 0;
    }

    get empty(): boolean {
        return !this.notEmpty;
    }

    toParameters(firstTerm?: boolean): string {
        if (!(this.chunks.length)) {
            throw new CoreError("Invalid Filter - no chunks defined");
        }
        if (this.combinedAs == "or") {
            if (firstTerm) {
                return "?" + Filter.QueryOr +
                    encodeURIComponent(this.chunks.join());
            }
            return "&" + Filter.QueryOr +
                encodeURIComponent(this.chunks.join());
        } else {
            if (firstTerm) {
                return "?" + Filter.QueryAnd +
                    encodeURIComponent(this.chunks.join());
            }
            return "&" + Filter.QueryAnd +
                encodeURIComponent(this.chunks.join());
        }
    }

    isNotNull(operand: string): Filter {
        this.chunks.push(`${operand.trim()}!`);
        return this;
    }

    isNull(operand: string): Filter {
        this.chunks.push(`${operand.trim()}~`);
        return this;
    }

    op(leftHand: string, operator: FilterOperator, rightHand: string,
                         asIs?: boolean): Filter {
        const finalRight = !asIs ? `'${rightHand}'` : rightHand.trim();
        this.chunks.push(`${leftHand.trim()}${operator.trim()}` + finalRight);
        return this;
    }

    get where(): string {
        if (this._sealedWhere) {
            if (this.chunks.length > 0) {
                return `${this._sealedWhere} and ( ${this.createWhere()} )`;
            }
            return this._sealedWhere;
        }
        if (this.chunks.length > 0) {
            return this.createWhere();
        }
        return "";
    }

    private lookAhead(input: string, pos: number, searchStr: string): boolean {
        if (pos >= (input.length - 1)) {
            return false;
        }
        const nextChar = input[pos + 1];
        for (const toMatch of searchStr) {
            if (toMatch == nextChar) {
                return true;
            }
        }
        return false;
    }

    private createWhere(): string {
        const components: string[] = [];
        for (const chunk of this.chunks) {
            let matched = chunk.match(Filter.NullNotNullRegex);
            if (matched) {
                const operation = matched[2] == "~" ? "is null" : "is not null";
                components.push(`${matched[1]} ${operation}`);
                continue;
            }
            matched = chunk.match(Filter.ComparisonRegex);
            if (matched) {
                components.push(`${matched[1]} ${matched[2]} ${matched[3]}`);
                continue;
            }
            throw new CoreError(`Invalid filter: '${chunk}'`);
        }
        return components.join(` ${this.combinedAs} `);
    }

    private split(input: string): void {
        let pos = 0;
        let chunk = "";
        while (pos < input.length) {
            const nextChar = input[pos];
            switch (nextChar) {
                case ",":
                    if (!chunk) {
                        throw new CoreError(`Empty component found in ` +
                                            `clause '${input}', pos: ${pos}`);
                    }
                    this.chunks.push(chunk);
                    chunk = "";
                    pos++;
                    break;
                case "\\":
                    if (!this.lookAhead(input, pos, "\\',")) {
                        throw new CoreError(`Unmatched escape found in ` +
                                            `clause '${input}', pos: ${pos}`);
                    }
                    pos++;
                    chunk += input[pos];
                    pos++;
                    break;
                default:
                    chunk += nextChar;
                    pos++;
            }
        }
        if (chunk) {
            this.chunks.push(chunk);
        }
    }
}

/*
 * Row is a thin wrapper around a JSON-like object.
 * This allows quick exchange of data to/from external sources without the need
 * to loop through data.
 */
export class Row {
    protected _row: JsonObject;

    static dataToRow(data: any, entity?: Entity): Row {
        if (data && typeof data == "object") {
            const keys = Object.keys(data);
            if (keys.length > 0) {
                if (entity) {
                    entity.transformDataForRow(<JsonObject>data);
                }
                return new Row(<JsonObject>data);
            } else {
                return new Row();
            }
        } else {
            return new Row();
        }
    }

    static emptyRow(columns: string[], core?: CoreColumns): Row {
        const data: JsonObject = {};
        if (core) {
            core.addToJsonObject(data);
        }
        columns.forEach((column) => {
            data[column] = null;
        });
        return new Row(data);
    }

    static rowToData(row: Row): JsonObject {
        if (!row || row.empty) {
            return {};
        }
        return row.raw();
    }

    constructor(object?: JsonObject) {
        if (object) {
            this._row = object;
        } else {
            this._row = {};
        }
    }

    get columnNumbers(): string[] {
        const numbers: string[] = [];
        let num = 1;
        Object.keys(this._row).forEach((column) => numbers.push(`\$${num++}`));
        return numbers;
    }

    getUpdateSet(): string[] {
        const statements: string[] = [];
        let num = 1;
        for (const column of Object.keys(this._row)) {
            statements.push(`${column} = \$${num++}`);
        }
        return statements;
    }

    get columns(): string[] {
        return Object.keys(this._row);
    }

    get empty(): boolean {
        return this.columns.length == 0;
    }

    isNull(column: string): boolean {
        return this.get(column) === null;
    }

    isNotNull(column: string): boolean {
        return this.get(column) !== null;
    }

    has(column: string): boolean {
        return Object.hasOwn(this._row, column);
    }

    hasAll(columns: string[]): boolean {
        return columns.every((column) => Object.hasOwn(this._row, column));
    }

    get(column: string): any {
        if (!Object.hasOwn(this._row, column)) {
            throw new CoreError(
                `Column '${column}' does not exist in row`);
        }
        return this._row[column];
    }

    getString(column: string): string {
        const val = this.get(column);
        if (val === null) {
            return "";
        }
        if (typeof val == "string") {
            return <string>val;
        }
        return "" + val;
    }

    put(column: string, value: any): void {
        if (value === undefined) {
            throw new CoreError(`Cannot assign column '${column}' to undefined`);
        }
        if (!Object.hasOwn(this._row, column)) {
            throw new CoreError(`Column '${column}' does not exist in row`);
        }
        this._row[column] = value;
    }

    tryPut(column: string, value: any): boolean {
        if (Object.hasOwn(this._row, column)) {
            this._row[column] = value;
            return true;
        } else {
            return false;
        }
    }

    updateOrAdd(column: string, value: any) {
        if (!this.tryPut(column, value)) {
            this.add(column, value);
        }
    }

    reset(): void {
        this.columns.forEach((column) => {
            this._row[column] = null;
        });
    }

    add(column: string, value: any): void {
        if (Object.hasOwn(this._row, column)) {
            throw new CoreError(`Cannot add duplicate column '${column}'`);
        }
        if (value === undefined) {
            throw new CoreError(`Cannot add column ${column} as undefined`);
        }
        this._row[column] = value;
    }

    raw(): Object {
        return this._row;
    }

    values(): any[] {
        return Object.values(this._row);
    }

    get core(): CoreColumns {
        return new CoreColumns(
            this.get("_id"),
            this.get("_rev"),
            this.has("inconflict") ? this.get("inconflict") : false,
            this.get("updated"),
            this.get("updatedby")
        );
    }

    delete(column: string): void {
        if (Object.hasOwn(this._row, column)) {
            delete this._row[column];
        }
    }

    assign(source: any): void {
        if (source && typeof source == "object") {
            this._row = <Object>source;
        } else {
            throw new CoreError(`Cannot assign Row from type ` +
                                `'${typeof source}'`);
        }
    }

    applyChanges(changed: Row, deleted?: string[]): void {
        for (const changedCol of changed.columns) {
            this.put(changedCol, changed.get(changedCol));
        }
        if (deleted) {
            for (const toDelete of deleted) {
                this.delete(toDelete);
            }
        }
    }

    copyFrom(source: Row): void {
        for (const col of this.columns) {
            this.put(col, source.get(col));
        }
    }
}

type RowFinderCallback = (row: Row) => boolean;

/* Classic resultset of data. You can only navigate a ResultSet forward.
 * The data itself may come from server, device or memory.
 */
export interface IResultSet {
    // Required:
    next(): boolean;
    get(column: string): any;
    getString(column: string): string;
    isNull(column: string): boolean;
    get rowCount(): number;

    // Required, but may need to call next() before:
    get core(): CoreColumns;
    getColumns(): string[];

    // Optional, could throw Error:
    reset(): void;
    rewind(): void;
    getAll(): Object[];
    getRow(): Row;
    find(callback: RowFinderCallback): Row | undefined;
}

export class EmptyResultSet implements IResultSet {

    getRow(): Row {
        throw new CoreError("Cannot call getRow() on EmptyResultSet");
    }

    find(callback: RowFinderCallback): Row | undefined {
        throw new CoreError("Cannot call find() on EmptyResultSet");
    }

    get(column: string): any {
        throw new CoreError("Cannot call get() on EmptyResultSet");
    }

    get core(): CoreColumns {
        throw new CoreError("Cannot call .core on EmptyResultSet");
    }

    isNull(column: string): boolean {
        throw new CoreError("Cannot call isNull() on EmptyResultSet");
    }

    getString(column: string): string {
        throw new CoreError("Cannot call getString() on EmptyResultSet");
    }

    next(): boolean {
        return false;
    }

    get rowCount(): number {
        return 0;
    }

    reset(): void {
        // no-op
    }

    rewind(): void {
        // no-op
    }

    getAll(): Object[] {
        throw new CoreError("Cannot call getAll() on EmptyResultSet");
    }

    getColumns(): string[] {
        throw new CoreError("Cannot call getColumns() on EmptyResultSet");
    }
}

export class MemResultSet implements IResultSet {
    private _store: Object[];
    private _storeReadIdx: number;
    private _row: Row;

    static fromRow(row: Row): MemResultSet {
        return new MemResultSet([row.raw()]);
    }

    constructor(store?: Object[]) {
        this._store = store || [];
        this._storeReadIdx = -1;
        this._row = new Row();
    }

    next(): boolean {
        const nextCacheIdx = this._storeReadIdx + 1;
        if (nextCacheIdx >= this._store.length) {
            return false;
        }
        this._storeReadIdx += 1;
        this._row = new Row(this._store[this._storeReadIdx]);
        return true;
    }

    get rowCount(): number {
        return this._store.length;
    }

    getRow(): Row {
        if (this._storeReadIdx < 0) {
            throw new CoreError("Must call next() before getRow()");
        }
        return this._row;
    }

    find(callback: RowFinderCallback): Row | undefined {
        this.rewind();
        while (this.next()) {
            if (callback(this._row)) {
                return this._row;
            }
        }
        return undefined;
    }

    has(column: string): boolean {
        if (this._storeReadIdx < 0) {
            throw new CoreError("Must call next() before has()");
        }
        return this._row.has(column);
    }

    hasAll(columns: string[]): boolean {
        if (this._storeReadIdx < 0) {
            throw new CoreError("Must call next() before hasAll()");
        }
        return this._row.hasAll(columns);
    }

    get(column: string): any {
        if (this._storeReadIdx < 0) {
            throw new CoreError("Must call next() before get()");
        }
        return this._row.get(column);
    }

    get core(): CoreColumns {
        if (this._storeReadIdx < 0) {
            throw new CoreError("Must call next() before .core");
        }
        return this._row.core;
    }

    isNull(column: string): boolean {
        if (this._storeReadIdx < 0) {
            throw new CoreError("Must call next() before value()");
        }
        return this._row.isNull(column);
    }

    getString(column: string): string {
        if (this._storeReadIdx < 0) {
            throw new CoreError("Must call next() before get()");
        }
        return this._row.getString(column);
    }

    getAll(): Object[] {
        return this._store;
    }

    getColumns(): string[] {
        return this._row.columns;
    }

    reset(): void {
        this._store = [];
        this._storeReadIdx = -1;
        this._row = new Row();
    }

    rewind(): void {
        this._storeReadIdx = -1;
    }

    loadAll(source: Object[]) {
        this._store = source;
        this._storeReadIdx = -1;
        this._row = new Row();
    }

    addRow(row: Row) {
        this.rewind();
        this._store.push(row.raw());
    }

    copyRow(source: IResultSet, columns: string[]) {
        const jsonObject: JsonObject = {};
        columns.forEach((column) => {
            jsonObject[column] = source.get(column);
        });
        this._store.push(jsonObject);
    }

    load(source: IResultSet): void {
        this.reset();
        if (source.next()) {
            const columns = source.getColumns();
            this.copyRow(source, columns);
            while (source.next()) {
                this.copyRow(source, columns);
            }
        }
    }
}

export interface IService {
    getGeneratorNext(logger: Logger, context: IContext,
                     generatorName: string): Promise<string>;
    getOne(logger: Logger, context: IContext, entity: Entity, id: string,
           rev?: string): Promise<Row>;
    getQueryOne(logger: Logger, context: IContext, entity: Entity,
                filter: Filter): Promise<Row>;
    queryCollection(logger: Logger, context: IContext, collection: Collection,
                    query?: Query): Promise<IResultSet>;
    getQuery(logger: Logger, context: IContext, entity: Entity,
             query: Query): Promise<IResultSet>;
    getSequenceId(logger: Logger, context: IContext,
                  entity: Entity): Promise<string>;
    put(logger: Logger, context: IContext, entity: Entity, id: string,
        row: Row): Promise<Row>;
    post(logger: Logger, context: IContext, entity: Entity,
         row: Row): Promise<Row>;
    delete(logger: Logger, context: IContext, entity: Entity, id: string,
           rev: string): Promise<void>;
    deleteImmutable(logger: Logger, context: IContext, entity: Entity,
                    id: string): Promise<void>;
    queryDeferredToken(logger: Logger, context: IContext, parent: string,
                       contained: string, parentField: string,
                       containedField: string,
                       id: string): Promise<DeferredToken | null>;
    getDeferredToken(logger: Logger, context: IContext,
                     tokenUuid: string): Promise<DeferredToken | null>;
    putDeferredToken(logger: Logger, context: IContext,
                     token: DeferredToken): Promise<number>;
    getDBInfo(logger: Logger, context: IContext): Promise<Row>;
}

export interface IAuthenticator {
    get isAuthenticator(): boolean;
    resetAuthentication(logger: Logger, row: Row): Promise<Row>;
    oneTimeLogin(logger: Logger, row: Row): Promise<IContext>;
    createLogin(logger: Logger, context: IContext, row: Row): Promise<Row>;
    login(logger: Logger, row: Row): Promise<IContext>;
    logout(logger: Logger, context: IContext): Promise<void>;
}

type Metadata = {
    name: string;
    description?: string;
}

export type ClassSpec = {
    type: string;
}

export type TypeCfg<SpecType extends ClassSpec> = {
    apiVersion: string;
    kind: string;
    metadata: Metadata;
    spec: SpecType;
}

export type IndexType = "none" | "asc" | "desc";

export type FieldCfg = ClassSpec & {
    name: string;
    required?: boolean;
    default?: string;
    maxlength?: number;
    indexed?: IndexType;
}

export type EntitySpec = ClassSpec & {
    table: string;
    keyFields: FieldCfg[];
    coreFields: FieldCfg[];
}

type MembershipCfg = {
    name: string;
    entity: string;
    through: string;
    source: string;
}

export type PersonaSpec = ClassSpec & {
    memberships: MembershipCfg[];
}

type CollectionVia = "server" | "collection";

export type OrderBy = {
    field: string;
    order: "asc" | "desc";
};

export type CollectionSpec = ClassSpec & {
    entity: string;
    source: string;
    via: CollectionVia;
    fields: string[];
    orderBy?: OrderBy[];
};

export type DeferredToken = {
    parent: string;
    contained: string;
    parentfield: string;
    containedfield: string;
    id: string;
    token: string;
    updatedby: string;
    updated: Date;
};

/* State-aware value
 */
export class FieldState {

    readonly name: string;
    private _dirty: boolean;
    private _oldState: any;
    private _newState: any;
    cachedResultSet?: IResultSet;

    constructor(name: string) {
        this.name = name;
        this._oldState = null;
        this._newState = null;
        this._dirty = false;
    }

    load(backend: any) {
        this._oldState = backend;
        this._newState = backend;
        this._dirty = false;
    }

    get isNull(): boolean {
        return this._newState === null;
    }

    get isNotNull(): boolean {
        return this._newState !== null;
    }

    get dirtyNotNull(): boolean {
        return this._dirty && (this._newState !== null);
    }

    get dirtyNull(): boolean {
        return this._dirty && (this._newState === null);
    }

    get dirty(): boolean {
        return this._dirty;
    }

    get asString(): string {
        return Entity.asString(this._newState);
    }

    get oldValue(): any {
        return this._oldState;
    }

    get value(): any {
        return this._newState;
    }

    set value(value: any) {
        if (value === undefined) {
            throw new CoreError("Cannot set state to undefined");
        }
        this._newState = value;
        this._dirty = true;
    }

    revert() {
        this._newState = this._oldState;
        this._dirty = false;
    }
}

type MD5Impl = (message: string) => string;

export class CoreColumns {
    _id: string;
    _rev: string;
    inconflict: boolean;
    updated: Date;
    updatedby: string;

    static V_NAMES = ["_id", "_rev", "updated", "updatedby"];
    static NAMES = CoreColumns.V_NAMES.concat("inconflict");

    static exclude(columns: string[]): string[] {
        return columns.filter((column) => !CoreColumns.NAMES.includes(column));
    }

    static addToEntity(columns: string[]): string[] {
        // Prevent duplicates of already existing core columns
        const noCores = CoreColumns.exclude(columns);
        return noCores.concat(CoreColumns.NAMES);
    }

    static addToV(columns: string[]): string[] {
        // Prevent duplicates of already existing core columns
        const noCores = CoreColumns.exclude(columns);
        return noCores.concat(CoreColumns.V_NAMES);
    }

    static ancestryToAncestors(ancestry: string): string[] {
        const ancestors: string[] = ancestry.split(".");
        for (let depth = ancestors.length; depth > 0; depth--) {
            const ancestor = `${depth}-${ancestors[depth - 1]}`;
            ancestors[depth - 1] = ancestor;
        }
        return ancestors;
    }

    static beats(current: string, challenger: string): boolean {
        if (challenger == current) {
            return false;
        }
        if (!current) {
            return true;
        }
        if (!challenger) {
            return false;
        }
        const currentDepth = CoreColumns.versionDepth(current);
        const challengerDepth = CoreColumns.versionDepth(challenger);
        if (currentDepth > challengerDepth) {
            return false;
        }
        if (challengerDepth > currentDepth) {
            return true;
        }
        const currentHash = current.substring(current.indexOf("-") + 1);
        const challengerHash = challenger.substring(
            challenger.indexOf("-") + 1);
        return challengerHash > currentHash;
    }

    static versionWinner(rows: Row[]): Row | undefined {
        if (!rows.length) {
            return undefined;
        }
        if (rows.length == 1) {
            return rows[0];
        }
        rows.sort(CoreColumns.versionCompare);
        return rows[rows.length - 1];
    }

    static versionCompare(left: Row, right: Row): number {
        const leftHash = CoreColumns.versionHash(left.get("_rev"));
        const rightHash = CoreColumns.versionHash(right.get("_rev"));
        if (leftHash == rightHash) {
            return 0;
        }
        if (leftHash < rightHash) {
            return -1;
        }
        return 1;
    }

    static versionHash(rev: string): string {
        if (!rev) {
            throw new CoreError("Empty or null rev has no hash");
        }
        const dashPos = rev.indexOf("-");
        if (dashPos == -1 || dashPos == 0 || dashPos >= (rev.length - 1)) {
            throw new CoreError(`Cannot establish hash from rev` +
                                `: '${rev}'`);
        }
        return rev.substring(dashPos + 1);
    }

    static versionDepth(rev: string): number {
        if (!rev) {
            throw new CoreError("Empty or null rev has no depth");
        }
        const dashPos = rev.indexOf("-");
        if (dashPos == -1 || dashPos == 0) {
            throw new CoreError(`Cannot establish rev depth from rev` +
                                `: '${rev}'`);
        }
        const depthStr = rev.substring(0, dashPos);
        let depthNum;
        try {
            depthNum = Number(depthStr);
        } catch (error) {
            throw new CoreError(
                `Cannot convert ${depthStr} to a number`, { cause: error });
        }
        if (!Number.isInteger(depthNum)) {
            throw new CoreError(`Cannot convert ${depthStr} to an integer`);
        }
        return depthNum;
    }

    static newVersion(row: Row, stage: "POST" | "PUT" | "DELETE",
                      md5Impl: MD5Impl): string {
        const depth = stage === "PUT" || stage === "DELETE" ?
            CoreColumns.versionDepth(row.get("_rev")) + 1 :
            1;
        // create the md5 hash of the row w/o the Core columns.
        const dataCols = CoreColumns.exclude(row.columns);
        if (!dataCols.length) {
            throw new CoreError("Cannot calculate rev on an empty row");
        }
        const data = Row.emptyRow(dataCols);
        data.copyFrom(row);
        if (stage === "DELETE") {
            data.add("_deleted", true);
        }
        const jsonStr = JSON.stringify(Row.rowToData(data));
        const md5Hash = md5Impl(jsonStr);

        return `${depth}-${md5Hash}`;
    }

    static addToRowForPost(row: Row, context: IContext, timestamp: Date,
                           md5Impl: MD5Impl): void {
        row.updateOrAdd("_id", Entity.generateId());
        row.updateOrAdd("updated", timestamp);
        row.updateOrAdd("_rev",
                        CoreColumns.newVersion(row, "POST", md5Impl));
        row.updateOrAdd("inconflict", false);
        row.updateOrAdd("updatedby", context.userAccountId);
    }

    static addToRowForPut(row: Row, context: IContext, timestamp: Date,
                          md5Impl: MD5Impl): void {
        row.updateOrAdd("updated", timestamp);
        row.updateOrAdd("_rev",
                        CoreColumns.newVersion(row, "PUT", md5Impl));
        row.updateOrAdd("inconflict", false);
        row.updateOrAdd("updatedby", context.userAccountId);
    }

    static addToRowForDelete(row: Row, context: IContext, timestamp: Date,
                          md5Impl: MD5Impl): void {
        row.updateOrAdd("updated", timestamp);
        row.updateOrAdd("_rev",
                        CoreColumns.newVersion(row, "DELETE", md5Impl));
        row.updateOrAdd("inconflict", false);
        row.updateOrAdd("updatedby", context.userAccountId);
    }

    constructor(id: string, rev: string, inconflict: boolean, updated: Date,
                updatedby: string) {
        this._id = id;
        this._rev = rev;
        this.inconflict = inconflict;
        this.updated = updated;
        this.updatedby = updatedby;
    }

    addToJsonObject(data: JsonObject) {
        data["_id"] = this._id;
        data["_rev"] = this._rev;
        data["inconflict"] = this.inconflict;
        data["updated"] = this.updated;
        data["updatedby"] = this.updatedby;
    }
}

export class BigDecimal {
    // Configuration: constants
    static DECIMALS = 18; // number of decimals on all instances
    static ROUNDED = true; // numbers are truncated (false) or rounded (true)
    static SHIFT = BigInt("1" + "0".repeat(BigDecimal.DECIMALS));

    private _n: bigint;

    constructor(value: any) {
        this._n = BigDecimal._toN(value);
    }

    private static _toN(value: any): bigint {
        if (typeof value === "bigint") {
            return <bigint>value;
        }
        if (value instanceof BigDecimal) {
            return (<BigDecimal>value)._n;
        }
        const [ints, decis] = String(value).split(".").concat("");
        const slicedDecimals = decis.padEnd(BigDecimal.DECIMALS, "0")
                                     .slice(0, BigDecimal.DECIMALS);
        const roundUp = decis[BigDecimal.DECIMALS] >= "5";
        return BigInt(ints + slicedDecimals) + BigInt(roundUp);
    }

    private static _divRound(dividend: bigint, divisor: bigint): bigint {
        return dividend / divisor + (dividend  * 2n / divisor % 2n);
    }

    static toN(value: any): bigint {
        return BigDecimal._toN(BigDecimal.guard(value));
    }

    static guard(value: any, callerContext?: string): any {
        if (value === null) {
            return 0;
        }
        if (typeof value === "bigint") {
            if (value < Number.MIN_SAFE_INTEGER ||
                    value > Number.MAX_SAFE_INTEGER) {
                const context = callerContext ? `${callerContext}: ` : "";
                throw new CoreError(
                    `${context}Value '${value}' magnitude too large`);
            }
            return Number.parseInt(value.toString());
        } else {
            return value;
        }
    }

    static formatNumeric(value: any, precision: number, scale: number,
                         callerContext?: string): string | null {
        if (value === null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (<BigDecimal>value).toNumeric(precision, scale);
        }
        return new BigDecimal(BigDecimal.guard(value, callerContext))
                   .toNumeric(precision, scale);
    }

    add(num: any): BigDecimal {
        return new BigDecimal(this._n + BigDecimal._toN(num));
    }

    subtract(num: any): BigDecimal {
        return new BigDecimal(this._n - BigDecimal._toN(num));
    }

    multiply(num: any): BigDecimal {
        return new BigDecimal(
            BigDecimal._divRound(this._n * BigDecimal._toN(num),
                                 BigDecimal.SHIFT)
        );
    }

    divide(num: any): BigDecimal {
        return new BigDecimal(
            BigDecimal._divRound(this._n * BigDecimal.SHIFT,
                                 BigDecimal._toN(num))
        );
    }

    equals(other: BigDecimal): boolean {
        return this._n == other._n;
    }

    toString(): string {
        const str = this._n.toString().padStart(BigDecimal.DECIMALS + 1, "0");
        return str.slice(0, -BigDecimal.DECIMALS) +
               "." +
               str.slice(-BigDecimal.DECIMALS).replace(/\.?0+$/, "");
    }

    toNumeric(precision: number, scale: number): string {
        if (!Number.isSafeInteger(precision) || !Number.isSafeInteger(scale) ||
           precision <= 0 || scale <= 0 || scale >= precision) {
            throw new CoreError(
                `Invalid precision '${precision}' and/or scale '${scale}'`);
        }
        const str = this.toString();
        const [ints, decis] = str.split(".").concat("");
        if (ints.length > (precision - scale)) {
            throw new CoreError(
                `Decimal ${str} does not fit in precision ${precision} ` +
                    `scale ${scale}`);
        }
        if (decis.length <= scale) {
            return `${ints}.${decis.padEnd(scale, "0")}`;
        }
        const truncated = `${ints}.${decis.slice(0, scale)}`;
        if (decis[scale] < "5") {
            return truncated;
        }
        // round up
        const rounded = new BigDecimal(truncated).add(
            `0.${"0".repeat(scale - 1)}1`);
        return rounded.toString().padEnd(ints.length + scale + 1, "0");
    }
}

export class State {

    private fields: Map<string, FieldState>;
    entity: Entity;
    core?: CoreColumns;

    constructor(entity: Entity, core?: CoreColumns) {
        this.core = core;
        this.entity = entity;
        this.fields = new Map();
        for (const field of entity.keyFields.values()) {
            field.toFieldStates(this.fields);
        }
        for (const field of entity.coreFields.values()) {
            field.toFieldStates(this.fields);
        }
    }

    hasId(): boolean {
        return !!this.core && !!this.core._id;
    }

    get id(): string {
        if (this.hasId()) {
            return this.core!._id;
        } else {
            throw new CoreError("id is not (yet) defined for this state");
        }
    }

    get rev(): string {
        if (this.core && this.core._rev) {
            return this.core._rev;
        } else {
            throw new CoreError("_rev is not (yet) defined for this state");
        }
    }

    findField(name: string): FieldState | undefined {
        return this.fields.get(name);
    }

    field(name: string): FieldState {
        const fieldState = this.fields.get(name);
        if (fieldState === undefined) {
            throw new CoreError(`state not found for field ${name}`);
        }
        return fieldState;
    }

    value(name: string): any {
        return this.field(name).value;
    }

    asString(name: string): any {
        return this.field(name).asString;
    }
}

export type Phase = "create" | "update" | "delete" | "set";

export type SideEffects = string[] | null;

export type FieldSignificance = "key" | "core";

/* Holds a scalar value.
 *
 * A Field is of a particular FieldType:
 * Derived: a scalar value calculated from an operation such as a combination
 * of other Entity fields, or a summation of other Entities.
 */
export class Field {
    required: boolean;
    significance: FieldSignificance;
    entity: Entity;
    readonly name: string;
    readonly type: string;
    default: string;
    indexed: IndexType;
    maxlength?: number;
    logger: Logger;

    constructor(entity: Entity, config: FieldCfg) {
        this.entity = entity;
        this.required = config.required ?? false;
        this.significance = "core";
        this.default = config.default ?? "";
        this.name = config.name;
        this.type = config.type;
        this.indexed = config.indexed ?? "none";
        this.maxlength = config.maxlength;
        this.logger = new Logger(`field/${this.entity.name}/${this.name}`);
    }

    get fqName(): string {
        return `${this.entity.name}.${this.name}`;
    }

    /*
     * Called by applyValue(). This method is therefore also meant to
     * be lightweight and synchronous. It is also meant to be able
     * to be called by other entities and fields, hence it's public.
     * Typically, type conversions, such as 'string' to 'our' value, should
     * be performed here.
     */
    transform(value: any): any {
        return value;
    }

    transformDataForRow(data: JsonObject): void {
        // Do nothing by default, just pass the data as-is.
    }

    /*
     * Called before validate(). This method is meant to
     * be lightweight and synchronous. If more heavy-duty tasks need to be
     * performed, overload setValue() instead.
     */
    protected applyValue(state: State, fieldState: FieldState,
                         value: any): void {
        fieldState.value = this.transform(value);
    }

    async setValue(state: State, value: any,
                   context: IContext): Promise<SideEffects> {
        this.undefinedCheck(value);
        const fieldState = state.field(this.name);
        this.applyValue(state, fieldState, value);
        await this.validate("set", state, fieldState, context);
        return await this.activate("set", state, fieldState, context);
    }

    value(state: State): any {
        return state.value(this.name);
    }

    async create(state: State, context: IContext,
                 service?: IService): Promise<void> {
        if (this.default) {
            state.field(this.name).value = this.transform(this.default);
        }
    }

    load(row: Row, state: State): void {
        const fieldState = state.field(this.name);
        fieldState.load(row.get(this.name));
    }

    save(row: Row, state: State): void {
        const fieldState = state.field(this.name);
        row.updateOrAdd(this.name, fieldState.value);
    }

    toFieldStates(fieldStates: Map<string, FieldState>): void {
        fieldStates.set(this.name, new FieldState(this.name));
    }

    toColumnName(columns: string[]): void {
        columns.push(this.name);
    }

    configure(configuration: IConfiguration) {
        this.logger.configure(configuration);
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        if (this.required && fieldState.isNull) {
            switch (phase) {
                case "create":
                    throw new CoreError(`${fieldState.name} requires a value`);
                case "update":
                    if (fieldState.dirty) {
                        throw new CoreError(
                            `${fieldState.name} requires a value`);
                    }
                    break;
            }
        }
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {
        return null;
    }

    testValidCharacters(input: any, regex: RegExp,
                        noTrow?: boolean): boolean {
        if (regex.test(input)) {
            return true;
        } else if (noTrow) {
            return false;
        } else {
            throw new CoreError(
                `Field '${this.fqName}' contains invalid characters or is of` +
                    ` an incorrect format (${regex}): '${input}'`);
        }
    }

    undefinedCheck(value: any): void {
        if (value === undefined) {
            throw new CoreError(
                `Cannot set ${this.fqName} to undefined`);
        }
    }

    hasChanged(oldValue: any, newValue: any): boolean {
        if (oldValue === undefined || newValue === undefined) {
            throw new CoreError(`${this.fqName}: cannot detect change if ` +
                                `either value is undefined`);
        }
        // we'll use the standard ES6 inequality algorithm
        return newValue != oldValue;
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.FieldDDL";
    }
}

export class Query {
    fields: string[];
    orderBy: OrderBy[];
    filter?: Filter;
    fromClause?: string;

    static FieldRegex = /^[a-z_][a-z0-9_]+$/;
    static COUNT = /^COUNT\(\*\)$/;
    static MAX = /^MAX\([a-z0-9][a-z0-9_]+\)$/;
    static MIN = /^MIN\([a-z0-9][a-z0-9_]+\)$/;
    static SUM = /^SUM\([a-z0-9][a-z0-9_]+\)$/;
    static GroupedFields = [ Query.COUNT, Query.MAX, Query.MIN, Query.SUM ];

    static isSelectStar(fields?: string[]): boolean {
        return !!fields && fields.length == 1 && fields[0] == "*";
    }

    static isValidField(field: string): boolean {
        if (!field) {
            return false;
        }
        if (Query.GroupedFields.some((regex) => regex.test(field))) {
            return true;
        }
        return Query.FieldRegex.test(field);
    }

    static prepareFields(fields?: string[]): string[] {
        if (!fields || !fields.length) {
            return ["*"];
        }
        // If *any* field is a GroupedField, then ONLY GroupedFields are kept
        if (fields.some((field) =>
                        Query.GroupedFields.some(
                            (regex) => regex.test(field)))) {
            return fields.filter(
                (field) => Query.GroupedFields.some(
                    (regex) => regex.test(field)
                )
            );
        }
        return fields;
    }

    constructor(fields?: string[], filter?: Filter, orderBy?: OrderBy[]) {
        this.fields = Query.prepareFields(fields);
        this.filter = filter;
        this.orderBy = orderBy || [];
    }

    get selectStar(): boolean {
        return Query.isSelectStar(this.fields);
    }

    get hasOrderBy(): boolean {
        return (this.orderBy.length > 0);
    }

    get hasFromClause(): boolean {
        return !!this.fromClause;
    }

    orderToParameters(firstTerm?: boolean): string {
        if (this.orderBy.length > 0) {
            const result: string[] = [];
            this.orderBy.forEach((element) => {
                const direction = element.order == "asc" ? "+" : "-";
                result.push(element.field + direction);
            });
            if (firstTerm) {
                return "?o=" + encodeURIComponent(result.join());
            } else {
                return "&o=" + encodeURIComponent(result.join());
            }
        } else {
            return "";
        }
    }

    toString(): string {
        let result = "f=" + this.fields.join();
        if (this.orderBy.length > 0) {
            result += this.orderToParameters();
        }
        if (this.filter) {
            result += this.filter.toParameters();
        }
        return result;
    }
}

type FieldClass = { new(entity: Entity, config: FieldCfg): Field; };

export class StringField extends Field {

    static isNullish(value: any) {
        if (value == null) {
            return true;
        }
        if (typeof value == "string" && !value) {
            return true;
        }
        return false;
    }

    transform(value: any): any {
        if (value !== null) {
            if (typeof value == "string") {
                if (value) {
                    return value;
                } else {
                    return null;
                }
            } else {
                // this conversion should work for most cases, otherwise
                // the caller must take care of a proper string representation.
                return "" + value;
            }
        } else {
            return null;
        }
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        if (this.maxlength && fieldState.isNotNull) {
            const strState = fieldState.asString;
            const len = strState.length;
            if (len > this.maxlength) {
                throw new CoreError(
                    `Field '${this.fqName}' length too long: '${len}'` +
                    ` characters is greater than ${this.maxlength}`);
            }
        }
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.StringFieldDDL";
    }

}

/* Field for 4-byte integer: -2147483648 to +2147483647
 */
export class IntegerField extends Field {
    static MIN = -2147483648;
    static MAX =  2147483647;

    transform(value: any): any {
        if (value !== null) {
            let result: bigint;
            if (typeof value === "bigint") {
                result = <bigint>value;
            } else {
                try {
                    result = BigInt(value);
                } catch (error) {
                    throw new CoreError(
                        `Cannot transform ${this.fqName} to ${value}`,
                        { cause: error });
                }
            }
            if (result < IntegerField.MIN || result > IntegerField.MAX) {
                throw new CoreError(
                    `${this.fqName}: value ${value} does not fit in a 4-byte` +
                    ` integer`);
            }
            try {
                return Number.parseInt(result.toString());
            } catch (error) {
                throw new CoreError(
                    `Cannot transform ${this.fqName} to ${value}`,
                    { cause: error });
            }
        } else {
            return null;
        }
    }

    transformDataForRow(data: JsonObject): void {
        if (this.name in data) {
            const val = data[this.name];
            if (val !== undefined && !(typeof val === "number")) {
                data[this.name] = this.transform(val);
            }
        }
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.IntegerFieldDDL";
    }

}

/* Field for type=number.
 */
export class NumberField extends Field {

    transform(value: any): any {
        if (value !== null) {
            if (typeof value === "number") {
                return value;
            }
            let result: number;
            try {
                result = Number(value);
            } catch (error) {
                throw new CoreError(`Cannot set ${this.fqName} to ${value}`,
                                    { cause: error });
            }
            if (isNaN(result)) {
                throw new CoreError(
                    `${this.fqName}: value ${value} resolves to NaN`);
            }
            return result;
        } else {
            return null;
        }
    }

    transformDataForRow(data: JsonObject): void {
        if (this.name in data) {
            const val = data[this.name];
            if (val !== undefined && !(typeof val === "number")) {
                data[this.name] = this.transform(val);
            }
        }
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.NumberFieldDDL";
    }

}

/* Field for type=boolean.
 *
 * Uses the ES6 coersion rules for boolean, with two exceptions:
 * 1. The exact string "false" is converted to false (instead of true).
 * 2. null is converted to null (instead of false).
 */
export class BooleanField extends Field {

    transform(value: any): any {
        if (value !== null) {
            if (value === "false") {
                return false;
            } else {
                return Boolean(value);
            }
        } else {
            return null;
        }
    }

    transformDataForRow(data: JsonObject): void {
        if (this.name in data) {
            const val = data[this.name];
            if (val !== undefined && !(typeof val === "boolean")) {
                data[this.name] = this.transform(val);
            }
        }
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.BooleanFieldDDL";
    }

}

export class DateField extends Field {

    transform(value: any): any {
        if (!StringField.isNullish(value)) {
            const dateObj = new Date(value);
            if (Number.isNaN(dateObj.valueOf())) {
                throw new CoreError(`Invalid date value: '${value}'`);
            }
            return dateObj;
        } else {
            return null;
        }
    }

    transformDataForRow(data: JsonObject): void {
        if (this.name in data) {
            const val = data[this.name];
            if (val !== undefined && !(val instanceof Date)) {
                data[this.name] = this.transform(val);
            }
        }
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.DateFieldDDL";
    }

}

export class DateTimeField extends DateField {

    get ddlCreatorClass(): string {
        return "base.core-ddl.DateTimeFieldDDL";
    }

}

export class CreationTimestamp extends DateTimeField {

    async create(state: State, context: IContext,
                 service?: IService): Promise<void> {
        state.field(this.name).value = new Date();
    }

    protected applyValue(state: State, fieldState: FieldState,
                         value: any): void {
        throw new CoreError(
            `${this.fqName}: cannot update this value once it's created.`);
    }

}

type AncestryFieldCfg = FieldCfg & {
    key: string;
    collection: string;
}

export class Cfg<T> {
    name: string;
    private _target?: T;

    constructor(name: string) {
        if (!name) {
            throw new CoreError(
                "Attempt to create Cfg with a null or empty name");
        }
        this.name = name;
    }

    set v(target: T) {
        this._target = target;
    }

    setIf(errContext: string, target?: T): void {
        if (!target) {
            throw new CoreError(`${errContext}'${this.name}' not found`);
        }
        this._target = target;
    }

    setIfCast(errContext: string, target: unknown, targetType: Function): void {
        if (!target) {
            throw new CoreError(`${errContext}'${this.name}' not found`);
        }
        if (target instanceof targetType) {
            this._target = target as T;
        } else {
            throw new CoreError(
                `${errContext}'${this.name}' must be a ${targetType.name}`);
        }
    }

    get v(): T {
        if (this._target === undefined) {
            throw new CoreError(`Unset Cfg error for ${this.name}`);
        }
        return this._target;
    }

    isSet(): boolean {
        return !!this._target;
    }
}

export class AncestryField extends StringField {
    key: Cfg<Field>;
    collection: Cfg<Collection>;

    static VALIDCHARS = /^[a-z0-9._]+$/;
    static VALIDKEYCHARS = /^[a-z0-9_]+$/;

    constructor(entity: Entity, config: AncestryFieldCfg) {
        super(entity, config);
        this.key = new Cfg(config.key);
        this.collection = new Cfg(config.collection);
    }

    transform(value: any): any {
        const orig = super.transform(value);
        if (!orig) {
            return orig;
        }
        const origStr = <string>orig;
        const strValue = origStr.toLowerCase();
        this.testValidCharacters(strValue, AncestryField.VALIDCHARS);
        return strValue;
    }

    configure(configuration: IConfiguration) {
        this.key.setIf(`Field '${this.fqName}': `,
                       this.entity.keyFields.get(this.key.name));
        if (!this.key.v.maxlength || this.key.v.maxlength > 256) {
            throw new CoreError(
                `Field '${this.fqName}': key '${this.key.name}' must have ` +
                    `a 'maxlength' specified with a value less than 256`);
        }
        this.collection.setIf(
            `AncestryField ${this.fqName}: collection `,
            configuration.collections.get(this.collection.name));
    }

    async hasChildren(id: string, ancestry: string, state: State,
                      context: IContext): Promise<boolean> {
        const filter = new Filter()
            .op(this.name, "<@", ancestry)
            .op("_id", "!=", id);
        this.entity.addRegionToFilter(state, filter);
        const resultSet = await this.collection.v.query(
            context, new Query(["COUNT(*)"], filter));
        if (!resultSet.next()) {
            throw new CoreError("Count query must always return one row");
        }
        return resultSet.get("count") > 0;
    }

    async checkParent(ancestry: string, state: State,
                      context: IContext): Promise<boolean> {
        const filter = new Filter().op(`${this.name}`, "=", ancestry);
        this.entity.addRegionToFilter(state, filter);
        const resultSet = await this.collection.v.query(
            context, new Query([], filter));
        return resultSet.next();
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        /*
         * From       To        Result
         * null       null      Pass
         *
         * Notnull    null      Interpreted as: remove hierarchy:
         *                      1. If children exist: Fail, otherwise Pass
         *
         * any        Notnull   Interpreted as: apply hierarchy:
         *                      1. If From not null and From == To: Pass
         *                      2. If children exist under From: Fail
         *                      3. Split To - 'anc.parent.us' into:
         *                         'anc.parent' and 'us'
         *                      4. If 'us' != key, Fail
         *                      5. Use spec.collection to find 'anc.parent' in
         *                         the current region. Found = Pass, otherwise
         *                         Fail.
         */
        if (phase == "set" && fieldState.dirty) {
            const oldValue = fieldState.oldValue;
            const newValue = fieldState.asString;
            if (oldValue && state.hasId() &&
                    (fieldState.isNull || newValue != oldValue)) {
                const hasChildren = await this.hasChildren(
                    state.id, "" + oldValue, state, context);
                if (hasChildren) {
                    throw new CoreError(
                        `${this.fqName}: cannot change ancestry, because` +
                            ` there are still children under ${oldValue}.`);
                }
            }
            if (fieldState.isNotNull) {
                const keyValue = state.asString(this.key.v.name).toLowerCase();
                this.key.v.testValidCharacters(keyValue,
                                              AncestryField.VALIDCHARS);
                if (newValue == keyValue) {
                    return;
                }
                const splits = newValue.split(".");
                if (splits.length == 1) {
                    throw new CoreError(
                        `${this.fqName}: if no parent is specified, then ` +
                                `ancestry field must be equal to key ` +
                                    `'${this.key.v.fqName}'`);
                }
                if (splits.some((part) => !part)) {
                    throw new CoreError(
                        `${this.fqName}: empty component(s) in value ` +
                            `'${newValue}'`);
                }
                const matchKey = splits.pop();
                if (matchKey != keyValue) {
                    throw new CoreError(
                        `${this.fqName}: the final component of the ` +
                                `ancestry field must be equal to key ` +
                                    `'${this.key.v.fqName}'`);
                }
                const parentAncestry = splits.join(".");
                const parentExists = await this.checkParent(
                    parentAncestry, state, context);
                if (!parentExists) {
                    throw new CoreError(
                        `${this.fqName}: parent '${parentAncestry}' does not ` +
                            `exist`);
                }
            }
        }
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.AncestryFieldDDL";
    }

}

export type ForeignKeyCfg = FieldCfg & {
    target: string;
    value: string;
    collection: string;
}

/* Holds the target field from a related Entity
 */
export class ForeignKey extends StringField {

    idName: string;
    targetEntity: Cfg<Entity>;
    targetField: Cfg<Field>;
    collection: Cfg<Collection>;

    constructor(entity: Entity, config: ForeignKeyCfg) {
        super(entity, config);
        this.idName = this.name + "_id";
        this.targetEntity = new Cfg(config.target);
        this.targetField = new Cfg(config.value);
        this.collection = new Cfg(config.collection);
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        // console.log(`Connecting ForeignKey ${this.fqName}`);
        this.targetEntity.setIf(
            `Invalid ForeignKey: ${this.fqName}: entity `,
            configuration.entities.get(this.targetEntity.name)
        );
        this.targetField.setIf(
            `Invalid ForeignKey: ${this.fqName}: ` +
               `field ${this.targetEntity.name}.`,
            this.targetEntity.v.findField(this.targetField.name)
        );
        if (this.targetField.v instanceof ForeignKey) {
            throw new CoreError(
                `Invalid ForeignKey: ${this.fqName}: target ` +
                `${this.targetEntity.name}.${this.targetField.name}` +
                ` is a ForeignKey itself.`);
        }
        this.collection.setIf(
            `ForeignKey ${this.fqName}: collection does not exist: `,
            configuration.collections.get(this.collection.name)
        );
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {

        if (phase == "create" || phase == "update") {
            await Promise.all([
                super.validate(phase, state, fieldState, context),
                super.validate(phase, state, state.field(this.idName), context)
            ]);
        } else if (phase == "set" && fieldState.dirtyNotNull) {
            const filter = new Filter();
            if (this.entity.regionalizedBy
                      && this.targetEntity.v.regionalizedBy) {
                this.entity.addRegionToFilter(
                    state, filter, this.targetEntity.v.regionalizedBy.name);
            }
            const strValue = fieldState.asString;
            filter.op(this.targetField.name, "=", strValue);
            const resultSet = await this.collection.v.query(
                context, new Query([], filter));
            if (!resultSet.next()) {
                throw new CoreError(
                    `${this.fqName} ${this.targetEntity.name} record ` +
                    `${strValue} does not exist`);
            }
            fieldState.cachedResultSet = resultSet;
        }
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {

        const sideEffects =
            await super.activate(phase, state, fieldState, context);

        if (phase == "set" && fieldState.dirty) {
            if (fieldState.isNotNull) {
                if (fieldState.cachedResultSet) {
                    state.field(this.idName).value =
                        fieldState.cachedResultSet.get("_id");
                }
            } else {
                state.field(this.idName).value = null;
            }
        }
        return sideEffects;
    }

    toColumnName(columns: string[]): void {
        columns.push(this.name, this.idName);
    }

    load(row: Row, state: State): void {
        const fieldState = state.field(this.name);
        const idState = state.field(this.idName);
        fieldState.load(row.get(this.name));
        idState.load(row.get(this.idName));
    }

    save(row: Row, state: State): void {
        const fieldState = state.field(this.name);
        const idState = state.field(this.idName);
        row.updateOrAdd(this.name, fieldState.value);
        row.updateOrAdd(this.idName, idState.value);
    }

    toFieldStates(fieldStates: Map<string, FieldState>): void {
        fieldStates.set(this.name, new FieldState(this.name));
        fieldStates.set(this.idName, new FieldState(this.idName));
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.ForeignKeyDDL";
    }
}

type CopyCfg = {
    from: string;
    to: string;
}

export type CrossoverForeignKeyCfg = ForeignKeyCfg & {
    target: string;
    value: string;
    collection: string;
    copies: CopyCfg[];
}

export class CrossoverForeignKey extends ForeignKey {
    copies: CopyCfg[];
    sideEffects: SideEffects;

    constructor(entity: Entity, config: CrossoverForeignKeyCfg) {
        super(entity, config);
        this.copies = config.copies;
        this.sideEffects = null;
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);

        const allSideEffects: string[] = [];
        const target = this.targetEntity.v;
        for (const copy of this.copies) {
            const targetField = target.findField(copy.from);
            if (!targetField) {
                throw new CoreError(
                    `${this.fqName}: field ${copy.from} does not exist on ` +
                    `target entity ${target.name}`);
            }
            const ourField = this.entity.findField(copy.to);
            if (!ourField) {
                throw new CoreError(
                    `${this.fqName}: sibling field ${copy.to} does not exist`);
            }
            if (copy.to == this.name) {
                throw new CoreError(
                    `${this.fqName}: the 'copies' array has an entry that ` +
                    `references its own field: ${copy.to}`);
            }
            const fromCols: string[] = [];
            const toCols: string[] = [];
            targetField!.toColumnName(fromCols);
            ourField!.toColumnName(toCols);
            if (fromCols.length != toCols.length) {
                throw new CoreError(
                    `${this.fqName}: fields ${ourField.fqName} and ` +
                    `${targetField.fqName} use an unequal number of columns: ` +
                    `${toCols.length} and ${fromCols.length}, respectively`);
            }
            allSideEffects.push(copy.to);
        }
        this.sideEffects = allSideEffects;
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {

        await super.activate(phase, state, fieldState, context);

        if (phase == "set" && ((fieldState.dirtyNotNull &&
                fieldState.cachedResultSet) || fieldState.dirtyNull)) {
            const clear = fieldState.isNull;
            const target = this.targetEntity.v;
            for (const copy of this.copies) {
                const fromCols: string[] = [];
                const toCols: string[] = [];
                target.getField(copy.from).toColumnName(fromCols);
                this.entity.getField(copy.to).toColumnName(toCols);
                for (let idx=0; idx < fromCols.length; idx++) {
                    const fromColumn = fromCols[idx];
                    const toColumn = toCols[idx];
                    state.field(toColumn).value = clear ? null :
                        fieldState.cachedResultSet!.get(fromColumn);
                }
            }
            return this.sideEffects;
        }
        return null;
    }
}

type SequenceCfg = {
    field: string;
    increment: number;
}

type SiblingSequenceKeyCfg = ForeignKeyCfg & {
    sequences: SequenceCfg[];
}

export class SiblingSequenceKey extends ForeignKey {
    siblingCfg: SiblingSequenceKeyCfg;

    constructor(entity: Entity, config: SiblingSequenceKeyCfg) {
        super(entity, config);
        this.siblingCfg = config;
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        for (const sequence of this.siblingCfg.sequences) {
            if (!this.entity.findField(sequence.field)) {
                throw new CoreError(
                    `${this.fqName}: cannot find sequence field: ` +
                    `'${sequence.field}'`);
            }
        }
    }

    async nextSequence(fieldName: string, filter: Filter, increment: number,
                       service: IService, context: IContext): Promise<BigInt> {
        const query = new Query([`MAX(${fieldName})`], filter);
        const resultSet = await service.getQuery(
            this.logger, context, this.entity, query);
        if (!resultSet.next()) {
            throw new CoreError("MAX query must always return one row");
        }
        const bigIncrement = BigInt(increment);
        if (resultSet.isNull("max")) {
            return bigIncrement;
        }
        return BigInt(resultSet.get("max")) + bigIncrement;
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {

        await super.activate(phase, state, fieldState, context);

        if (phase == "set" && fieldState.dirtyNotNull && context) {
            const filter = new Filter()
                .op(this.idName, "=", state.asString(this.idName));
            for (const sequence of this.siblingCfg.sequences) {
                const target = state.field(sequence.field);
                if (target.isNull) {
                    const service = this.collection.v.source.v.service;
                    const targetField = this.entity.getField(sequence.field);
                    const nextVal = await this.nextSequence(
                        sequence.field, filter, sequence.increment, service,
                        context);
                    target.value = targetField.transform(nextVal);
                }
            }
        }
        return null;
    }
}

type UniqueSiblingSequenceCfg = FieldCfg & {
    key: string;
    source: string;
}

export class UniqueSiblingSequence extends IntegerField {
    key: Cfg<ForeignKey>;
    source: Cfg<Source>;

    constructor(entity: Entity, config: UniqueSiblingSequenceCfg) {
        super(entity, config);
        this.key = new Cfg(config.key);
        this.source = new Cfg(config.source);
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        const field = this.entity.keyFields.get(this.key.name);
        if (!(field instanceof ForeignKey)) {
            throw new CoreError(
                `${this.fqName}: key field '${this.key.name}' must be a ` +
                `ForeignKey or one of its derivatives`);
        }
        this.key.v = <ForeignKey>field;
        this.source.setIf(
            `${this.fqName}: 'source' `,
            configuration.sources.get(this.source.name));
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        const idName = this.key.v.idName;
        if (phase == "set" && context && fieldState.dirtyNotNull &&
                state.field(idName).isNotNull) {
            const service = this.source.v.service;
            const filter = new Filter()
                .op(idName, "=", state.asString(idName))
                .op(this.name, "=", fieldState.asString, true);
            if (state.hasId()) {
                filter.op("_id", "!=", state.id);
            }
            const query = new Query(["COUNT(*)"], filter);
            const resultSet = await service.getQuery(
                this.logger, context, this.entity, query);
            if (!resultSet.next()) {
                throw new CoreError("COUNT query must always return one row");
            }
            if (resultSet.get("count") > 0) {
                throw new CoreError(
                    `${this.fqName}: value '${fieldState.value}' is already ` +
                    `used by another ${this.entity.name}`);
            }
        }
    }
}

export class Entity {
    readonly name: string;
    readonly type: string;
    readonly table: string;
    protected _immutable: boolean;
    keyFields: Map<string, Field>;
    coreFields: Map<string, Field>;
    contains: ContainedEntity[];
    changeLogs: FieldChangeLogEntity[];
    regionalizedBy?: Field;
    logger: Logger;

    static getFieldClass(kind: string,
                         blueprints: Map<string, any>): FieldClass {
        const clazz = blueprints.get(kind);
        if (clazz !== undefined) {
            return <FieldClass>clazz;
        } else {
            throw new CoreError(`Cannot resolve ${kind} to a class`);
        }
    }

    static asString(input: unknown): string {
        if (input === undefined || input === null) {
            return "";
        }
        if (typeof input == "string") {
            return input as string;
        }
        if (typeof input == "object" && input instanceof Date) {
            return (<Date>input).toISOString();
        }
        return "" + input;
    }

    static generateId(): string {
        return crypto.randomUUID();
    }

    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        this.name = config.metadata.name;
        this.type = config.spec.type;
        this._immutable = false;
        this.table = config.spec.table;
        this.keyFields = new Map();
        this.coreFields = new Map();
        this.contains = [];
        this.changeLogs = [];
        for (const field of config.spec.keyFields) {
            const field_name = field.name;
            this.checkDuplicateField(field_name);
            this.keyFields.set(
                field_name, this.loadField(field, blueprints, "key"));
        }
        for (const field of config.spec.coreFields) {
            const field_name = field.name;
            this.checkDuplicateField(field_name);
            this.coreFields.set(
                field_name, this.loadField(field, blueprints, "core"));
        }
        this.logger = new Logger(`entity/${this.name}`);
    }

    async query(service: IService, query: Query,
                context: IContext): Promise<IResultSet> {
        return service.getQuery(this.logger, context, this, query);
    }

    async queryOne(service: IService, pairs: KeyValue[],
                   context: IContext): Promise<State> {
        const filter = new Filter();
        for (const keyName of this.keyFields.keys()) {
            const match = pairs.find((pair) => pair.k == keyName);
            if (!match || !match.v) {
                throw new CoreError(
                    `Missing key entry for '${this.name}${keyName}'`);
            }
            const isString = (typeof match.v === "string");
            if (isString) {
                filter.op(keyName, "=", <string>match.v);
            } else {
                filter.op(keyName, "=", "" + match.v, true);
            }
        }
        const row = await service.getQueryOne(
            this.logger, context, this, filter);
        if (row.empty) {
            throw new CoreError(
                `Record not found: ${this.name} : ${filter.where}`);
        }
        const state = new State(this, row.core);
        for (const field of this.allFields) {
            field.load(row, state);
        }
        return state;
    }

    async load(service: IService, context: IContext,
               id: string, rev?: string): Promise<State> {
        const row = await service.getOne(
            this.logger, context, this, id, rev);
        if (row.empty) {
            throw new CoreError(`Record not found: ${this.name} = ${id}`);
        }
        const state = new State(this, row.core);
        for (const field of this.keyFields.values()) {
            field.load(row, state);
        }
        for (const field of this.coreFields.values()) {
            field.load(row, state);
        }
        return state;
    }

    from(resultSet: IResultSet): State {
        const state = new State(this, resultSet.core);
        const row = resultSet.getRow();
        this.loadState(row, state);
        return state;
    }

    loadState(row: Row, state: State): void {
        for (const field of this.keyFields.values()) {
            field.load(row, state);
        }
        for (const field of this.coreFields.values()) {
            field.load(row, state);
        }
    }

    async create(context: IContext, service?: IService): Promise<State> {
        const state = new State(this);
        const promises: Promise<void>[] = [];
        for (const field of this.keyFields.values()) {
            promises.push(field.create(state, context, service));
        }
        for (const field of this.coreFields.values()) {
            promises.push(field.create(state, context, service));
        }
        await Promise.all(promises);
        return state;
    }

    transformDataForRow(data: JsonObject): void {
        for (const field of this.allFields) {
            field.transformDataForRow(data);
        }
    }

    toRow(state: State): Row {
        const columns: string[] = [];
        const fields = this.allFields;

        for (const field of fields) {
            field.toColumnName(columns);
        }

        const row = Row.emptyRow(columns, state.core);

        for (const field of fields) {
            field.save(row, state);
        }

        return row;
    }

    async put(service: IService, state: State,
              context: IContext): Promise<Row> {
        await this.validate("update", state, context);
        const row = this.toRow(state);
        return await service.put(
            this.logger, context, this, state.id, row);
    }

    async post(service: IService, state: State,
               context: IContext): Promise<Row> {
        await this.validate("create", state, context);
        const row = this.toRow(state);
        return await service.post(this.logger, context, this, row);
    }

    async delete(service: IService, state: State,
                 context: IContext): Promise<void> {
        await this.validate("delete", state, context);
        if (state.hasId()) {
            await service.delete(
                this.logger, context, this, state.id, state.rev);
        }
    }

    configure(configuration: IConfiguration) {
        this.logger.configure(configuration);
        for (const field of this.keyFields.values()) {
            field.configure(configuration);
        }
        for (const field of this.coreFields.values()) {
            field.configure(configuration);
        }
    }

    checkDuplicateField(name: string) {
        if (this.keyFields.has(name) || this.coreFields.has(name)) {
            throw new CoreError(
                    `Duplicate field ${name} for entity ${this.name}`);
        }
    }

    hasField(name: string): boolean {
        return this.keyFields.has(name) || this.coreFields.has(name);
    }

    findField(name: string): Field | undefined {
        const field = this.keyFields.get(name);
        if (field) {
            return field;
        }
        return this.coreFields.get(name);
    }

    getField(name: string): Field {
        const field = this.findField(name);
        if (field) {
            return field;
        }
        throw new CoreError(`Entity ${this.name} has no field called ${name}`);
    }

    async setValue(state: State, name: string, value: any,
                  context: IContext): Promise<SideEffects> {
        return this.getField(name).setValue(state, value, context);
    }

    get immutable(): boolean {
        return this._immutable;
    }

    get allFields(): Field[] {
        return Array.from(this.keyFields.values()).concat(
            Array.from(this.coreFields.values()));
    }

    get allFieldColumns(): string[] {
        const columns: string[] = [];
        const fields = this.allFields;
        for (const field of fields) {
            field.toColumnName(columns);
        }
        return columns;
    }

    get requiredFieldColumns(): string[] {
        const columns: string[] = [];

        for (const field of this.keyFields.values()) {
            field.toColumnName(columns);
        }

        for (const field of this.coreFields.values()) {
            if (field.required) {
                field.toColumnName(columns);
            }
        }

        return columns;
    }

    loadField(config: FieldCfg, blueprints: Map<string, any>,
              significance: FieldSignificance): Field {
        const type_j = config.type;
        if (!type_j) {
            throw new CoreError(
                `Missing type attribute for field name ${config.name}`);
        }
        const field_clazz = Entity.getFieldClass(type_j, blueprints);
        const field = new field_clazz(this, config);
        field.significance = significance;
        return field;
    }

    hasMembership(through: string): boolean {
        return false;
    }

    /*
     * Return a list of UIDs of 'this' Entity (or a related entity, typically
     * 'contained' by this entity), where the 'person' is a member,
     * as defined by 'through'.
     * Returns empty [] if none exist, never null.
     */
    async getMembers(service: IService, context: IContext, person: string,
                     through: string): Promise<string[]> {
        throw new CoreError(`Entity ${this.name} does not support` +
                            `membership through ${through}`);
    }

    addRegionToFilter(state: State, filter: Filter, columnName?: string): void {
        if (this.regionalizedBy) {
            const ourRegion = state.value(this.regionalizedBy.name);
            if (ourRegion === null) {
                throw new CoreError(
                    `${this.name} cannot be validated because ` +
                        `${this.regionalizedBy.name} needs a value ` +
                        `first`);
            }
            filter.op(columnName || this.regionalizedBy.name,
                      "=", "" + ourRegion);
        }
    }

    async validate(phase: Phase, state: State,
                   context: IContext): Promise<void> {
        if (phase == "update" && !state.hasId()) {
            throw new CoreError(`${this.name} is missing its '_id' for update`);
        }
        const fieldPromises: Promise<void>[] = [];
        for (const field of this.allFields) {
            const fieldState = state.field(field.name);
            fieldPromises.push(
                field.validate(phase, state, fieldState, context));
        }
        await Promise.all(fieldPromises);
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.EntityDDL";
    }

}

export class ImmutableEntity extends Entity {
    constructor(config: TypeCfg<EntitySpec>, blueprints: Map<string, any>) {
        super(config, blueprints);
        this._immutable = true;
    }

    async delete(service: IService, state: State,
                 context: IContext): Promise<void> {
        await this.validate("delete", state, context);
        if (state.hasId()) {
            await service.deleteImmutable(this.logger, context, this, state.id);
        }
    }
}

export type ContainedEntitySpec = EntitySpec & {
    parents: string[];
}

export class ContainedEntity extends Entity {
    parentNames: string[];
    parents: Entity[];

    constructor(config: TypeCfg<ContainedEntitySpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.parentNames = config.spec.parents;
        this.parents = [];
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        for (const parentName of this.parentNames) {
            const target = configuration.entities.get(parentName);
            if (target === undefined) {
                throw new CoreError(`Cannot locate parent entity ` +
                                    `${parentName} for ${this.name}`);
            }
            // Make sure we have a ForeignKey to this parent.
            this.parentKeyFor(parentName);
            // console.log(`Adding ContainedEntity ${this.name} to parent ` +
            //            `${parentName}`);
            this.parents.push(target);
            target.contains.push(this);
        }
    }

    parentKeyFor(parentName: string): ForeignKey {
        // Iterate over our key fields to find a ForeignKey that targets the
        // parent.
        for (const field of this.keyFields.values()) {
            if (field instanceof ForeignKey) {
                const foreignKey = <ForeignKey>field;
                if (foreignKey.targetEntity.name == parentName) {
                    return foreignKey;
                }
            }
        }
        throw new CoreError(`ContainedEntity ${this.name} fails to provide` +
                           ` a foreignKey to parent ${parentName}`);
    }
}

export class ImmutableContainedEntity extends ContainedEntity {
    constructor(config: TypeCfg<ContainedEntitySpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this._immutable = true;
    }
}

type AliasCfg = {
    for: string;
    aliases: string[];
}

type AliasValueListCfg = FieldCfg & {
    internal: string[];
    aliases: AliasCfg[];
}

/* A fixed list of unique values with a configurable alias.
 */
export class AliasValueList extends StringField {
    internal: string[];
    values: Map<string, string>;

    constructor(entity: Entity, config: AliasValueListCfg) {
        super(entity, config);
        this.internal = config.internal;
        this.values = new Map<string, string>();
        for (const value of this.internal) {
            this.values.set(value, value);
        }
        for (const aliasCfg of config.aliases) {
            if (!this.internal.includes(aliasCfg.for)) {
                throw new CoreError(`${this.fqName}: invalid alias 'for': ` +
                                    `${aliasCfg.for}`);
            }
            const aliases = aliasCfg.aliases || [];
            aliases.forEach((entry) => {
                if (this.internal.includes(entry)) {
                    throw new CoreError(`${entry} cannot be an alias for ` +
                            `${aliasCfg.for} since it already exists as an ` +
                            `internal value`);
                }
                this.values.set(entry, aliasCfg.for);
            });
        }
        if (this.default && !this.values.has(this.default)) {
            throw new CoreError(
                `Default value ${this.default} is not a valid` +
                ` entry for ${this.fqName}`)
        }
    }

    getInternalValue(value: string): string {
        if (!value) {
            return "";
        }
        const int_value = this.values.get(value);
        if (int_value === undefined) {
            throw new CoreError(`${value} is an invalid value for ` +
                                `${this.fqName}`)
        }
        return int_value;
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        if (phase != "delete" && fieldState.isNotNull) {
            if (!this.values.has(fieldState.asString)) {
                throw new CoreError(
                    `${fieldState.asString} is not a valid` +
                    ` entry for ${this.fqName}`)
            }
        }
    }
}

/* A field that regionalizes an Entity.
 */
export class RegionField extends ForeignKey {
    static MAXLENGTH = 32;

    constructor(entity: Entity, config: FieldCfg) {
        const foreignKeyCfg = {
            type: config.type,
            name: config.name,
            required: config.required,
            maxlength: RegionField.MAXLENGTH,
            target: "region",
            value: "regionnum",
            collection: "regions"
        };
        super(entity, foreignKeyCfg);
        this.maxlength = RegionField.MAXLENGTH;
        entity.regionalizedBy = this;
    }
}

type GeneratorSpec = FieldCfg & {
    format: string;
    min: number;
    max?: number;
}

/* A human-readable and identifiable field, but auto-generated.
 */
export class GeneratorField extends StringField {

    generatorSpec: GeneratorSpec;
    generatorName: string;

    constructor(entity: Entity, config: FieldCfg) {
        super(entity, config);
        this.generatorSpec = <GeneratorSpec>config;
        this.generatorName = `${this.entity.name}_${this.name}_seq`;
    }

    async create(state: State, context: IContext,
                 service?: IService): Promise<void> {
        if (service && context) {
            const nextVal =
                await service.getGeneratorNext(
                    this.logger, context, this.generatorName);

            let result = this.generatorSpec.format;
            if (result.includes("$YY")) {
                const currentYear = "" + new Date().getFullYear();
                const currentYearShort = currentYear.slice(-2);
                result = result.replaceAll("$YYYY", currentYear);
                result = result.replaceAll("$YY", currentYearShort);
            }
            result = result.replaceAll("$NEXT", nextVal);
            state.field(this.name).value = result;
        }
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.GeneratorFieldDDL";
    }
}

type UniqueFieldCfg = FieldCfg & {
    source: string;
}

export class UniqueField extends StringField {
    source: Cfg<Source>;

    constructor(entity: Entity, config: UniqueFieldCfg) {
        super(entity, config);
        this.source = new Cfg(config.source);
    }

    configure(configuration: IConfiguration) {
        this.source.v = configuration.getSource(this.source.name);
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        if (phase == "set" && fieldState.dirtyNotNull) {
            const service = this.source.v.service;
            const filter = new Filter()
                .op(this.name, "=", fieldState.asString);
            if (state.hasId()) {
                filter.op("_id", "!=", state.id);
            }
            const query = new Query(["COUNT(*)"], filter);
            const resultSet = await service.getQuery(
                this.logger, context, this.entity, query);
            if (!resultSet.next() || resultSet.get("count") > 0) {
                throw new CoreError(
                    `${this.fqName}: value '${fieldState.value}' is already ` +
                    `used by another ${this.entity.name}`);
            }
        }
    }
}

type FieldChangeLogEntitySpec = ContainedEntitySpec & {
    targetEntity: string;
    triggeredBy: string;
    alsoLogs: string[];
}

export class FieldChangeLogEntity extends ContainedEntity {
    targetEntity: Cfg<Entity>;
    triggeredBy: string;
    alsoLogs: string[];

    constructor(config: TypeCfg<FieldChangeLogEntitySpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.targetEntity = new Cfg(config.spec.targetEntity);
        this.triggeredBy = config.spec.triggeredBy;
        this.alsoLogs = config.spec.alsoLogs;
        this._immutable = true;
    }

    shouldTrigger(oldRow: Row, row: Row): boolean {
        const field = this.targetEntity.v.getField(this.triggeredBy);
        return oldRow.has(this.triggeredBy) &&
            row.has(this.triggeredBy) &&
            field.hasChanged(oldRow.get(this.triggeredBy),
                             row.get(this.triggeredBy));
    }

    createChangeLogRow(row: Row, updatedby: string, timestamp: Date,
                       oldRow?: Row): Row {
        const log = new Row();
        log.add("_id", oldRow ? oldRow.get("_id") : row.get("_id"));
        log.add("updated", timestamp);
        log.add("updatedby", updatedby);

        const state = new State(this);

        // Same key(s) as target entity.
        for (const field of this.keyFields.values()) {
            if (row.has(field.name)) {
                // Load state(s) from row
                field.load(row, state);
                field.save(log, state);
            } else if (oldRow) {
                // Load state(s) from oldRow
                field.load(oldRow, state);
                field.save(log, state);
            }
        }

        // TriggeredBy, if not already added as part of the keys
        if (!log.has(this.triggeredBy)) {
            log.add(this.triggeredBy, row.get(this.triggeredBy));
        }
        // AlsoLogs, if not already added earlier, and only if existing in
        // the new row
        for (const alsoLog of this.alsoLogs) {
            if (!log.has(alsoLog)) {
                if (row.has(alsoLog)) {
                    log.add(alsoLog, row.get(alsoLog));
                }
            }
        }
        return log;
    }

    checkForField(where: string, fieldName: string): void {
        if (where == "key") {
            if (!this.keyFields.has(fieldName)) {
                throw new CoreError(
                    `${this.name}: missing key field '${fieldName}'`);
            }
        } else if (where == "core") {
            if (!this.coreFields.has(fieldName)) {
                throw new CoreError(
                    `${this.name}: missing core field '${fieldName}'`);
            }
        } else if (where == "any") {
            if (!this.keyFields.has(fieldName) &&
                    !this.coreFields.has(fieldName)) {
                throw new CoreError(
                    `${this.name}: missing field '${fieldName}'`);
            }
        } else {
            throw new CoreError(
                `${this.name}: unknown 'where': ${where}`);
        }
    }

    configure(configuration: IConfiguration) {
        // make sure we wire in targetEntity BEFORE calling super.configure(),
        // since our super method calls parentKeyFor().
        this.targetEntity.v = configuration.getEntity(this.targetEntity.name);
        if (!this.targetEntity.v.findField(this.triggeredBy)) {
            throw new CoreError(
                `${this.name}: field ${this.triggeredBy} does not exist` +
                    ` on entity ${this.targetEntity.name}`);
        }

        if (this.parentNames.includes(this.targetEntity.name)) {
            throw new CoreError(
                `${this.name}: targetEntity ${this.targetEntity.name} can ` +
                    `not also be defined as a parent in 'parents'`);
        }

        super.configure(configuration);

        for (const fieldName of this.alsoLogs) {
            if (!this.targetEntity.v.findField(fieldName)) {
                throw new CoreError(
                    `${this.name}: field ${fieldName} does not exist` +
                        ` on entity ${this.targetEntity.name}`);
            }
        }
        // For keyFields, we must have the exact same keys as the targetEntity.
        for (const fieldName of this.targetEntity.v.keyFields.keys()) {
            this.checkForField("key", fieldName);
        }
        // We also require a matching triggeredBy and all defined under
        // 'alsoLogs' to be defined, either as a Key or Core field.
        this.checkForField("any", this.triggeredBy);
        for (const fieldName of this.alsoLogs) {
            this.checkForField("any", fieldName);
        }
        //console.log(`Adding FieldChangeLogEntity ${this.name} to target ` +
        //            `${this.targetEntity.name}`);
        this.targetEntity.v.changeLogs.push(this);
    }

    parentKeyFor(parentName: string): ForeignKey {
        // Iterate over targetEntity's key fields to find a ForeignKey that
        // targets the parent.
        for (const field of this.targetEntity.v.keyFields.values()) {
            if (field instanceof ForeignKey) {
                const foreignKey = <ForeignKey>field;
                if (foreignKey.targetEntity.name == parentName) {
                    return foreignKey;
                }
            }
        }
        throw new CoreError(
            `${this.name}: target ${this.targetEntity.name} fails to ` +
                `provide a foreignKey to parent ${parentName}`);
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.FieldChangeLogEntityDDL";
    }

}

type AmountFieldCfg = FieldCfg & {
    precision: number;
    scale: number;
}

/*
 * State: holds either BigDecimal or <null>
 * Row: loads: <any>, saves: string
 */
export class AmountField extends Field {
    precision: number;
    scale: number;

    constructor(entity: Entity, config: AmountFieldCfg) {
        super(entity, config);
        if (!config.precision || !config.scale) {
            throw new CoreError(
                `${this.fqName}: missing 'precision' and/or 'scale' config`);
        }
        this.precision = config.precision;
        this.scale = config.scale;
    }

    transform(value: any): any {
        if (value instanceof BigDecimal) {
            return value;
        }
        return new BigDecimal(BigDecimal.guard(value, this.fqName));
    }

    get ddlCreatorClass(): string {
        return "base.core-ddl.AmountFieldDDL";
    }

    load(row: Row, state: State): void {
        const fieldState = state.field(this.name);
        fieldState.load(this.transform(row.get(this.name)));
    }

    save(row: Row, state: State): void {
        const fieldState = state.field(this.name);
        row.updateOrAdd(this.name,
                BigDecimal.formatNumeric(fieldState.value, this.precision,
                                         this.scale, this.fqName));
    }

    hasChanged(oldValue: any, newValue: any): boolean {
        if (oldValue instanceof BigDecimal && newValue instanceof BigDecimal) {
            return (<BigDecimal>newValue).equals(<BigDecimal>oldValue);
        }
        return super.hasChanged(oldValue, newValue);
    }

    async create(state: State, context: IContext,
                 service?: IService): Promise<void> {
        if (this.required) {
            state.field(this.name).value = new BigDecimal("0");
        }
    }
}

type SummaryFieldCfg = AmountFieldCfg & {
    entity: string;
    field: string;
    operation: string;
}

export class SummaryField extends AmountField {
    fieldName: string;
    parentIdName: Cfg<string>;
    containedEntity: Cfg<ContainedEntity>;

    constructor(entity: Entity, config: SummaryFieldCfg) {
        super(entity, config);
        this.parentIdName = new Cfg("parentIdName");
        this.containedEntity = new Cfg(config.entity);
        this.fieldName = config.field;
    }

    configure(configuration: IConfiguration) {
        super.configure(configuration);
        this.containedEntity.setIfCast(
            `${this.fqName}: configuration error: 'entity' `,
            configuration.entities.get(this.containedEntity.name),
            ContainedEntity);
        this.parentIdName.v = this.containedEntity.v.parentKeyFor(
            this.entity.name).idName;
    }

    async performTokenUpdate(service: IService, token: DeferredToken,
                             context: IContext): Promise<void> {
        // Calculate the new total
        const query = new Query(
            [this.fieldName],
            new Filter().op(this.parentIdName.v, "=", token.id)
        );
        const resultSet = await service.getQuery(
            this.logger, context, this.containedEntity.v, query);
        let totalN = BigDecimal.toN("0");
        while (resultSet.next()) {
            const amountN = BigDecimal.toN(resultSet.get(this.fieldName));
            totalN += amountN;
        }
        const total = new BigDecimal(totalN);
        // Query the parent entity
        const state = await this.entity!.load(service, context, token.id);
        // Update the total
        await this.setValue(state, total, context);
        // Save the changed entity
        await this.entity.put(service, state, context);
    }

    deferUpdate(service: IService, contained: ContainedEntity,
                containedState: State, context: IContext): void {
        const id = containedState.asString(this.parentIdName.v);
        const token: DeferredToken = {
            parent: this.entity.name,
            contained: this.containedEntity.v.name,
            parentfield: this.name,
            containedfield: this.fieldName,
            id: id,
            token: Entity.generateId(),
            updatedby: context.userAccountId,
            updated: new Date()
        };
        service.putDeferredToken(this.logger, context, token)
        .then((waitMillis) => {
            if (!waitMillis) {
                console.log(
                    `Deferred ${this.fqName} update token: ${token.token} ` +
                    `is managed by the service.`);
                return;
            }
            console.log(
                `Deferred ${this.fqName} update with token: ${token.token}`);
            setTimeout(() => {
                service.queryDeferredToken(
                    this.logger, context, token.parent, token.contained,
                    token.parentfield, token.containedfield, token.id)
                .then((currToken) => {
                    if (currToken) {
                        // Only if the current token is the same as 'our' token
                        // do we take action. Otherwise, a further update has
                        // occurred and we simply exit without doing anything.
                        console.log(
                            `Deferred ${this.fqName} comparing target token: ` +
                            `${token.token} to latest token: ` +
                            `${currToken.token}`);

                        if (currToken.token == token.token) {
                            this.performTokenUpdate(service, token, context);
                        } else {
                            console.log(
                               `Deferred ${this.fqName} token ${token.token} ` +
                               `superseded by token ${currToken.token}`);
                        }
                    } else {
                        console.log(
                            `Deferred ${this.fqName} token ${token.token} no ` +
                            `longer exists`);
                    }
                })
                .catch((error) => {
                    console.error(
                        `${this.fqName}: cannot execute deferred update due ` +
                        `to: ${error}`);
                });
            }, waitMillis);
        })
        .catch((error) => {
            console.error(
                `${this.fqName}: cannot schedule deferred update due ` +
                `to: ${error}`);
        });
    }
}

export class Collection {
    readonly name: string;
    via: CollectionVia;
    fields: string[];
    orderBy: OrderBy[];
    entity: Cfg<Entity>;
    source: Cfg<Source>;
    logger: Logger;

    constructor(config: TypeCfg<CollectionSpec>, blueprints: Map<string, any>) {
        this.name = config.metadata.name;
        this.entity = new Cfg(config.spec.entity);
        this.source = new Cfg(config.spec.source);
        this.via = config.spec.via;
        if (Query.isSelectStar(config.spec.fields)) {
            this.fields = config.spec.fields;
        } else {
            this.fields = CoreColumns.addToEntity(config.spec.fields);
        }
        this.orderBy = config.spec.orderBy || [];
        this.logger = new Logger(`collection/${this.name}`);
    }

    configure(configuration: IConfiguration): void {
        this.logger.configure(configuration);
        this.entity.setIf(
            `Collection '${this.name}' references invalid entity `,
            configuration.entities.get(this.entity.name)
        );
        this.source.setIf(
            `Collection '${this.name}' references invalid source `,
            configuration.sources.get(this.source.name)
        );
    }

    async createFilter(context: IContext, filter?: Filter): Promise<Filter> {
        return filter || new Filter();
    }

    async createQuery(context: IContext, query?: Query): Promise<Query> {
        const finalFilter = await this.createFilter(context, query?.filter);
        const finalFields = (query && !query.selectStar) ? query.fields :
            this.fields;
        const finalOrderBy = (query && query.orderBy.length > 0) ?
            query.orderBy : this.orderBy;
        return new Query(finalFields, finalFilter, finalOrderBy);
    }

    async query(context: IContext, query?: Query): Promise<IResultSet> {
        if (this.via == "collection") {
            return this.source.v.service.queryCollection(
                this.logger, context, this, query);
        } else {
            const finalQuery = await this.createQuery(context, query);
            return this.source.v.service.getQuery(
                this.logger, context, this.entity.v, finalQuery);
        }
    }
}

export class Source {
    readonly name: String;

    constructor(config: TypeCfg<ClassSpec>, blueprints: Map<string, any>) {
        this.name = config.metadata.name;
    }

    configure(configuration: IConfiguration) {
    }

    get service(): IService {
        throw new CoreError(`Source ${this.name} has an undefined service`);
    }
}

export class Authenticator {
    readonly name: String;

    constructor(config: TypeCfg<ClassSpec>, blueprints: Map<string, any>) {
        this.name = config.metadata.name;
    }

    configure(configuration: IConfiguration) {
    }

    get service(): IAuthenticator {
        throw new CoreError(
            `Authenticator ${this.name} has an undefined service`);
    }
}

type Membership = {
    entity: Entity;
    through: string;
    source: Source;
}

export class Persona {
    readonly name: string;
    readonly description?: string;
    membershipCfgs: MembershipCfg[];
    memberships: Map<string, Membership>;

    constructor(config: TypeCfg<PersonaSpec>, blueprints: Map<string, any>) {
        this.name = config.metadata.name;
        this.description = config.metadata.description;
        this.membershipCfgs = config.spec.memberships;
        this.memberships = new Map();
    }

    configure(configuration: IConfiguration) {
        for (const membershipCfg of this.membershipCfgs) {
            const entity = configuration.entities.get(membershipCfg.entity);
            if (entity === undefined) {
                throw new CoreError(`Persona '${this.name}' membership ` +
                                      `Entity '${membershipCfg.entity}' not ` +
                                      `found`);
            }
            if (!entity.hasMembership(membershipCfg.through)) {
                throw new CoreError(`Persona '${this.name}': entity ` +
                                    `'${membershipCfg.entity}' does not ` +
                                    `support membership through ` +
                                    `'${membershipCfg.through}'`);
            }
            const source = configuration.sources.get(membershipCfg.source);
            if (source === undefined) {
                throw new CoreError(`Persona '${this.name}' source ` +
                                      `'${membershipCfg.source}' not found`);
            }
            const membershipObj = {
                entity: entity,
                through: membershipCfg.through,
                source: source
            };
            this.memberships.set(membershipCfg.name, membershipObj);
        }
    }

}

export class Nobody extends Persona {
    static ID = "00000000-0000-1000-8000-000000000000";
    static NUM = "NOBODY";
    static INSTANCE = new Nobody();

    constructor() {
        const config = {
            apiVersion: "",
            kind: "",
            metadata: { name: "NOBODY", description: "" },
            spec: {
                type: "",
                memberships: []
            }
        };
        super(config, new Map());
    }
}

export class DaemonWorker implements AsyncTask {

    constructor(config: TypeCfg<ClassSpec>, blueprints: Map<string, any>) {
    }

    configure(configuration: IConfiguration): void {
    }

    async start(): Promise<any> {
    }

    async stop(): Promise<any> {
    }
}

