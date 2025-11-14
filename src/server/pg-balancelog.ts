/*
    RZO - A Business Application Framework

    Copyright (C) 2025 Frank Vanderham

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

import pg from "pg";
import Pool from "pg-pool";
import Cursor from "pg-cursor";

import {
    Entity, IResultSet, IConfiguration, DaemonWorker, MemResultSet, Row,
    TypeCfg, ClassSpec, _IError, Nobody, Cfg, Logger, IntegerField, BigDecimal
} from "../base/core.js";

import { LocalDay, AccSplit } from "../accting/acc-core.js";

import { LeaderElector } from "./election.js";

import { PgConnection } from "./pg-client.js";

class BalanceLogError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

type BalanceLogWorkerSpec = ClassSpec & {
    pool: string;
    leaderElector: string;
    /* Set extraMinutes to the number of minutes we wait after midnight of
     * the new day to close out the previous day.
     * Use this value to set a grace period where you expect transactions to
     * be posted after midnight for the previous day.
     * We use UTC elapsed time to determine action, which should work for
     * virtually all automated systems that create or replicate transactions,
     * but be aware that if transactions are linked to Daylight Savings
     * changes, then this parameter may have to be set longer to compensate.
     */
    extraMinutes: number;
    checkFrequency: number;
}

export class BalanceLogWorker extends DaemonWorker {
    readonly name: string;
    private _checkId: NodeJS.Timeout | null;
    private _leader: boolean;
    private _conn_pool?: Pool<pg.Client>;
    conn: Cfg<PgConnection>;
    checkFrequency: number;
    leaderElector: Cfg<LeaderElector>;
    logger: Logger;
    extraMinutes: number;
    yesterdayMillis: number;
    balanceLogEntity: Cfg<Entity>;
    lastBalanceLogEntity: Cfg<Entity>;

    constructor(config: TypeCfg<BalanceLogWorkerSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.checkFrequency = config.spec.checkFrequency;
        this.extraMinutes = config.spec.extraMinutes;
        this._leader = false;
        this.leaderElector = new Cfg(config.spec.leaderElector);
        if (this.checkFrequency < 1000) {
            throw new BalanceLogError(
                `Invalid BalanceLogWorker configuration ${this.name}, one ` +
                `or more time/frequency values is below 1000`);
        }
        this._checkId = null;
        this.logger = new Logger(`balancelogger/${this.name}`);
        this.extraMinutes = IntegerField.parseInteger(config.spec.extraMinutes);
        this.yesterdayMillis = (24*60 + this.extraMinutes) * 60 * 1000;
        this.conn = new Cfg(config.spec.pool);
        this.balanceLogEntity = new Cfg("balancelog");
        this.lastBalanceLogEntity = new Cfg("lastbalancelog");
    }

    configure(configuration: IConfiguration): void {
        super.configure(configuration);
        this.logger.configure(configuration);
        this.leaderElector.setIfCast(
            `Invalid BalanceLogWorker: leaderElector `,
            configuration.workers.get(this.leaderElector.name),
            LeaderElector);
        this.leaderElector.v.onChange((leader) => {
            this._leader = leader;
        });
        const conn: unknown = configuration.workers.get(this.conn.name);
        if (conn instanceof PgConnection) {
            this.conn.v = conn as PgConnection;
        } else {
            throw new BalanceLogError(
                `Worker ${this.conn.name} is not a PgConnection`);
        }
        this.balanceLogEntity.setIf(
            `BalanceLogWorker '${this.name}': `,
            configuration.entities.get(this.balanceLogEntity.name));
        this.lastBalanceLogEntity.setIf(
            `BalanceLogWorker '${this.name}': `,
            configuration.entities.get(this.lastBalanceLogEntity.name));
        configuration.registerAsyncTask(this);
    }

    get pool(): Pool<pg.Client> {
        if (this._conn_pool) {
            return this._conn_pool;
        } else {
            this._conn_pool = this.conn.v.pool;
            return this._conn_pool;
        }
    }

    protected log(statement: string, parameters?: any[]): void {
        this.logger.debug(statement);
        if (parameters && parameters.length) {
            this.logger.debugAny(parameters);
        }
    }

    private maxDayPeriod(now: Date): number {
        const maxDayDate = new Date(now.valueOf() - this.yesterdayMillis);
        return LocalDay.fromUtc(maxDayDate).getPeriod();
    }

    private schedule(): void {
        this._checkId = setTimeout(() => {
            this.execute();
        }, this.checkFrequency);
    }

    private async processDailies(dayPeriod: number,
                                 lastBalanceLogs: IResultSet): Promise<void> {
        lastBalanceLogs.rewind();
        while (lastBalanceLogs.next()) {
            const lastBalanceLog = lastBalanceLogs.getRow();
            if (lastBalanceLog.isNotNull("invalidated") ||
                lastBalanceLog.get("period") != dayPeriod) {
                await this.processDaily(
                    dayPeriod,
                    Row.dataToRow(lastBalanceLog.raw(),
                                  this.lastBalanceLogEntity.v));
            }
        }
    }

    private async addTotal(lastBalanceLog: Row): Promise<void> {
        const now = new Date();
        const rowId = Entity.generateId();
        const row = new Row({
            _id: rowId,
            updated: now,
            updatedby: Nobody.ID,
            account: lastBalanceLog.get("account"),
            account_id: lastBalanceLog.get("account_id"),
            type: lastBalanceLog.get("type"),
            period: lastBalanceLog.get("period"),
            balance: lastBalanceLog.get("balance"),
            presentvalue: lastBalanceLog.get("presentvalue"),
            created: now,
            posted: now
        });
        const client = await this.pool.connect();
        try {
            let statement = "BEGIN";
            this.log(statement);
            await client.query(statement);
            statement = `insert into balancelog ` +
                `(${row.columns.join()}) ` +
                `values (` +
                `${row.columnNumbers.join()}` +
                `)`;
            let parameters = row.values();
            this.log(statement, parameters);
            await client.query(statement, parameters);
            if (lastBalanceLog.has("_id")) {
                const noIdRow = lastBalanceLog.copyWithout(["_id"]);
                noIdRow.put("invalidated", null);
                const setList = noIdRow.getUpdateSet();
                statement =
                    `update lastbalancelog set ${setList.join(", ")} ` +
                    `where _id = \$${setList.length + 1}`;
                parameters = noIdRow.values().concat(lastBalanceLog.get("_id"));
                this.log(statement, parameters);
                await client.query(statement, parameters);
            } else {
                /* Repeat the balancelog statement but for lastbalancelog with
                 * the same parameters as for the balancelog insert.
                 */
                statement = `insert into lastbalancelog ` +
                    `(${row.columns.join()}) ` +
                    `values (` +
                    `${row.columnNumbers.join()}` +
                    `)`;
                this.log(statement, parameters);
                await client.query(statement, parameters);
                lastBalanceLog.add("_id", rowId);
            }
            statement = "COMMIT";
            this.log(statement);
            await client.query(statement);
        } catch (err: any) {
            const statement = "ROLLBACK";
            this.log(statement);
            await client.query(statement);
            throw err;
        } finally {
            client.release();
        }
    }

    private async catchUp(lastPeriod: number, dayPeriod: number,
                          lb: Row): Promise<void> {

        /* select gen_random_uuid(), 'account',
         *   cast(to_char(a, 'YYYYMMDD') as integer)
         *   from generate_series('2025-06-19'::timestamp,
         *      '2025-07-02'::timestamp, '1 day'::interval) as s(a);
         */
        const minPeriod = LocalDay.fromPeriod(lastPeriod).next().getPeriod();
        const now = new Date();
        const minPgDate = LocalDay.toPostgresDate(minPeriod);
        const maxPgDate = LocalDay.toPostgresDate(dayPeriod);
        let statement =
            `insert into balancelog ` +
            `(_id, ` +
            `updated, updatedby, account, account_id, type, ` +
            `period, ` +
            `balance, presentvalue, created, posted) ` +
            `select ` +
            `gen_random_uuid(), ` +
            `\$1, \$2, \$3, \$4, \$5, ` +
            `cast(to_char(a, 'YYYYMMDD') as integer), ` +
            `\$6, \$7, \$8, \$9 ` +
            `from ` +
            `generate_series(` +
            `'${minPgDate}'::timestamp, '${maxPgDate}'::timestamp, ` +
            `'1 day'::interval) as s(a)`;
        let parameters = [
            now, Nobody.ID, lb.get("account"), lb.get("account_id"),
            lb.get("type"), lb.get("balance"), lb.get("presentvalue"),
            now, now
        ];
        const client = await this.pool.connect();
        try {
            let ctrlStatement = "BEGIN";
            this.log(ctrlStatement);
            await client.query(ctrlStatement);
            this.log(statement, parameters);
            const result = await this.pool.query(statement, parameters);
            this.logger.debug(
                `Created ${result.rowCount} rows for catchup on account ` +
                `${lb.get("account")} from ${minPgDate} to ${maxPgDate}`);
            statement =
               `update lastbalancelog ` +
               `set period = \$1, invalidated = null ` +
               `where _id = \$2`;
            parameters = [dayPeriod, lb.get("_id")];
            this.log(statement, parameters);
            await this.pool.query(statement, parameters);
            ctrlStatement = "COMMIT";
            this.log(ctrlStatement);
            await client.query(ctrlStatement);
        } catch (err: any) {
            const ctrlStatement = "ROLLBACK";
            this.log(ctrlStatement);
            await client.query(ctrlStatement);
            throw err;
        } finally {
            client.release();
        }
    }

    private async queryInvalidatedBalance(invalidated: number,
                                          lastBalance: Row): Promise<void> {
        /* Query balancelog for the total the day BEFORE invalidated. It is
         * possible that there isn't such a row, in which case we set the
         * current totals to zero.
         */
        const statement =
            `select * from balancelog where ` +
            `account_id = \$1 and type = 'D' and period = \$2`;
        const parameters = [lastBalance.get("account_id"), invalidated - 1];
        this.log(statement, parameters);
        const result = await this.pool.query(statement);
        if (result.rows.length > 0) {
            const balanceLog =
                Row.dataToRow(result.rows[0], this.balanceLogEntity.v);
            lastBalance.put("balance", balanceLog.get("balance"));
            lastBalance.put("presentvalue", balanceLog.get("presentvalue"));
        } else {
            lastBalance.put("balance", new BigDecimal("0"));
            lastBalance.put("presentvalue", new BigDecimal("0"));
        }
    }

    private async processDaily(dayPeriod: number,
                               lastBalance: Row): Promise<void> {
        /*
         *  02-MAR-2002 17:35:00   CR    $12.43
         *  ---> DAY PERIOD 20020303  TOTAL = $10,012.43 (Starting balance)
         *  03-MAR-2002 09:35:00   CR    $10.00
         *  03-MAR-2002 10:35:00   DR    $ 1.00
         *  03-MAR-2002 14:35:00   DR    $ 2.00
         *  03-MAR-2002 20:35:00   CR    $13.00
         *  ---> DAY PERIOD 20020304  TOTAL = $10,032.43 (Starting balance)
         *
         */
        const accountId = lastBalance.get("account_id");
        const lastPeriod = lastBalance.get("period");
        let startUtc = lastPeriod != null ?
            LocalDay.fromPeriod(lastPeriod).utc :
            null;
        if (startUtc != null) {
            const invalidated = lastBalance.get("invalidated");
            if (invalidated != null) {
                await this.queryInvalidatedBalance(invalidated, lastBalance);
                startUtc = LocalDay.fromPeriod(invalidated).utc;
            }
        }
        /* Since we want to capture the full day (23, 24 or 25 hrs, depending
         * on Daylight savings), we query 'less than the next day's midnight'.
         */
        const endUtc = LocalDay.fromPeriod(dayPeriod).utc;
        const statement = startUtc != null ?
            `select * from accsplit where ` +
            `account_id = \$1 and ` +
            `posted >= \$2 and posted < \$3 ` +
            `order by posted` :
            `select * from accsplit where ` +
            `account_id = \$1 and ` +
            `posted < \$2 ` +
            `order by posted`;
        const parameters = startUtc != null ?
            [accountId, startUtc, endUtc] :
            [accountId, endUtc];
        this.log(statement, parameters);
        const client = await this.pool.connect();
        try {
            const cursor = client.query(new Cursor(statement, parameters));
            let rows = await cursor.read(this.conn.v.pageSize);
            if (rows.length > 0) {
                /* The start period may be before dayPeriod, so check the first
                 * row for its posted date.
                 */
                let period = LocalDay.fromUtc(
                    Row.dataToRow(rows[0]).get("posted")).getPeriod();
                let splitPeriod = period;
                if (startUtc == null) {
                    /* If there was no last balancelog, then this is the first
                     * split encountered for this account. Create the starting
                     * balance of 0.00 at that period.
                     */
                    lastBalance.put("period", period);
                    await this.addTotal(lastBalance);
                }
                while (rows.length > 0) {
                    // Next page of rows
                    const page = new MemResultSet(rows);
                    while (page.next()) {
                        const splitRow = page.getRow();
                        splitPeriod = LocalDay.fromUtc(
                            splitRow.get("posted")).getPeriod();
                        if (period != splitPeriod) {
                            const nextPeriod =
                                LocalDay.fromPeriod(period).next().getPeriod();
                            /* Commit the total for the "period", which is
                             * actually period + 1, since the starting balance
                             * is the closing balance of the previous day.
                             */
                            lastBalance.put("period", nextPeriod);
                            await this.addTotal(lastBalance);
                            /* Perform 'catch-up' if the next period is not
                             * period + 1 to add the missing day(s).
                             */
                            if (splitPeriod > nextPeriod) {
                                await this.catchUp(
                                    nextPeriod, splitPeriod, lastBalance);
                            }
                            // Start tracking the new period
                            period = splitPeriod;
                        }
                        // Update the totals
                        const amount = new BigDecimal(splitRow.get("amount"));
                        const quantity = splitRow.isNotNull("quantity") ?
                            new BigDecimal(splitRow.get("quantity")) :
                            amount;
                        const curBal = BigDecimal.ensure(
                            lastBalance.get("balance"));
                        const curVal = BigDecimal.ensure(
                            lastBalance.get("presentvalue"));;
                        if (AccSplit.balanceOperator(splitRow) == "+=") {
                            lastBalance.put("balance", curBal.add(quantity));
                            lastBalance.put(
                                "presentvalue", curVal.add(amount));
                        } else {
                            lastBalance.put(
                                "balance", curBal.subtract(quantity));
                            lastBalance.put(
                                "presentvalue", curVal.subtract(amount));
                        }
                    }
                    rows = await cursor.read(this.conn.v.pageSize);
                }
                // Write the last total
                lastBalance.put(
                    "period",
                    LocalDay.fromPeriod(period).next().getPeriod());
                await this.addTotal(lastBalance);
                period = LocalDay.fromPeriod(splitPeriod).next().getPeriod();
                if (period < dayPeriod) {
                    // Perform any catch-up to the current dayPeriod, if needed
                    await this.catchUp(period, dayPeriod, lastBalance);
                }
            } else {
                /* If there's no rows returned, repeat the last total or create
                 * a "0.00" sum if there wasn't a last total for the dayPeriod.
                 */
                lastBalance.put("period", dayPeriod);
                await this.addTotal(lastBalance);
            }
        } finally {
            await client.release();
        }
    }

    private async queryLastBalanceLogs(accounts: Map<string, Row>)
                                                : Promise<IResultSet> {
        const statement =
            `select * from lastbalancelog ` +
            `where type = 'D'`;
        this.log(statement);
        const result = await this.pool.query(statement);
        const balanceLogs = new MemResultSet(result.rows);
        const missing = Array.from(accounts.keys()).filter((id) =>
             !(balanceLogs.find(
                 (dRow) => dRow.get("account_id") == id))
        );
        for (const id of missing) {
            balanceLogs.addRow(
                new Row({
                    account: accounts.get(id)!.get("name"),
                    account_id: id,
                    type: "D",
                    period: null,
                    invalidated: null,
                    balance: new BigDecimal("0.0"),
                    presentvalue: new BigDecimal("0.0")
                })
            );
        }
        return balanceLogs;
    }

    private async queryAccounts(): Promise<Map<string, Row>> {
        const statement =
            `select * from account where ` +
            `status = 'ACTIVE' and islogged = true`;
        this.log(statement);
        const result = await this.pool.query(statement);
        const rs = new MemResultSet(result.rows);
        const accounts: Map<string, Row> = new Map();
        while (rs.next()) {
            const account = rs.getRow();
            accounts.set(account.get("_id"), account);
        }
        return accounts;
    }

    private async run(): Promise<void> {
        const now = new Date();
        const dayPeriod = this.maxDayPeriod(now);
        const accounts = await this.queryAccounts();
        const lastBalanceLogs = await this.queryLastBalanceLogs(accounts);
        await this.processDailies(dayPeriod, lastBalanceLogs);
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
            `BalanceLogger '${this.name}' running every ${this.checkFrequency}`);
        super.start();
        this.schedule();
    }

    async stop(): Promise<any> {
        if (this._checkId) {
            clearInterval(this._checkId);
        }
        this.logger.log(`BalanceLogger ${this.name} stopped`);
    }
}

