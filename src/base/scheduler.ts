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

import { Row, _IError, IContext } from "./core.js";

class SchedulerError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export interface ITaskRunner {
    runTask(row: Row, context: IContext): void;
}

type Task = {
    row: Row;
    context: IContext;
    due: number;
}

export class Scheduler {
    timeout: number;
    runner: ITaskRunner;
    private _tasks: Task[];
    private _started: boolean;
    private _running: boolean;
    private _stopped: boolean;
    private _timerId?: any;

    constructor(timeout: number, runner: ITaskRunner) {
        this.timeout = timeout;
        this.runner = runner;
        this._tasks = [];
        this._started = false;
        this._running = false;
        this._stopped = false;
    }

    get started(): boolean {
        return this._started;
    }

    get running(): boolean {
        return this._running;
    }

    get stopped(): boolean {
        return this._stopped;
    }

    private checkTasks(): void {
        const now = Date.now();
        while (this._tasks.length && this._tasks[0].due <= now) {
            const task = this._tasks.shift();
            if (task) {
                this.runner.runTask(task.row, task.context);
            }
        }
    }

    start(): void {
        this._started = true;
        this._timerId = setInterval(() => {
            this.checkTasks();
        }, 1000);
        this._running = true;
        console.log(
            `Scheduler started with timeout: ${this.timeout} on runner ` +
            `'${this.runner.constructor.name}'`);
    }

    stop(): void {
        this._stopped = true;
        this._tasks.splice(0);
        if (this._timerId) {
            clearInterval(this._timerId);
        }
        this._running = false;
        console.log(
            `Scheduler stopped on runner '${this.runner.constructor.name}'`);
    }

    schedule(row: Row, context: IContext): void {
        if (!this._running || this._stopped) {
            throw new SchedulerError(
                "Cannot schedule a task on a stopped Scheduler");
        }
        const due = Date.now() + this.timeout;
        const task: Task = {
            row: row,
            context: context,
            due: due
        };
        this._tasks.push(task);
    }
}

