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
    IncomingMessage, Server, ServerResponse, createServer
} from "http";

import {
    ClassSpec, TypeCfg, IConfiguration, DaemonWorker, _IError, Cfg, Logger
} from "../base/core.js";

import { IAdapter, AdapterError, getHeader } from "./adapter.js";

class RestServerError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

class RestServer {
    httpServer: Server;
    config: IConfiguration;
    adapters: Map<string, IAdapter>;
    logger: Logger;

    constructor(logger: Logger, config: IConfiguration,
                adapterSpecs: AdapterRefSpec[]) {
        this.logger = logger;
        this.config = config;
        this.adapters = new Map();
        for (const spec of adapterSpecs) {
            if (this.adapters.has(spec.context)) {
                throw new RestServerError(
                    `REST server: duplicate context: '${spec.context}'`);
            }

            const worker: unknown = config.workers.get(spec.adapter);
            if (!worker || !((<any>worker).isAdapter)) {
                throw new RestServerError(
                    `Invalid REST Server: worker ${spec.adapter} does not ` +
                    `exist or it is not an 'Adapter'`);
            }
            this.adapters.set(spec.context, <IAdapter>worker);
            this.logger.log(
                `Adapter context '${spec.context}', worker type ` +
                `${worker.constructor.name}`);
        }
        this.httpServer = createServer((request, response) => {
            this.handle(request, response);
        });
    }

    handleCORS(request: IncomingMessage, response: ServerResponse): void {
        const origin = getHeader(request.headers, "origin");
        if (origin) {
            const requestMethod = getHeader(
                request.headers, "access-control-request-method");
            const requestHeaders = getHeader(
                request.headers, "access-control-request-headers");
            this.logger.debug(`CORS Origin: ${origin}`);
            this.logger.debug(`CORS Access-Control-Request-Method: ` +
                        `${requestMethod}`);
            this.logger.debug(`CORS Access-Control-Request-Headers: ` +
                        `${requestHeaders}`);
            response.setHeader("Access-Control-Allow-Origin", origin);
            response.setHeader("Access-Control-Allow-Methods",
                               "GET, POST, PUT, DELETE, HEAD, OPTIONS");
            response.setHeader("Access-Control-Allow-Headers",
                               "rzo-sessionid, Content-Type");
            response.setHeader("Access-Control-Max-Age", "86400");
            response.statusCode = 204;
            response.end();
        } else {
            throw new RestServerError("Invalid request: missing 'Origin'");
        }
    }

    handle(request: IncomingMessage, response: ServerResponse): void {
        this.logger.info(`${request.method} - ${request.url}`);
        try {
            // Handle CORS
            if (request.method == "OPTIONS") {
                this.handleCORS(request, response);
                return;
            }
            response.setHeader('Content-Type', 'application/json');
            /* If the request includes an "Origin" header, we respond
             * with the matching Access-Control-Allow-Origin.
             */
            const origin = getHeader(request.headers, "origin");
            if (origin) {
                response.setHeader("Access-Control-Allow-Origin", origin);
            }
            let url = request.url || "/";
            if (url == "/") {
                const welcome = { msg: "Welcome to RZO" };
                response.end(JSON.stringify(welcome));
            } else {
                /* split the url by '/', '?' and remove all empty
                 * and '/' elements:
                 */
                const regexp = /([\/\?])/;
                const uriElements = url.split(regexp).filter((element) => {
                    return !!element && element.length > 0 && element != "/";
                });
                if (!uriElements.length || uriElements[0] == "?") {
                    throw new RestServerError(
                        "Request must have a context specified");
                }
                const adapter = this.adapters.get(uriElements[0]);
                if (!adapter) {
                    throw new RestServerError(
                        `Unknown context: '${uriElements[0]}'`, 400);
                }
                adapter.handle(request, response, uriElements);
            }
        } catch (error) {
            this.logger.error("Caught at handle()");
            AdapterError.toResponse(this.logger, error, response);
        }
    }
}

type AdapterRefSpec = {
    context: string;
    adapter: string;
}

type RestServerWorkerSpec = ClassSpec & {
    ports: number[];
    adapters: AdapterRefSpec[];
}

export class RestServerWorker extends DaemonWorker {
    readonly name: string;
    server: Cfg<RestServer>;
    ports: number[];
    adapters: AdapterRefSpec[];
    running: boolean;
    logger: Logger;

    constructor(config: TypeCfg<RestServerWorkerSpec>,
                blueprints: Map<string, any>) {
        super(config, blueprints);
        this.name = config.metadata.name;
        this.ports = config.spec.ports;
        this.adapters = config.spec.adapters;
        this.server = new Cfg("server");
        this.running = false;
        this.logger = new Logger(`server/${this.name}`);
    }

    configure(configuration: IConfiguration): void {
        this.logger.configure(configuration);
        this.server.v = new RestServer(
            this.logger, configuration, this.adapters);
        configuration.registerAsyncTask(this);
    }

    private listenPort(portIndex: number, reject: (error: any) => void): void {
        if (portIndex < this.ports.length &&
            !this.server.v.httpServer.listening) {
            const port = this.ports[portIndex];
            this.server.v.httpServer.listen(port);
        } else {
            reject(new RestServerError(
                `Server '${this.name}' has exhausted all configured ports ` +
                `or it is already listening on another port`));
        }
    }

    start(): Promise<any> {
        this.logger.log("Starting REST server...");
        return new Promise<void>((resolve, reject) => {
            if (!this.server.isSet()) {
                reject(new RestServerError(
                    `Cannot start RestServerWorker '${this.name}' because ` +
                    `its configuration is invalid`));
            } else {
                let portIndex = 0;
                this.server.v.httpServer.on("listening", () => {
                    this.logger.log(
                        `Server '${this.name}' listening on port ` +
                        `${this.ports.at(portIndex) || "????"}`);
                    this.running = true;
                    resolve();
                });
                this.server.v.httpServer.on("error", (error) => {
                    if ((<any>error).code == "EADDRINUSE") {
                        portIndex++;
                        if (portIndex < this.ports.length) {
                            setTimeout(() => {
                                this.server.v.httpServer.close();
                                this.listenPort(portIndex, reject);
                            }, 1000);
                        } else {
                            reject(new RestServerError(
                                `Server '${this.name}' has exhausted all ` +
                                `configured ports and could not find an ` +
                                `available one`));
                        }
                    } else {
                        this.logger.log(
                            `Server '${this.name}' error: ${error}`);
                        reject(error);
                    }
                });
                this.listenPort(portIndex, reject);
            }
        });
    }

    stop(): Promise<any> {
        return new Promise<void>((resolve, reject) => {
            if (this.server.isSet() && this.running) {
                this.server.v.httpServer.close((error) => {
                    if (error) {
                        this.logger.error(
                            `Server '${this.name}' cannot close due to: ` +
                            `${error}`);
                        reject(error);
                    } else {
                        this.logger.log(`Server '${this.name}' closed`);
                        resolve();
                    }
                });
            } else {
                resolve();
            }
        });
    }
}

