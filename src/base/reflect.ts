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

import * as core from "./core.js";

class ReflectError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
    }
}

export type ClassInfo = {
    name: string;
    clazz: any;
}

type ReflectMap = { [key: string]: any };

export class Reflection {
    private allModules: ReflectMap;
    private coreModule: ReflectMap;

    constructor() {
        this.allModules = {};
        this.coreModule = core as ReflectMap;
    }

    async reflect(fqn: string): Promise<ClassInfo> {
        const lastDot = fqn.lastIndexOf(".");
        if (lastDot == -1) {
            // load from core
            const clazz = this.coreModule[fqn];
            if (clazz !== undefined) {
                // console.log(`Reflection complete for ${fqn}`);
                return { name: fqn, clazz: clazz };
            } else {
                throw new ReflectError(
                    `core class not found: ${fqn}`);
            }
        } else {
            const moduleName = fqn.substring(0, lastDot);
            const className = fqn.substring(lastDot + 1);
            let mod;
            if (moduleName in this.allModules) {
                mod = this.allModules[moduleName];
            } else {
                const modulePath = "../" + moduleName.replaceAll(".", "/") +
                    ".js";
                try {
                    mod = await import(modulePath);
                    this.allModules[moduleName] = mod;
                } catch (error) {
                    throw new ReflectError(
                        `Cannot load module ${modulePath}`, { cause: error });
                }
            }
            const clazz = mod[className];
            if (clazz !== undefined) {
                // console.log(`Reflection complete for ${fqn}`);
                return { name: fqn, clazz: clazz };
            } else {
                throw new ReflectError(
                    `Cannot locate class ${className} in module ${moduleName}`);
            }
        }
    }

}

