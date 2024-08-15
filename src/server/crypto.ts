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

import { Buffer } from "node:buffer";

import {
    _IError, FieldCfg, StringField, Entity, Phase, State, FieldState,
    IContext, SideEffects
} from "../base/core.js";

class CryptoError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export type JwkType = {
    n: string;
    e: string;
}

export class Jwk {
    jwk: JwkType;

    constructor(from: string | JwkType) {
        if (typeof from === "string") {
            this.jwk = JSON.parse(from) as JwkType;
        } else {
            this.jwk = <JwkType>from;
        }
    }

    bytesNeeded(input: number): number {
        const bitsNeeded = Math.floor(Math.log2(input)) + 1;
        return Math.ceil(bitsNeeded / 8);
    }

    toPEM(isPrivate?: boolean): string {
        const nBuffer = Buffer.from(this.jwk.n, "base64url");
        const eBuffer = Buffer.from(this.jwk.e, "base64url");

        if (!nBuffer.length || !eBuffer.length) {
            throw new CryptoError("Modulus and/or Exponent of key is empty");
        }

        const nLenBytes = this.bytesNeeded(nBuffer.length);
        const eLenBytes = this.bytesNeeded(eBuffer.length);

        const nPadded = !!(0x80 & nBuffer[0]);
        const ePadded = !!(0x80 & eBuffer[0]);

        const nLen = 1 +
            (nBuffer.length > 0x7F ? nLenBytes + 1 : 1) +
            (nPadded ? 1 : 0) +
            nBuffer.length;

        const eLen = 1 +
            (eBuffer.length > 0x7F ? eLenBytes + 1 : 1) +
            (ePadded ? 1 : 0) +
            eBuffer.length;

        const payloadLen = nLen + eLen;
        const payloadLenBytes = this.bytesNeeded(payloadLen);

        const seqHeaderLen = 1 +
            (payloadLen > 0x7F ? payloadLenBytes + 1 : 1);

        const outBuf = Buffer.alloc(seqHeaderLen + nLen + eLen);
        let offset = 0;

        /* Sequence
         */
        offset = outBuf.writeUInt8(0x30, offset);
        if (payloadLenBytes > 1) {
            // LENGTH
            offset = outBuf.writeUInt8(0x80 | payloadLenBytes, offset);
            offset = outBuf.writeIntBE(payloadLen, offset, payloadLenBytes);
        } else {
            offset = outBuf.writeUInt8(payloadLen, offset); // LENGTH
        }

        /* Integer - Modulus 'n'
         */
        offset = outBuf.writeUInt8(0x02, offset); // INTEGER
        if (nLenBytes > 1) {
            // LENGTH
            offset = outBuf.writeUInt8(0x80 | nLenBytes, offset);
            offset = outBuf.writeIntBE(nLen, offset, nLenBytes);
        } else {
            offset = outBuf.writeUInt8(nLen, offset); // LENGTH
        }
        /* Modulus payload
         */
        if (nPadded) {
            offset = outBuf.writeUInt8(0x00, offset); // integer-pad
        }
        offset += nBuffer.copy(outBuf, offset);

        /* Integer - Exponent 'e'
         */
        offset = outBuf.writeUInt8(0x02, offset); // INTEGER
        if (eLenBytes > 1) {
            // LENGTH
            offset = outBuf.writeUInt8(0x80 | eLenBytes, offset);
            offset = outBuf.writeIntBE(eLen, offset, eLenBytes);
        } else {
            offset = outBuf.writeUInt8(eLen, offset); // LENGTH
        }
        /* Exponent payload
         */
        if (ePadded) {
            offset = outBuf.writeUInt8(0x00, offset); // integer-pad
        }
        offset += eBuffer.copy(outBuf, offset);
        const b64Encoded = outBuf.toString("base64");

        const keyType = isPrivate ? "PRIVATE": "PUBLIC";
        const outputArray: string[] = [];
        outputArray.push(`-----BEGIN ${keyType} KEY-----`);
        for (let index = 0; index < b64Encoded.length; index += 64) {
            outputArray.push(b64Encoded.slice(index, index + 63));
        }
        outputArray.push(`-----END ${keyType} KEY-----`);
        return outputArray.join(`\n`);
    }
}

type PasswordFieldCfg = FieldCfg & {
    minLength: number;
    specials: string;
    minSpecials: number;
    minNumbers: number;
    algorithm: string;
}

export class PasswordField extends StringField {
    minLength: number;
    specials: string;
    minSpecials: number;
    minNumbers: number;
    algorithm: string;

    static async digest(algorithm: string, input: string,
                        salt: string): Promise<string> {
        const toDigest = `${input}${salt}`;
        // encode as (utf-8) Uint8Array
        const data = new TextEncoder().encode(toDigest);
        const digest =
            await crypto.subtle.digest(algorithm, data);
        // convert buffer to byte array
        const digestArray = Array.from(new Uint8Array(digest));
        const digestHex = digestArray
            .map((b) => b.toString(16).padStart(2, "0"))
            .join(""); // convert bytes to hex string
        return digestHex;
    }

    constructor(entity: Entity, config: PasswordFieldCfg) {
        super(entity, config);
        if (!config.specials || !config.minLength || !config.minSpecials ||
           !config.minNumbers || !config.algorithm) {
            throw new CryptoError(`${this.fqName}: invalid configuration`);
        }
        this.minLength = config.minLength;
        this.specials = config.specials;
        this.minSpecials = config.minSpecials;
        this.minNumbers = config.minNumbers;
        this.algorithm = config.algorithm;
    }

    async validate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<void> {
        await super.validate(phase, state, fieldState, context);
        if (phase == "set" && fieldState.dirtyNotNull) {
            const clearText = fieldState.asString;
            if (clearText.length < this.minLength) {
                throw new CryptoError(
                    `${this.fqName}: must be at least ${this.minLength} ` +
                    `characters`);
            }
            const nums = /[0-9]+/g;
            const numSearch = clearText.match(nums);
            if (!numSearch || numSearch.length < this.minNumbers) {
                throw new CryptoError(
                    `${this.fqName} must have at least ${this.minNumbers} ` +
                    ` number(s)`);
            }
            let specCount = 0;
            for (const spec of this.specials) {
                let pos = clearText.indexOf(spec);
                while (pos != -1 && specCount < this.minSpecials) {
                    specCount++;
                    pos = clearText.indexOf(spec, pos + 1);
                }
                if (specCount >= this.minSpecials) {
                    break;
                }
            }
            if (specCount < this.minSpecials) {
                throw new CryptoError(
                    `${this.fqName} does not have the required number of ` +
                    `special characters: ${this.minSpecials} from ` +
                    `${this.specials} `);
            }
        }
    }

    async activate(phase: Phase, state: State, fieldState: FieldState,
                   context: IContext): Promise<SideEffects> {
        const sideEffects =
            await super.activate(phase, state, fieldState, context);
        if (phase == "set" && fieldState.dirtyNotNull) {
            const salt = crypto.randomUUID();
            const digest = await PasswordField.digest(
                this.algorithm, fieldState.asString, salt);
            fieldState.value = `${this.algorithm}/${salt}/${digest}`;
        }
        return sideEffects;
    }
}

