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

import md5 from "md5";

import {
    _IError, IResultSet, Row, IContext, Entity, Logger, Nobody
} from "../base/core.js";

type VcRecord = {
    seq?: number;
    _id?: string;
    _rev?: string;
    updated?: Date;
    updatedby?: string;
    versiondepth?: number;
    ancestry?: string;
    isleaf: boolean;
    isdeleted: boolean;
    isstub: boolean;
    isconflict: boolean;
    iswinner: boolean;
}

type VcResult = {
    action: "post" | "put";
    record: VcRecord;
}

type LeafAction = {
    type: "none" | "post" | "put" | "swap" | "delete";
    _id?: string;
    _rev?: string;
    payload?: Row;
}

type VersionTableAction = {
    type: "none" | "post";
    payload?: Row;
}

type VcVersion = {
    depth: number;
    hash: string;
}

type DocRevisions = {
    ids: string[];
    start: number;
}

const ENTITY_EXCLUDED = ["_revisions", "updated", "updatedby"];
const HASH_EXCLUDED = ["_id", "_rev", "_revisions", "updated", "updatedby"];

class MvccError extends _IError {
    constructor(message: string, code?: number, options?: ErrorOptions) {
        super(code || 500, message, options);
    }
}

export class MvccResult {
    vcTables: VcResult[];
    versionTable: VersionTableAction;
    leafTable: LeafAction;

    constructor() {
        this.vcTables = [];
        this.versionTable = { type: "none" };
        this.leafTable = { type: "none" };
    }

    addVcTableAction(action: "post" | "put", record: VcRecord) {
        this.vcTables.push({ action: action, record: record });
    }
}

export class MvccController {
    logger: Logger;

    constructor(logger: Logger) {
        this.logger = logger;
    }

    private couchRevsToAncestry(revs: string[]): string {
        return Array.from(revs).reverse().join(".");
    }

    private beats(challenger: Row, current?: Row): Row {
        if (current) {
            const challengerRev = challenger.get("_rev");
            const currentRev = current.get("_rev");
            if (currentRev == challengerRev) {
                return challenger;
            }
            if (!challengerRev) {
                return current;
            }
            const challengerRevDepth = this.versionDepth(challengerRev);
            const currentRevDepth = this.versionDepth(currentRev);
            if (challengerRevDepth > currentRevDepth) {
                return challenger;
            }
            if (currentRevDepth > challengerRevDepth) {
                return current;
            }
            const challengerRevHash = this.versionHash(challengerRev);
            const currentRevHash = this.versionHash(currentRev);
            if (challengerRevHash > currentRevHash) {
                return challenger;
            }
            if (currentRevHash > challengerRevHash) {
                return current;
            }
            // This should never happen
            throw new MvccError("Two identical versions exist");
        } else {
            return challenger;
        }
    }


    private winner(leafs: Row[]): Row {
        if (leafs.length < 2) {
            throw new MvccError(
                "Cannot determine winner from a set that's less than 2");
        }
        let winner: Row | undefined = undefined;
        for (const row of leafs) {
            winner = this.beats(row, winner);
        }
        if (winner) {
            return winner;
        } else {
            throw new MvccError("Cannot determine winner");
        }
    }

    toInteger(input?: any, defValue?: number): number {
        if (input != null && input != undefined) {
            const value = Number(input);
            if (!Number.isNaN(value) && Number.isSafeInteger(value)) {
                return value;
            } else {
                throw new MvccError(
                    `Value ${input} cannot be converted to a Number, or is ` +
                    `too large to a safe integer`);
            }
        } else if (defValue != undefined) {
            return defValue;
        } else {
            throw new MvccError(
                "Number cannot be null or undefined at this point");
        }
    }

    toBigInt(input?: any, defValue?: bigint): bigint {
        if (input != null && input != undefined) {
            try {
                return BigInt(input);
            } catch (error) {
                throw new MvccError(
                    `Cannot transform ${input} to a BigInt`, 500,
                    { cause: error });
            }
        } else if (defValue != undefined) {
            return defValue;
        } else {
            throw new MvccError(
                "Number/BigInt cannot be null or undefined at this point");
        }
    }

    removeNonEntityFields(row: Row): void {
        for (const col of ENTITY_EXCLUDED) {
            row.delete(col);
        }
    }

    convertToPayload(row: Row): void {
        for (const col of HASH_EXCLUDED) {
            row.delete(col);
        }
    }

    private versionHash(rev: string): string {
        const dashPos = rev.indexOf("-");
        if (dashPos == -1 || dashPos == 0 || dashPos >= (rev.length - 1)) {
            throw new MvccError(`Cannot establish hash from rev: '${rev}'`);
        }
        return rev.substring(dashPos + 1);
    }

    versionDepth(rev: string): number {
        const dashPos = rev.indexOf("-");
        if (dashPos == -1 || dashPos == 0) {
            throw new MvccError(`Cannot establish rev depth from rev` +
                                `: '${rev}'`);
        }
        const depthStr = rev.substring(0, dashPos);
        let depthNum;
        try {
            depthNum = Number(depthStr);
        } catch (error) {
            throw new MvccError(
                `Cannot convert ${depthStr} to a number`, 500,
                { cause: error });
        }
        if (!Number.isInteger(depthNum)) {
            throw new MvccError(`Cannot convert ${depthStr} to an integer`);
        }
        return depthNum;
    }

    private newVersion(payload: Row, fromRev?: string): VcVersion {
        if (payload.empty) {
            throw new MvccError(`Cannot create version hash on empty data row`);
        }
        const hash = md5(JSON.stringify(payload.raw()));
        const depth = fromRev ? this.versionDepth(fromRev) + 1 : 1;
        return { depth: depth, hash: hash };
    }

    private putForceDelete(row: Row, versions: IResultSet,
                           context?: IContext): MvccResult {
        const result = new MvccResult();
        const id = row.get("_id");
        const rev = row.get("_rev");
        const updated = row.has("updated") ? row.get("updated") : new Date();
        const updatedBy =
            row.has("updatedby") ? row.get("updatedby") :
            (context ? context.userAccountId : Nobody.ID);
        const revDepth = this.versionDepth(rev);
        const revHash = this.versionHash(rev);
        const rowRevisions = row.has("_revisions") ?
            row.get("_revisions") as DocRevisions : undefined;
        const rowAncestry =
            rowRevisions ? this.couchRevsToAncestry(rowRevisions.ids) : revHash;
        this.convertToPayload(row);
        this.logger.debug("C 001");
        // In all cases, create a vc record for the passed row
        result.addVcTableAction("post", {
            _id: id,
            _rev: rev,
            updated: updated,
            updatedby: updatedBy,
            versiondepth: revDepth,
            ancestry: rowAncestry,
            isleaf: false,
            isdeleted: true,
            isstub: false,
            isconflict: false,
            iswinner: false
        });
        /* Attempt to find the revision that this deletion record points to.
         * If we cannot find it, no further edits will be made.
         */
        if (rowRevisions && rowRevisions.ids.length > 1) {
            this.logger.debug("C 002");
            /* If the 'start - 1' revision in the array matches one of our
             * 'versions' and that match is a (non-deleted) leaf, then delete
             * that leaf.
             */
            const target = `${rowRevisions.start - 1}-${rowRevisions.ids[1]}`;
            const toDeleteRev = versions.find(
                (rec) => rec.get("_rev") == target);
            // Only make further edits if we have a match for this rev.
            if (toDeleteRev) {
                this.logger.debug("C 003");
                if (toDeleteRev.get("isleaf")) {
                    this.logger.debug("C 004");
                    /*
                     * Post a new vc record for the deletion.
                     * Mark the 'start - 1' rev isleaf=false, isconflict=false,
                     * iswinner=false.
                     * If 'start - 1' was the winner and there were no
                     * conflicts, delete the leaf.
                     * Else If there were conflicts, determine the new winner
                     * and if different swap the leaf with the new winner and
                     * then update the 'isconflict' values accordingly based on
                     * the remaining (if any) conflicts.
                     */
                    result.addVcTableAction("put", {
                        seq: toDeleteRev.get("seq"),
                        isleaf: false,
                        isdeleted: false,
                        isstub: false,
                        isconflict: false,
                        iswinner: false
                    });
                    if (toDeleteRev.get("iswinner") &&
                        !toDeleteRev.get("isconflict")) {
                        this.logger.debug("C 008");
                        result.leafTable.type = "delete";
                        result.leafTable._id = id;
                    } else if (toDeleteRev.get("isconflict")) {
                        this.logger.debug("C 009");
                        /* Mark the toDeleteRev row as isleaf=false, and then
                         * determine the new winner, with toDeleteRev now
                         * removed. If this also clears the conflict by having
                         * only one leaf remaining, remove the conflict flag
                         * from the new winner as well.
                         */
                        toDeleteRev.put("isleaf", false);
                        // Find all remaining leafs
                        const leafs = versions.filter(
                            (row) => row.get("isleaf"));
                        if (leafs.length == 0) {
                            this.logger.error("C 011 - EXC");
                            throw new MvccError(
                                `Data integrity issue C 011 - record id ` +
                                `${id} version ${target} is marked ` +
                                `isconflict, but there are no other leaf records`);
                        }
                        if (leafs.length == 1) {
                            this.logger.debug("C 012");
                            // This clears the conflict, only one leaf remaining
                            result.addVcTableAction("put", {
                                seq: leafs[0].get("seq"),
                                isleaf: true,
                                isdeleted: false,
                                isstub: false,
                                isconflict: false,
                                iswinner: true
                            });
                            /* If this last leaf wasn't already the winner, swap
                             * the entity record to this leaf.
                             */
                            if (!leafs[0].get("iswinner")) {
                                this.logger.debug("C 014");
                                result.leafTable.type = "swap";
                                result.leafTable._id = id;
                                result.leafTable._rev = leafs[0].get("_rev");
                            }
                        } else {
                            this.logger.debug("C 013");
                            // Find the old winner, this could be toDeleteRev
                            const oldWinner = versions.find(
                                (row) => row.get("iswinner"));
                            if (oldWinner) {
                                this.logger.debug("C 015");
                                const newWinner = this.winner(leafs);
                                if (newWinner.get("_rev") !=
                                    oldWinner.get("_rev")) {
                                    this.logger.debug("C 016");
                                    // there is a new winner, mark its vc record
                                    result.addVcTableAction("put", {
                                        seq: newWinner.get("seq"),
                                        isleaf: true,
                                        isdeleted: false,
                                        isstub: false,
                                        isconflict: false,
                                        iswinner: true
                                    });
                                    // Swap the entity record to the new winner
                                    result.leafTable.type = "swap";
                                    result.leafTable._id = id;
                                    result.leafTable._rev =
                                        newWinner.get("_rev");
                                }
                                /* The winner hasn't changed, so no other
                                 * modifications are needed
                                 */
                            } else {
                                this.logger.error("C 017 - EXC");
                                throw new MvccError(
                                    `Data integrity issue - record id ${id} ` +
                                    `version ${target} is marked 'isconflict'` +
                                    `, but no record is marked as 'iswinner'`);
                            }
                        }
                    } else {
                        /* There are no conflicts and toDeleteRev wasn't the
                         * current entity record, so no further modifications are
                         * needed.
                         */
                        this.logger.debug("C 010");
                    }
                } else {
                    /* We have a match for the deletion, but it's not a leaf,
                     * no further modification are made.
                     */
                    this.logger.debug("C 007");
                }
            } else {
                this.logger.debug("C 006");
            }
        } else {
            this.logger.debug("C 005");
        }
        return result;
    }

    private putForce(row: Row, versions: IResultSet,
                     context?: IContext): MvccResult {
        /* Force the creation of the row's version, possibly creating a conflict
         * We've already established that there is no existing matching version
         * and that 'forced' is true.
         */
        if (this.logger.willLog("Info") &&
            (!row.has("updated") || !row.has("updatedby"))) {
            this.logger.info(
                `WARNING Row ${row.get("_id")} version ${row.get("_rev")} is ` +
                `missing 'updated' or 'updatedby'`);
        }
        if (row.has("_deleted")) {
            this.logger.debug("C 018");
            return this.putForceDelete(row, versions, context);
        }
        this.logger.debug("C 019");
        const result = new MvccResult();
        const id = row.get("_id");
        const rev = row.get("_rev");
        const updated = row.has("updated") ? row.get("updated") : new Date();
        const updatedBy =
            row.has("updatedby") ? row.get("updatedby") :
            (context ? context.userAccountId : Nobody.ID);
        const revDepth = this.versionDepth(rev);
        const revHash = this.versionHash(rev);
        const rowRevisions = row.has("_revisions") ?
            row.get("_revisions") as DocRevisions : undefined;
        const rowAncestry =
            rowRevisions ? this.couchRevsToAncestry(rowRevisions.ids) : revHash;
        this.removeNonEntityFields(row);
        // Always create the version record for the passed leaf
        result.versionTable.type = "post";
        result.versionTable.payload = row;
        const oldWinner = versions.find((rec) => rec.get("iswinner"));
        const hasConflicts = versions.some((rec) => rec.get("isconflict"));
        // Do some data-integrity sanity checks
        if (oldWinner) {
            const oldWinnerRev = oldWinner.get("_rev");
            if (!oldWinner.get("isleaf") || versions.some(
                (rec) => rec.get("_rev") != oldWinnerRev &&
                rec.get("iswinner"))) {
                this.logger.error("C 076 - EXC");
                throw new MvccError(
                    `VC table inconsistency trying to process id = ${id}, ` +
                    `rev = ${rev}`);
            }
        }
        // Attempt to find the row for this leaf's parent. If we cannot find it,
        // we force the creation of a (possibly conflicted) leaf.
        if (rowRevisions && rowRevisions.ids.length > 1) {
            this.logger.debug("C 077");
            const parentRev =
                `${rowRevisions.start - 1}-${rowRevisions.ids[1]}`;
            const parentRow = versions.find(
                (rec) => rec.get("_rev") == parentRev);
            if (parentRow && parentRow.get("isleaf")) {
                /* The passed row is a new version for the parent, update
                 * the parent's vc record, since it is no longer a leaf.
                 */
                this.logger.debug("C 021");
                result.addVcTableAction("put", {
                    seq: parentRow.get("seq"),
                    isleaf: false,
                    isdeleted: false,
                    isstub: false,
                    isconflict: false,
                    iswinner: false
                });
                if (hasConflicts) {
                    /* The passed leaf replaces parent, and then recalculate
                     * a new winner.
                     * To calculate the new winner, replace the parent with
                     * the passed row in leafs.
                     */
                    this.logger.debug("C 023");
                    const leafs = versions.filter(
                        (rec) => rec.get("_rev") != parentRev &&
                            rec.get("isleaf"));
                    // Add the passed leaf
                    leafs.push(new Row({_rev: rev}));
                    if (oldWinner) {
                        this.logger.debug("C 025");
                        const newWinner = this.winner(leafs);
                        const oldWinnerRev = oldWinner.get("_rev");
                        const newWinnerRev = newWinner.get("_rev");
                        // Add the passed leaf, mark it as winner if so
                        result.addVcTableAction("post", {
                            _id: id,
                            _rev: rev,
                            updated: updated,
                            updatedby: updatedBy,
                            versiondepth: revDepth,
                            ancestry: rowAncestry,
                            isleaf: true,
                            isdeleted: false,
                            isstub: false,
                            isconflict: true,
                            iswinner: (newWinnerRev == rev)
                        });
                        /* If the new winner is the passed leaf, swap the
                         * entity table to it.
                         * If not, check if the new winner has changed and if
                         * so, mark the new winner as such.
                         */
                        if (newWinnerRev == rev) {
                            this.logger.debug("C 027");
                            result.leafTable.type = "swap";
                            result.leafTable.payload = row;
                        } else if (newWinnerRev != oldWinnerRev) {
                            this.logger.debug("C 028");
                            result.addVcTableAction("put", {
                                seq: newWinner.get("seq"),
                                isleaf: true,
                                isdeleted: false,
                                isstub: false,
                                isconflict: true,
                                iswinner: true
                            });
                        } else {
                            this.logger.debug("C 029");
                        }
                        /* If the old winner was not the parent and is no
                         * longer the winner, remove its iswinner mark.
                         */
                        if (oldWinnerRev != parentRev &&
                            newWinnerRev != oldWinnerRev) {
                            this.logger.debug("C 030");
                            result.addVcTableAction("put", {
                                seq: oldWinner.get("seq"),
                                isleaf: true,
                                isdeleted: false,
                                isstub: false,
                                isconflict: true,
                                iswinner: false
                            });
                        } else {
                            this.logger.debug("C 031");
                        }
                    } else {
                        this.logger.error("C 026 - EXC");
                        throw new MvccError(
                            `Record ${id} has revisions in conflict but is` +
                            ` missing an 'iswinner' version`);
                    }
                } else {
                    // No conflicts: the passed leaf replaces parent.
                    this.logger.debug("C 024");
                    result.addVcTableAction("post", {
                        _id: id,
                        _rev: rev,
                        updated: updated,
                        updatedby: updatedBy,
                        versiondepth: revDepth,
                        ancestry: rowAncestry,
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: false,
                        iswinner: true
                    });
                    result.addVcTableAction("put", {
                        seq: parentRow.get("seq"),
                        isleaf: false,
                        isdeleted: false,
                        isstub: false,
                        isconflict: false,
                        iswinner: false
                    });
                    result.leafTable.type = "swap";
                    result.leafTable.payload = row;
                }
            } else {
                /* Unknown parent, or the known parent is not a leaf. Create
                 * the passed leaf, check for conflicts and elect a winner if
                 * needed.
                 */
                this.logger.debug("C 022");
                const leafs = versions.filter((rec) => rec.get("isleaf"));
                if (leafs.length == 1) {
                    this.logger.debug("C 032");
                    /* We are introducing a conflict, because there was only one
                     * leaf and now we're adding another.
                     */
                    const originalLeaf = leafs[0];
                    /* Add the passed row as a leaf and then determine the
                     * winner.
                     */
                    leafs.push(new Row({_rev: rev}));
                    const winnerRev = this.winner(leafs).get("_rev");
                    // Add the passed leaf, mark it as winner if so
                    result.addVcTableAction("post", {
                        _id: id,
                        _rev: rev,
                        updated: updated,
                        updatedby: updatedBy,
                        versiondepth: revDepth,
                        ancestry: rowAncestry,
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: true,
                        iswinner: (winnerRev == rev)
                    });
                    /* If the passed row is the winner, swap the leaf table to
                     * it.
                     */
                    if (winnerRev == rev) {
                        this.logger.debug("C 035");
                        result.leafTable.type = "swap";
                        result.leafTable.payload = row;
                    } else {
                        this.logger.debug("C 036");
                    }
                    // Update the original leaf record
                    result.addVcTableAction("put", {
                        seq: originalLeaf.get("seq"),
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: true,
                        iswinner: !(winnerRev == rev)
                    });
                } else if (leafs.length > 1) {
                    this.logger.debug("C 033");
                    /* We are adding more conflicts to an already conflicted
                     * record.
                     * Add the passed row and recalculate the winner.
                     */
                    leafs.push(new Row({_rev: rev}));
                    const winnerRev = this.winner(leafs).get("_rev");
                    // Add the passed leaf, mark it as winner if so
                    result.addVcTableAction("post", {
                        _id: id,
                        _rev: rev,
                        updated: updated,
                        updatedby: updatedBy,
                        versiondepth: revDepth,
                        ancestry: rowAncestry,
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: true,
                        iswinner: (winnerRev == rev)
                    });
                    /* If the passed row is the winner, swap the leaf table to
                     * it.
                     */
                    if (winnerRev == rev) {
                        this.logger.debug("C 037");
                        result.leafTable.type = "swap";
                        result.leafTable.payload = row;
                    } else {
                        this.logger.debug("C 038");
                    }
                    // Update the old winner, if it has changed
                    if (oldWinner) {
                        this.logger.debug("C 039");
                        if (oldWinner.get("_rev") != winnerRev) {
                            this.logger.debug("C 039");
                            result.addVcTableAction("put", {
                                seq: oldWinner.get("seq"),
                                isleaf: true,
                                isdeleted: false,
                                isstub: false,
                                isconflict: true,
                                iswinner: false
                            });
                        } else {
                            this.logger.debug("C 040");
                        }
                    } else {
                        this.logger.error("C 041 - EXC");
                        throw new MvccError(
                            `Record ${id} has revisions in conflict but is` +
                            ` missing an 'iswinner' version`);
                    }
                } else {
                    /* The passed leaf is either a new record, or a
                     * resurrection of a deleted record, since there were no
                     * leafs prior. In both cases, we simply create the leaf.
                     */
                    this.logger.debug("C 034");
                    // Add the passed leaf
                    result.addVcTableAction("post", {
                        _id: id,
                        _rev: rev,
                        updated: updated,
                        updatedby: updatedBy,
                        versiondepth: revDepth,
                        ancestry: rowAncestry,
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: false,
                        iswinner: true
                    });
                    result.leafTable.type = "post";
                    result.leafTable.payload = row;
                }
            }
        } else if (rowRevisions && rowRevisions.ids.length == 1) {
            /* The is a 'post' of a single version. If there are no existing
             * versions then treat it as a 'post', otherwise create a conflicing
             * version and calculate the winner
             */
            this.logger.debug("C 020");
            if (!versions.rowCount) {
                this.logger.debug("C 042");
                // This is the first version for this _id
                result.addVcTableAction("post", {
                    _id: id,
                    _rev: rev,
                    updated: updated,
                    updatedby: updatedBy,
                    versiondepth: revDepth,
                    ancestry: rowAncestry,
                    isleaf: true,
                    isdeleted: false,
                    isstub: false,
                    isconflict: false,
                    iswinner: true
                });
            } else {
                this.logger.debug("C 043");
                const leafs = versions.filter((rec) => rec.get("isleaf"));
                if (leafs.length) {
                    this.logger.debug("C 044");
                    // To calculate the winner, add the passed leaf
                    leafs.push(new Row({_rev: rev}));
                    const winnerRev = this.winner(leafs).get("_rev");
                    if (oldWinner && oldWinner.get("_rev") != winnerRev) {
                        this.logger.debug("C 046");
                        // Unmark the old winner
                        result.addVcTableAction("put", {
                            seq: oldWinner.get("seq"),
                            isleaf: true,
                            isdeleted: false,
                            isstub: false,
                            isconflict: true,
                            iswinner: false
                        });
                    } else {
                        this.logger.debug("C 047");
                    }
                    // Add the passed row, mark it as winner if needed
                    result.addVcTableAction("post", {
                        _id: id,
                        _rev: rev,
                        updated: updated,
                        updatedby: updatedBy,
                        versiondepth: revDepth,
                        ancestry: rowAncestry,
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: true,
                        iswinner: (winnerRev == rev)
                    });
                } else {
                    this.logger.debug("C 045");
                    // No existing leafs found, this is automatically the winner
                    result.addVcTableAction("post", {
                        _id: id,
                        _rev: rev,
                        updated: updated,
                        updatedby: updatedBy,
                        versiondepth: revDepth,
                        ancestry: rowAncestry,
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: false,
                        iswinner: true
                    });
                }
            }
            // Add the leaf table
            result.leafTable.type = "post";
            result.leafTable.payload = row;
        } else {
            this.logger.debug("C 021");
        }
        return result;
    }

    private putNoForce(row: Row, versions: IResultSet, vcRow: Row,
                       context?: IContext): MvccResult {
        this.logger.debug("C 048");
        const result = new MvccResult();
        const id = row.get("_id");
        const rev = row.get("_rev");
        // No conflicts may exist
        const conflicts = versions.find((rec) => rec.get("isconflict"));
        if (conflicts) {
            this.logger.error("C 049 - EXC");
            throw new MvccError(
                `Version conflict: id = ${id}, rev = ${rev}`, 409);
        }
        // Current version must be a leaf and the winner
        if (!vcRow.get("isleaf") || !vcRow.get("iswinner")) {
            this.logger.error("C 050 - EXC");
            throw new MvccError(
                `Version conflict: id = ${id}, rev = ${rev}`, 409);
        }
        // Current version must be the only leaf and winner
        if (versions.some((rec) => rec.get("_rev") != rev &&
                          (rec.get("isleaf") || rec.get("iswinner")))) {
            this.logger.error("C 075 - EXC");
            throw new MvccError(
                `Version conflict: id = ${id}, rev = ${rev}`, 409);
        }
        const updated = new Date();
        const updatedBy = context ? context.userAccountId : Nobody.ID;
        this.convertToPayload(row);
        const newRev = this.newVersion(row, rev);
        const newRevString = `${newRev.depth}-${newRev.hash}`;
        const newAncestry =
            vcRow.get("ancestry") ? `${vcRow.get("ancestry")}.${newRev.hash}`
                : newRev.hash;
        row.add("_id", id);
        row.add("_rev", newRevString);
        result.addVcTableAction("post", {
            _id: id,
            _rev: newRevString,
            updated: updated,
            updatedby: updatedBy,
            versiondepth: newRev.depth,
            ancestry: newAncestry,
            isleaf: true,
            isdeleted: false,
            isstub: false,
            isconflict: false,
            iswinner: true
        });
        // Unmark the old winner
        result.addVcTableAction("put", {
            seq: vcRow.get("seq"),
            isleaf: false,
            isdeleted: false,
            isstub: false,
            isconflict: false,
            iswinner: false
        });
        result.versionTable.type = "post";
        result.versionTable.payload = row;
        result.leafTable.type = "put";
        result.leafTable.payload = row;
        result.leafTable._rev = rev;
        return result;
    }

    putMvcc(row: Row, versions: IResultSet, force: boolean,
            context?: IContext): MvccResult {
        if (!row.has("_id") || row.isNull("_id") || !row.has("_rev") ||
            row.isNull("_rev")) {
            this.logger.error("C 051 - EXC");
            throw new MvccError("Missing _id or _rev in row");
        }
        const id = row.get("_id");
        const rev = row.get("_rev");
        const vcRow = versions.find((rec) => rec.get("_rev") == rev);
        if (!vcRow && !force) {
            this.logger.error("C 052 - EXC");
            throw new MvccError(`Not found: id = ${id}, rev = ${rev}`, 404);
        }
        if (vcRow && force) {
            this.logger.error("C 053 - EXC");
            throw new MvccError(
                `Duplicate record: id = ${id}, rev = ${rev}`, 500);
        }
        if (vcRow && !force) {
            this.logger.debug("C 054");
            if (row.has("_deleted")) {
                this.logger.error("C 055 - EXC");
                throw new MvccError(
                    "Deletion must be done via the deleteMvcc() API call");
            }
            this.logger.debug("C 056");
            return this.putNoForce(row, versions, vcRow, context);
        }
        this.logger.debug("C 057");
        return this.putForce(row, versions, context);
    }

    deleteMvcc(id: string, rev: string, versions: IResultSet,
               context: IContext): MvccResult {
        this.logger.debug("C 058");
        const result = new MvccResult();
        const toDeleteVC = versions.find((rec) => rec.get("_rev") == rev);
        if (toDeleteVC) {
            this.logger.debug("C 059");
            if (toDeleteVC.get("isleaf")) {
                this.logger.debug("C 061");
                /* Add the deleted tombstone. Calculate the hash from _id,
                 * updated, updatedby and _deleted: true.
                 * This is different from the normal hash calculation,
                 * which excludes all those fields.
                 */
                const updated = new Date();
                const updatedBy = context.userAccountId;
                const tombstoneRow = new Row({
                    _id: id,
                    updated: updated,
                    updatedby: updatedBy,
                    _deleted: true
                });
                const newRev = this.newVersion(tombstoneRow, rev);
                const newRevString = `${newRev.depth}-${newRev.hash}`;
                const newAncestry =
                    toDeleteVC.get("ancestry") ?
                    `${toDeleteVC.get("ancestry")}.${newRev.hash}` :
                    newRev.hash;
                result.addVcTableAction("post", {
                    _id: id,
                    _rev: newRevString,
                    updated: updated,
                    updatedby: updatedBy,
                    versiondepth: newRev.depth,
                    ancestry: newAncestry,
                    isleaf: false,
                    isdeleted: true,
                    isstub: false,
                    isconflict: false,
                    iswinner: false
                });
                // Mark the vc record as non-leaf
                result.addVcTableAction("put", {
                    seq: toDeleteVC.get("seq"),
                    isleaf: false,
                    isdeleted: false,
                    isstub: false,
                    isconflict: false,
                    iswinner: false
                });
                if (toDeleteVC.get("isconflict")) {
                    this.logger.debug("C 063");
                    const remainingConflicts = versions.filter(
                        (rec) => rec.get("_rev") != rev &&
                            rec.get("isconflict"));
                    if (remainingConflicts.length == 1) {
                        this.logger.debug("C 065");
                        /* Clear the conflict status.
                         * If the remaining record wasn't the winner, mark it as
                         * the winner and swap the leaf record.
                         */
                        const newWinner = remainingConflicts[0];
                        result.addVcTableAction("put", {
                            seq: newWinner.get("seq"),
                            isleaf: true,
                            isdeleted: false,
                            isstub: false,
                            isconflict: false,
                            iswinner: true
                        });
                        if (!newWinner.get("iswinner")) {
                            /* The remaining conflict is now the winner, swap
                             * the leaf record.
                             */
                            result.leafTable.type = "swap";
                            result.leafTable._id = id;
                            result.leafTable._rev = newWinner.get("_rev");
                        }
                    } else if (remainingConflicts.length > 1) {
                        this.logger.debug("C 066");
                        /* There were more than 1 conflicting records,
                         * determine the new winner with the target removed,
                         * which could be the same as the current winner.
                         * The old winner was either one of the remaining
                         * conflicts, or the targeted version.
                         */
                        const oldWinner = versions.find(
                            (rec) => rec.get("iswinner"));
                        if (oldWinner) {
                            this.logger.debug("C 068");
                            const newWinner = this.winner(remainingConflicts);
                            if (!newWinner.get("iswinner")) {
                                this.logger.debug("C 070");
                                /* Mark the new winner as iswinner.
                                 * If the old winner isn't the targeted rev,
                                 * unmark it.
                                 */
                                result.addVcTableAction("put", {
                                    seq: newWinner.get("seq"),
                                    isleaf: true,
                                    isdeleted: false,
                                    isstub: false,
                                    isconflict: true,
                                    iswinner: true
                                });
                                // Swap the leaf record to the new winner
                                result.leafTable.type = "swap";
                                result.leafTable._id = id;
                                result.leafTable._rev = newWinner.get("_rev");
                                if (oldWinner.get("_rev") != rev) {
                                    this.logger.debug("C 072");
                                    result.addVcTableAction("put", {
                                        seq: oldWinner.get("seq"),
                                        isleaf: true,
                                        isdeleted: false,
                                        isstub: false,
                                        isconflict: true,
                                        iswinner: false
                                    });
                                } else {
                                    this.logger.debug("C 073");
                                }
                            } else {
                                // Else: do nothing, the winner hasn't changed
                                this.logger.debug("C 071");
                            }
                        } else {
                            this.logger.error("C 069 - EXC");
                            // This should never happen
                            throw new MvccError(
                                `id ${id} has leaf version(s) but no record ` +
                                `marked as winner`);
                        }
                    } else {
                        this.logger.error("C 067 - EXC");
                        // This should never happen
                        throw new MvccError(
                            `id ${id}, rev ${rev} was marked isconflict, but ` +
                            `there were no other records in conflict`);
                    }
                } else {
                    this.logger.debug("C 064");
                    // No conflicts, remove the leaf record
                    result.leafTable.type = "delete";
                    result.leafTable._id = id;
                    result.leafTable._rev = rev;
                }
            } else {
                this.logger.error("C 062 - EXC");
                throw new MvccError(`${id}, rev ${rev} is not a leaf`, 404);
            }
        } else {
            this.logger.error("C 060 - EXC");
            throw new MvccError(`Cannot find rev ${rev} to delete`, 404);
        }
        return result;
    }

    postMvcc(row: Row, context: IContext): MvccResult {
        this.logger.debug("C 074");
        const result = new MvccResult();
        this.convertToPayload(row);
        const id = Entity.generateId();
        const updated = new Date();
        const updatedBy = context.userAccountId;
        const rev = this.newVersion(row);
        const revString = `${rev.depth}-${rev.hash}`;
        row.add("_id", id);
        row.add("_rev", revString);
        result.addVcTableAction("post", {
            _id: id,
            _rev: revString,
            updated: updated,
            updatedby: updatedBy,
            versiondepth: rev.depth,
            ancestry: rev.hash,
            isleaf: true,
            isdeleted: false,
            isstub: false,
            isconflict: false,
            iswinner: true
        });
        result.versionTable.type = "post";
        result.versionTable.payload = row;
        result.leafTable.type = "post";
        result.leafTable.payload = row;
        return result;
    }

    ancestryToAncestors(ancestry: string): string[] {
        const ancestors: string[] = ancestry.split(".");
        for (let depth = ancestors.length; depth > 0; depth--) {
            const ancestor = `${depth}-${ancestors[depth - 1]}`;
            ancestors[depth - 1] = ancestor;
        }
        return ancestors;
    }
}

