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
    _IError, IResultSet, Row, Nobody, IContext, Entity
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
    type: "none" | "post" | "swap" | "delete";
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

function couchRevsToAncestry(revs: string[]): string {
    return Array.from(revs).reverse().join(".");
}

function beats(challenger: Row, current?: Row): Row {
    if (current) {
        const challengerRev = challenger.get("_rev");
        const currentRev = current.get("_rev");
        if (currentRev == challengerRev) {
            return challenger;
        }
        if (!challengerRev) {
            return current;
        }
        const challengerRevDepth = versionDepth(challengerRev);
        const currentRevDepth = versionDepth(currentRev);
        if (challengerRevDepth > currentRevDepth) {
            return challenger;
        }
        if (currentRevDepth > challengerRevDepth) {
            return current;
        }
        const challengerRevHash = versionHash(challengerRev);
        const currentRevHash = versionHash(currentRev);
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


function winner(leafs: Row[]): Row {
    if (leafs.length < 2) {
        throw new MvccError(
            "Cannot determine winner from a set that's less than 2");
    }
    let winner: Row | undefined = undefined;
    for (const row of leafs) {
        winner = beats(row, winner);
    }
    if (winner) {
        return winner;
    } else {
        throw new MvccError("Cannot determine winner");
    }
}

export function convertToPayload(row: Row): void {
    for (const col of HASH_EXCLUDED) {
        row.delete(col);
    }
}

function versionHash(rev: string): string {
    const dashPos = rev.indexOf("-");
    if (dashPos == -1 || dashPos == 0 || dashPos >= (rev.length - 1)) {
        throw new MvccError(`Cannot establish hash from rev: '${rev}'`);
    }
    return rev.substring(dashPos + 1);
}

export function versionDepth(rev: string): number {
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
            `Cannot convert ${depthStr} to a number`, 500, { cause: error });
    }
    if (!Number.isInteger(depthNum)) {
        throw new MvccError(`Cannot convert ${depthStr} to an integer`);
    }
    return depthNum;
}

function newVersion(payload: Row, fromRev?: string): VcVersion {
    if (payload.empty) {
        throw new MvccError(`Cannot create version hash on empty data row`);
    }
    const hash = md5(JSON.stringify(payload.raw()));
    const depth = fromRev ? versionDepth(fromRev) : 1;
    return { depth: depth, hash: hash };
}

function putForceDelete(row: Row, versions: IResultSet): MvccResult {
    const result = new MvccResult();
    const id = row.get("_id");
    const rev = row.get("_rev");
    const updated = row.has("updated") ? row.get("updated") : new Date();
    const updatedBy = row.has("updatedby") ? row.get("updatedby") : Nobody.ID;
    const revDepth = versionDepth(rev);
    const revHash = versionHash(rev);
    const rowRevisions = row.has("_revisions") ?
        row.get("_revisions") as DocRevisions : undefined;
    const rowAncestry =
        rowRevisions ? couchRevsToAncestry(rowRevisions.ids) : revHash;
    convertToPayload(row);
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
    // Attempt to find the revision that this deletion record points to. If we
    // cannot find it, no further edits will be made.
    if (rowRevisions && rowRevisions.ids.length > 1) {
        /* If the 'start - 1' revision in the array matches one of our
         * 'versions' and that match is a (non-deleted) leaf, then delete
         * that leaf.
         */
        const target = `${rowRevisions.start - 1}-${rowRevisions.ids[1]}`;
        const toDeleteRev = versions.find((rec) => rec.get("_rev") == target);
        // Only make further edits if we have a match for this rev.
        if (toDeleteRev) {
            if (toDeleteRev.get("isleaf")) {
                /*
                 * Post a new vc record for the deletion.
                 * Mark the 'start - 1' rev isleaf=false, isconflict=false,
                 * iswinner=false.
                 * If 'start - 1' was the winner and there were no conflicts,
                 * delete the leaf.
                 * Else If there were conflicts, determine the new winner and
                 * if different swap the leaf with the new winner and then
                 * update the 'isconflict' values accordingly based on the
                 * remaining (if any) conflicts.
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
                    result.leafTable.type = "delete";
                    result.leafTable._id = id;
                } else if (toDeleteRev.get("isconflict")) {
                    /* Mark the toDeleteRev row as isleaf=false, and then
                     * determine the new winner, with toDeleteRev now removed.
                     * If this also clears the conflict by having only one leaf
                     * remaining, remove the conflict flag from the new winner
                     * as well.
                     */
                    toDeleteRev.put("isleaf", false);
                    // Find all remaining leafs
                    const leafs = versions.filter((row) => row.get("isleaf"));
                    if (leafs.length == 0) {
                        throw new MvccError(
                            `Data integrity issue - record id ${id} version ` +
                            `${target} is marked inconflict, but there are ` +
                            `no other leaf records`);
                    }
                    if (leafs.length == 1) {
                        // This clears the conflict, only one leaf remaining
                        result.addVcTableAction("put", {
                            seq: leafs[0].get("seq"),
                            isleaf: true,
                            isdeleted: false,
                            isstub: false,
                            isconflict: false,
                            iswinner: true
                        });
                        // If this last leaf wasn't already the winner, swap
                        // the entity record to this leaf.
                        if (!leafs[0].get("iswinner")) {
                            result.leafTable.type = "swap";
                            result.leafTable._id = id;
                            result.leafTable._rev = leafs[0].get("_rev");
                        }
                    } else {
                        // Find the old winner, this could be toDeleteRev still
                        const oldWinner = versions.find(
                            (row) => row.get("iswinner"));
                        if (oldWinner) {
                            const newWinner = winner(leafs);
                            if (newWinner.get("_rev") !=
                                oldWinner.get("_rev")) {
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
                                result.leafTable._rev = newWinner.get("_rev");
                            }
                            // The winner hasn't changed, so no other
                            // modifications are needed
                        } else {
                            throw new MvccError(
                                `Data integrity issue - record id ${id} ` +
                                `version ${target} is marked 'inconflict', ` +
                                `but no record is marked as 'iswinner'`);
                        }
                    }
                }
                // Else: There are no conflicts and toDeleteRev wasn't the
                // current entity record, so no further modifications are
                // needed.
            }
            // Else: We have a match for the deletion, but it's not a leaf, no
            // further modification are made.
        }
    }
    return result;
}

function putForce(row: Row, versions: IResultSet): MvccResult {
    /* Force the creation of the row's version, possibly creating a conflict
     * We've already established that there is no existing matching version and
     * that 'forced' is true.
     */
    if (row.has("_deleted")) {
        return putForceDelete(row, versions);
    }
    const result = new MvccResult();
    const id = row.get("_id");
    const rev = row.get("_rev");
    const updated = row.has("updated") ? row.get("updated") : new Date();
    const updatedBy = row.has("updatedby") ? row.get("updatedby") : Nobody.ID;
    const revDepth = versionDepth(rev);
    const revHash = versionHash(rev);
    const rowRevisions = row.has("_revisions") ?
        row.get("_revisions") as DocRevisions : undefined;
    const rowAncestry =
        rowRevisions ? couchRevsToAncestry(rowRevisions.ids) : revHash;
    convertToPayload(row);
    // Always create the version record for the passed leaf
    result.versionTable.type = "post";
    result.versionTable.payload = row;
    const oldWinner = versions.find((row) => row.get("iswinner"));
    const hasConflicts = versions.some((row) => row.get("isconflict"));
    // Attempt to find the row for this leaf's parent. If we cannot find it,
    // we force the creation of a (possibly conflicted) leaf.
    if (rowRevisions && rowRevisions.ids.length > 1) {
        const parentRev = `${rowRevisions.start - 1}-${rowRevisions.ids[1]}`;
        const parentRow = versions.find((rec) => rec.get("_rev") == parentRev);
        if (parentRow && parentRow.get("isleaf")) {
            /* The passed row is a new version for the parent, update
             * the parent's vc record, since it is no longer a leaf.
             */
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
                const leafs = versions.filter(
                    (row) => row.get("_rev") != parentRev &&
                        row.get("isleaf"));
                // Add the passed leaf
                leafs.push(new Row({_rev: rev}));
                if (oldWinner) {
                    const newWinner = winner(leafs);
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
                        result.leafTable.type = "swap";
                        result.leafTable.payload = row;
                    } else if (newWinnerRev != oldWinnerRev) {
                        result.addVcTableAction("put", {
                            seq: newWinner.get("seq"),
                            isleaf: true,
                            isdeleted: false,
                            isstub: false,
                            isconflict: true,
                            iswinner: true
                        });
                    }
                    /* If the old winner was not the parent and is no
                     * longer the winner, remove its iswinner mark.
                     */
                    if (oldWinnerRev != parentRev &&
                        newWinnerRev != oldWinnerRev) {
                        result.addVcTableAction("put", {
                            seq: oldWinner.get("seq"),
                            isleaf: true,
                            isdeleted: false,
                            isstub: false,
                            isconflict: true,
                            iswinner: false
                        });
                    }
                } else {
                    throw new MvccError(
                        `Record ${id} has revisions in conflict but is` +
                        ` missing an 'iswinner' version`);
                }
            } else {
                // No conflicts: the passed leaf replaces parent.
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
            /* Unknown parent, or the known parent is not a leaf. Create the
             * passed leaf, check for conflicts and elect a winner if needed.
             */
            const leafs = versions.filter((row) => row.get("isleaf"));
            if (leafs.length == 1) {
                /* We are introducing a conflict, because there was only one
                 * leaf and now we're adding another.
                 */
                const originalLeaf = leafs[0];
                // Add the passed row as a leaf and then determine the winner
                leafs.push(new Row({_rev: rev}));
                const winnerRev = winner(leafs).get("_rev");
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
                // If the passed row is the winner, swap the leaf table to it
                if (winnerRev == rev) {
                    result.leafTable.type = "swap";
                    result.leafTable.payload = row;
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
                /* We are adding more conflicts to an already conflicted record
                 * Add the passed row and recalculate the winner.
                 */
                leafs.push(new Row({_rev: rev}));
                const winnerRev = winner(leafs).get("_rev");
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
                // If the passed row is the winner, swap the leaf table to it
                if (winnerRev == rev) {
                    result.leafTable.type = "swap";
                    result.leafTable.payload = row;
                }
                // Update the old winner, if it has changed
                if (oldWinner) {
                    if (oldWinner.get("_rev") != winnerRev) {
                        result.addVcTableAction("put", {
                            seq: oldWinner.get("seq"),
                            isleaf: true,
                            isdeleted: false,
                            isstub: false,
                            isconflict: true,
                            iswinner: false
                        });
                    }
                } else {
                    throw new MvccError(
                        `Record ${id} has revisions in conflict but is` +
                        ` missing an 'iswinner' version`);
                }
            } else {
                /* The passed leaf is either a new record, or a resurrection of
                 * a deleted record, since there were no leafs prior. In both
                 * cases, we simply create the leaf.
                 */
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
                row.add("_id", id);
                row.add("_rev", rev);
                result.leafTable.type = "post";
                result.leafTable.payload = row;
            }
        }
    } else if (rowRevisions && rowRevisions.ids.length == 1) {
        /* The is a 'post' of a single version. If there are no existing
         * versions then treat it as a 'post', otherwise create a conflicing
         * version and calculate the winner
         */
        if (!versions.rowCount) {
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
            const leafs = versions.filter((row) => row.get("isleaf"));
            if (leafs.length) {
                // To calculate the winner, add the passed leaf
                leafs.push(new Row({_rev: rev}));
                const winnerRev = winner(leafs).get("_rev");
                if (oldWinner && oldWinner.get("_rev") != winnerRev) {
                    // Unmark the old winner
                    result.addVcTableAction("put", {
                        seq: oldWinner.get("seq"),
                        isleaf: true,
                        isdeleted: false,
                        isstub: false,
                        isconflict: true,
                        iswinner: false
                    });
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
        row.add("_id", id);
        row.add("_rev", rev);
        result.leafTable.type = "post";
        result.leafTable.payload = row;
    }
    return result;
}

function putNoForce(row: Row, versions: IResultSet,
                    vcRow: Row): MvccResult {
    const result = new MvccResult();
    const id = row.get("_id");
    const rev = row.get("_rev");
    // No conflicts may exist
    const conflicts = versions.find((rec) => rec.get("isconflict"));
    if (conflicts) {
        throw new MvccError(
            `Version conflict: id = ${id}, rev = ${rev}`, 409);
    }
    // Current version must be a leaf and the winner
    if (!vcRow.get("isleaf") || !vcRow.get("iswinner")) {
        throw new MvccError(
            `Version conflict: id = ${id}, rev = ${rev}`, 409);
    }
    const updated = row.has("updated") ? row.get("updated") : new Date();
    const updatedBy = row.has("updatedby") ? row.get("updatedby") : Nobody.ID;
    convertToPayload(row);
    const newRev = newVersion(row, rev);
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
    result.versionTable.type = "post";
    result.versionTable.payload = row;
    result.leafTable.type = "post";
    result.leafTable.payload = row;
    return result;
}

export function putMvcc(row: Row, versions: IResultSet,
                        force: boolean): MvccResult {
    if (!row.has("_id") || row.isNull("_id") || !row.has("_rev") ||
        row.isNull("_rev")) {
        throw new MvccError("Missing _id or _rev in row");
    }
    const id = row.get("_id");
    const rev = row.get("_rev");
    const vcRow = versions.find((rec) => rec.get("_rev") == rev);
    if (!vcRow && !force) {
        throw new MvccError(`Not found: id = ${id}, rev = ${rev}`, 404);
    }
    if (vcRow && force) {
        throw new MvccError(`Duplicate record: id = ${id}, rev = ${rev}`, 500);
    }
    if (vcRow && !force) {
        if (row.has("_deleted")) {
            throw new MvccError(
                "Deletion must be done via the deleteMvcc() API call");
        }
        return putNoForce(row, versions, vcRow);
    }
    return putForce(row, versions);
}

export function deleteMvcc(id: string, rev: string,
                           versions: IResultSet): MvccResult {
    const result = new MvccResult();
    const toDeleteVC = versions.find((rec) => rec.get("_rev") == rev);
    if (toDeleteVC) {
        if (toDeleteVC.get("isleaf")) {
            // Mark the vc record as deleted
            result.addVcTableAction("put", {
                seq: toDeleteVC.get("seq"),
                isleaf: false,
                isdeleted: true,
                isstub: false,
                isconflict: false,
                iswinner: false
            });
            if (toDeleteVC.get("isconflict")) {
                const remainingConflicts = versions.filter(
                    (rec) => rec.get("_rev") != rev && rec.get("inconflict"));
                if (remainingConflicts.length == 1) {
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
                        /* The remaining conflict is now the winner, swap the
                         * leaf record.
                         */
                        result.leafTable.type = "swap";
                        result.leafTable._id = id;
                        result.leafTable._rev = newWinner.get("_rev");
                    }
                } else if (remainingConflicts.length > 1) {
                    /* There were more than 1 conflicting records, determine
                     * the new winner with the target removed, which could be
                     * the same as the current winner.
                     * The old winner was either one of the remaining conflicts,
                     * or the targeted version.
                     */
                    const oldWinner = versions.find(
                        (rec) => rec.get("iswinner"));
                    if (oldWinner) {
                        const newWinner = winner(remainingConflicts);
                        if (!newWinner.get("iswinner")) {
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
                                result.addVcTableAction("put", {
                                    seq: oldWinner.get("seq"),
                                    isleaf: true,
                                    isdeleted: false,
                                    isstub: false,
                                    isconflict: true,
                                    iswinner: false
                                });
                            }
                        } // Else: do nothing, since the winner hasn't changed
                    } else {
                        // This should never happen
                        throw new MvccError(
                            `id ${id} has leaf version(s) but no record ` +
                            `marked as winner`);
                    }
                } else {
                    // This should never happen
                    throw new MvccError(
                        `id ${id}, rev ${rev} was marked isconflict, but ` +
                        `there were no other records in conflict`);
                }
            } else {
                // No conflicts, remove the leaf record
                result.leafTable.type = "delete";
                result.leafTable._id = id;
                result.leafTable._rev = rev;
            }
        } else {
            throw new MvccError(`${id}, rev ${rev} is not a leaf`);
        }
    } else {
        throw new MvccError(`Cannot find rev ${rev} to delete`, 404);
    }
    return result;
}

export function postMvcc(row: Row, context: IContext): MvccResult {
    const result = new MvccResult();
    convertToPayload(row);
    const id = Entity.generateId();
    const updated = new Date();
    const updatedBy = context.userAccountId;
    const rev = newVersion(row);
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

export function ancestryToAncestors(ancestry: string): string[] {
    const ancestors: string[] = ancestry.split(".");
    for (let depth = ancestors.length; depth > 0; depth--) {
        const ancestor = `${depth}-${ancestors[depth - 1]}`;
        ancestors[depth - 1] = ancestor;
    }
    return ancestors;
}

