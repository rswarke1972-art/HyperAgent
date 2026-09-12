/**
 * StateManager: Canonical State + Isolated Speculative Branches
 */
export class StateManager {
  constructor(initialState = {}) {
    this.canonicalState = JSON.parse(JSON.stringify(initialState));
    this.speculativeBranches = new Map();
  }

  createSpeculativeBranch(branchId, predictedState) {
    const branch = JSON.parse(JSON.stringify(this.canonicalState));
    Object.assign(branch, predictedState);
    this.speculativeBranches.set(branchId, branch);
    return branch;
  }

  commitSpeculativeBranch(branchId, resultKey, resultVal) {
    this.canonicalState[resultKey] = resultVal;
    this.speculativeBranches.delete(branchId);
  }

  discardSpeculativeBranch(branchId) {
    this.speculativeBranches.delete(branchId);
  }

  getCanonical() {
    return JSON.parse(JSON.stringify(this.canonicalState));
  }
}
