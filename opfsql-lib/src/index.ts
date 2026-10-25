export { Engine, EngineError, PreparedStatement, type EngineOptions, type OpenOptions, type Result } from "./engine/index.js";
export type { Value } from "./types.js";
export { OPFSSyncStorage, type OPFSStorageOptions } from "./store/backend/opfs-storage.js";
export { StorageError, PageOverflowError, WalUnknownStateError } from "./store/errors.js";
