export { Engine, EngineError, PreparedStatement, SerializationError, type EngineOptions, type OpenOptions, type Result, type Session } from "./engine/index.js";
export type { Value } from "./types.js";
export { OPFSSyncStorage, type OPFSStorageOptions } from "./store/backend/opfs-storage.js";
export { StorageError, PageOverflowError, DatabaseInUseError, UnsupportedFormatError, WalUnknownStateError } from "./store/errors.js";
