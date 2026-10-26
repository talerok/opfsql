// The binder (docs/planning.md): syntax trees and a schema to bound
// statements — names resolved through scopes, types through the function
// registry, DDL to the schema model — and the schema itself from the
// schema table's rows.

export { bindIndexKey } from "./create-index.js";
export { loadSchema, type SchemaRow } from "./load-schema.js";
export { bindStatement, type BindableStatement } from "./statement.js";
