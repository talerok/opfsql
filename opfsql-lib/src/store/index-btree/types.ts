export type IndexKeyValue = string | number | boolean | null;
export type IndexKey = IndexKeyValue[];

export interface RangeOptions {
  lower?: IndexKey;
  upper?: IndexKey;
  lowerInclusive?: boolean;
  upperInclusive?: boolean;
}
