package org.apache.calcite.jdbc;

import org.apache.calcite.schema.Schema;

/**
 * Created by jni on 3/3/15.
 */
public class CachingCalciteRootSchema extends CachingCalciteSchema
  implements CalciteRootSchema {
  /** Creates a root schema. */
  CachingCalciteRootSchema(Schema schema) {
    super(null, schema, "");
  }

}
