/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.hive;

import java.util.List;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchIcebergViewException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A utility class to validate Hive Iceberg Table and Views. */
final class HiveCatalogUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogUtil.class);

  // the max size is based on HMS backend database. For Hive versions below 2.3, the max table
  // parameter size is 4000
  // characters, see https://issues.apache.org/jira/browse/HIVE-12274
  // set to 0 to not expose Iceberg metadata in HMS Table properties.
  static final String HIVE_TABLE_PROPERTY_MAX_SIZE = "iceberg.hive.table-property-max-size";
  static final long HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT = 32672;

  private HiveCatalogUtil() {
    // empty constructor for utility class
  }

  static boolean isTableWithTypeExists(
      ClientPool<IMetaStoreClient, TException> clients,
      TableIdentifier identifier,
      TableType tableType) {
    String database = identifier.namespace().level(0);
    String tableName = identifier.name();
    try {
      List<String> tables = clients.run(client -> client.getTables(database, tableName, tableType));
      return !tables.isEmpty();
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to check table existence " + database + "." + tableName, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to listTables", e);
    }
  }

  static void validateTableIsIcebergView(Table table, String fullName) {
    String tableType = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    NoSuchIcebergViewException.check(
        table.getTableType().equalsIgnoreCase(TableType.VIRTUAL_VIEW.name())
            && tableType != null
            && tableType.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE),
        "Not an iceberg view: %s (type=%s) (tableType=%s)",
        fullName,
        tableType,
        table.getTableType());
  }

  static void validateTableIsIceberg(Table table, String fullName) {
    if (table.getTableType().equalsIgnoreCase(TableType.VIRTUAL_VIEW.name())) {
      throw new AlreadyExistsException(
          "View with same name already exists: %s.%s", table.getDbName(), table.getTableName());
    }
    String tableType = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    NoSuchIcebergTableException.check(
        tableType != null
            && tableType.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE),
        "Not an iceberg table: %s (type=%s) (tableType=%s)",
        fullName,
        tableType,
        table.getTableType());
  }
}
