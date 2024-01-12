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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.IcebergMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** All the HMS operations like table,view,materialized_view should implement this. */
interface HiveOperationsBase {

  Logger LOG = LoggerFactory.getLogger(HiveOperationsBase.class);
  // The max size is based on HMS backend database. For Hive versions below 2.3, the max table
  // parameter size is 4000
  // characters, see https://issues.apache.org/jira/browse/HIVE-12274
  // set to 0 to not expose Iceberg metadata in HMS Table properties.
  String HIVE_TABLE_PROPERTY_MAX_SIZE = "iceberg.hive.table-property-max-size";
  long HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT = 32672;
  String NO_LOCK_EXPECTED_KEY = "expected_parameter_key";
  String NO_LOCK_EXPECTED_VALUE = "expected_parameter_value";

  TableType tableType();

  ClientPool<IMetaStoreClient, TException> metaClients();

  long maxHiveTablePropertySize();

  String database();

  String table();

  String metadataKeyValue(IcebergMetadata metadata, String key, String defaultValue);

  BaseMetastoreTableOperations.CommitStatus validateNewLocationAndReturnCommitStatus(
      IcebergMetadata metadata, String newMetadataLocation);

  Set<String> obsoleteProps(IcebergMetadata base, IcebergMetadata metadata);

  Table loadHmsTable() throws TException, InterruptedException;

  void setHmsParameters(
      IcebergMetadata metadata,
      Table tbl,
      String newMetadataLocation,
      Set<String> obsoleteProps,
      boolean hiveEngineEnabled);

  boolean hiveEngineEnabled(IcebergMetadata metadata);

  boolean hiveLockEnabled(IcebergMetadata metadata);

  default Map<String, String> hmsEnvContext(String metadataLocation) {
    return metadataLocation == null
        ? ImmutableMap.of()
        : ImmutableMap.of(
            NO_LOCK_EXPECTED_KEY,
            BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
            NO_LOCK_EXPECTED_VALUE,
            metadataLocation);
  }

  default boolean exposeInHmsProperties() {
    return maxHiveTablePropertySize() > 0;
  }

  default void setSchema(Schema tableSchema, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SCHEMA);
    if (exposeInHmsProperties() && tableSchema != null) {
      String schema = SchemaParser.toJson(tableSchema);
      setField(parameters, TableProperties.CURRENT_SCHEMA, schema);
    }
  }

  default void setField(Map<String, String> parameters, String key, String value) {
    if (value.length() <= maxHiveTablePropertySize()) {
      parameters.put(key, value);
    } else {
      LOG.warn(
          "Not exposing {} in HMS since it exceeds {} characters", key, maxHiveTablePropertySize());
    }
  }

  static void validateTableIsIceberg(Table table, String fullName) {
    String tableType = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    NoSuchIcebergTableException.check(
        tableType != null
            && tableType.equalsIgnoreCase(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE),
        "Not an iceberg table: %s (type=%s)",
        fullName,
        tableType);
  }

  default void persistTable(Table hmsTable, boolean updateHiveTable, String metadataLocation)
      throws TException, InterruptedException {
    if (updateHiveTable) {
      metaClients()
          .run(
              client -> {
                MetastoreUtil.alterTable(
                    client, database(), table(), hmsTable, hmsEnvContext(metadataLocation));
                return null;
              });
    } else {
      metaClients()
          .run(
              client -> {
                client.createTable(hmsTable);
                return null;
              });
    }
  }

  StorageDescriptor storageDescriptor(IcebergMetadata metadata, boolean hiveEngineEnabled);

  static StorageDescriptor storageDescriptor(
      Schema schema, String location, boolean hiveEngineEnabled) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(HiveSchemaUtil.convert(schema));
    storageDescriptor.setLocation(location);
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setParameters(Maps.newHashMap());

    if (hiveEngineEnabled) {
      storageDescriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
      storageDescriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
      serDeInfo.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
    } else {
      storageDescriptor.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
      storageDescriptor.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
      serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    }

    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  default void cleanupMetadataAndUnlock(
      HiveLock lock,
      FileIO io,
      BaseMetastoreTableOperations.CommitStatus commitStatus,
      String metadataLocation) {
    try {
      if (commitStatus.name().equalsIgnoreCase("FAILURE")) {
        // If we are sure the commit failed, clean up the uncommitted metadata file
        io.deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed to cleanup metadata file at {}", metadataLocation, e);
    } finally {
      lock.unlock();
    }
  }

  default HiveLock lockObject(IcebergMetadata metadata, Configuration conf, String catalogName) {
    if (hiveLockEnabled(metadata)) {
      return new MetastoreLock(conf, metaClients(), catalogName, database(), table());
    } else {
      return new NoLock();
    }
  }

  default Table newHmsTable(String hmsTableOwner) {
    Preconditions.checkNotNull(hmsTableOwner, "'hmsOwner' parameter can't be null");
    final long currentTimeMillis = System.currentTimeMillis();

    Table newTable =
        new Table(
            table(),
            database(),
            hmsTableOwner,
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            null,
            Collections.emptyList(),
            Maps.newHashMap(),
            null,
            null,
            tableType().name());

    if (tableType().equals(TableType.EXTERNAL_TABLE)) {
      newTable
          .getParameters()
          .put("EXTERNAL", "TRUE"); // using the external table type also requires this
    }

    return newTable;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  default void commitWithLocking(
      Configuration conf,
      String catalogName,
      IcebergMetadata base,
      IcebergMetadata metadata,
      String baseMetadataLocation,
      String newMetadataLocation,
      String fullName,
      FileIO io) {
    boolean newTable = base == null;
    boolean hiveEngineEnabled = hiveEngineEnabled(metadata);
    String opName = TableType.VIRTUAL_VIEW.equals(tableType()) ? "View" : "Table";
    BaseMetastoreTableOperations.CommitStatus commitStatus =
        BaseMetastoreTableOperations.CommitStatus.FAILURE;
    boolean updateHiveTable = false;
    HiveLock lock = lockObject(metadata, conf, catalogName);
    try {
      lock.lock();
      Table tbl = loadHmsTable();

      if (tbl != null) {
        String tableType = tbl.getTableType();
        if (!tableType.equalsIgnoreCase(tableType().name())) {
          throw new AlreadyExistsException(
              "%s with same name already exists: %s.%s",
              tableType.equalsIgnoreCase(TableType.VIRTUAL_VIEW.name()) ? "View" : "Table",
              tbl.getDbName(),
              tbl.getTableName());
        }

        // If we try to create the table but the metadata location is already set, then we had a
        // concurrent commit
        if (newTable
            && tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                != null) {
          throw new AlreadyExistsException("%s already exists: %s.%s", opName, database(), table());
        }

        updateHiveTable = true;
        LOG.debug("Committing existing {}: {}", opName.toLowerCase(), fullName);
      } else {
        tbl =
            newHmsTable(
                metadataKeyValue(
                    metadata, HiveCatalog.HMS_TABLE_OWNER, HiveHadoopUtil.currentUser()));
        LOG.debug("Committing new {}: {}", opName.toLowerCase(), fullName);
      }

      tbl.setSd(storageDescriptor(metadata, hiveEngineEnabled)); // set to pickup any schema changes

      String metadataLocation =
          tbl.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);

      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Cannot commit: Base metadata location '%s' is not same as the current %s metadata location '%s' for %s.%s",
            baseMetadataLocation, opName.toLowerCase(), metadataLocation, database(), table());
      }

      setHmsParameters(
          metadata, tbl, newMetadataLocation, obsoleteProps(base, metadata), hiveEngineEnabled);

      lock.ensureActive();

      try {
        persistTable(tbl, updateHiveTable, hiveLockEnabled(metadata) ? null : baseMetadataLocation);
        lock.ensureActive();

        commitStatus = BaseMetastoreTableOperations.CommitStatus.SUCCESS;
      } catch (LockException le) {
        commitStatus = BaseMetastoreTableOperations.CommitStatus.UNKNOWN;
        throw new CommitStateUnknownException(
            "Failed to heartbeat for hive lock while "
                + "committing changes. This can lead to a concurrent commit attempt be able to overwrite this commit. "
                + "Please check the commit history. If you are running into this issue, try reducing "
                + "iceberg.hive.lock-heartbeat-interval-ms.",
            le);
      } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
        throw new AlreadyExistsException(
            "%s already exists: %s.%s", opName, tbl.getDbName(), tbl.getTableName());
      } catch (InvalidObjectException e) {
        throw new ValidationException(e, "Invalid Hive object for %s.%s", database(), table());
      } catch (CommitFailedException | CommitStateUnknownException e) {
        throw e;
      } catch (Throwable e) {
        if (e.getMessage()
            .contains(
                "The table has been modified. The parameter value for key '"
                    + BaseMetastoreTableOperations.METADATA_LOCATION_PROP
                    + "' is")) {
          throw new CommitFailedException(
              e, "The table %s.%s has been modified concurrently", database(), table());
        }

        if (e.getMessage() != null
            && e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
          throw new RuntimeException(
              "Failed to acquire locks from metastore because the underlying metastore "
                  + "table 'HIVE_LOCKS' does not exist. This can occur when using an embedded metastore which does not "
                  + "support transactions. To fix this use an alternative metastore.",
              e);
        }

        LOG.error(
            "Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
            database(),
            table(),
            e);
        commitStatus = validateNewLocationAndReturnCommitStatus(metadata, newMetadataLocation);

        switch (commitStatus) {
          case SUCCESS:
            break;
          case FAILURE:
            throw e;
          case UNKNOWN:
            throw new CommitStateUnknownException(e);
        }
      }
    } catch (TException e) {
      throw new RuntimeException(
          String.format("Metastore operation failed for %s.%s", database(), table()), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } catch (LockException e) {
      throw new CommitFailedException(e);
    } finally {
      cleanupMetadataAndUnlock(lock, io, commitStatus, newMetadataLocation);
    }
  }
}
