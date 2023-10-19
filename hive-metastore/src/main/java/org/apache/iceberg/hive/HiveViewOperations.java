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

import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.hive.HiveCatalogUtil.HIVE_TABLE_PROPERTY_MAX_SIZE;
import static org.apache.iceberg.hive.HiveCatalogUtil.HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT;
import static org.apache.iceberg.hive.HiveCatalogUtil.isTableWithTypeExists;
import static org.apache.iceberg.hive.HiveCatalogUtil.validateTableIsIcebergView;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hive implementation of Iceberg ViewOperations. */
final class HiveViewOperations extends BaseViewOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HiveViewOperations.class);

  private final String fullName;
  private final String database;
  private final String tableName;
  private final Configuration conf;
  private final FileIO fileIO;
  private final ClientPool<IMetaStoreClient, TException> metaClients;
  private final long maxHiveTablePropertySize;
  private final TableIdentifier identifier;

  HiveViewOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      String catalogName,
      TableIdentifier tableIdentifier) {
    this.identifier = tableIdentifier;
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    this.conf = conf;
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.fullName = catalogName + "." + dbName + "." + tableName;
    this.database = dbName;
    this.tableName = tableName;
    this.maxHiveTablePropertySize =
        conf.getLong(HIVE_TABLE_PROPERTY_MAX_SIZE, HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
  }

  @Override
  public ViewMetadata current() {
    if (isTableWithTypeExists(metaClients, identifier, TableType.EXTERNAL_TABLE)) {
      throw new AlreadyExistsException(
          "Table with same name already exists: %s.%s", database, tableName);
    }
    return super.current();
  }

  @Override
  public void doRefresh() {
    String metadataLocation = null;
    try {
      Table table = metaClients.run(client -> client.getTable(database, tableName));
      validateTableIsIcebergView(table, fullName);
      metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException("View does not exist: %s.%s", database, tableName);
      }
    } catch (TException e) {
      String errMsg =
          String.format("Failed to get view info from metastore %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }
    refreshFromMetadataLocation(metadataLocation);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public void doCommit(ViewMetadata base, ViewMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    boolean keepHiveStats = conf.getBoolean(ConfigProperties.KEEP_HIVE_STATS, false);

    boolean updateHiveTable = false;

    try {

      Table tbl = loadHmsTable();

      if (tbl != null) {
        // If we try to create the view but the metadata location is already set, then we had a
        // concurrent commit
        if (newTable && tbl.getParameters().get(METADATA_LOCATION_PROP) != null) {
          throw new AlreadyExistsException("View already exists: %s.%s", database, tableName);
        }

        updateHiveTable = true;
        LOG.debug("Committing existing view: {}", fullName);
      } else {
        tbl = newHmsTable(metadata);
        LOG.debug("Committing new view: {}", fullName);
      }

      tbl.setSd(storageDescriptor(metadata)); // set to pickup any schema changes

      String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, tableName);
      }

      setHmsTableParameters(newMetadataLocation, tbl, metadata);

      if (!keepHiveStats) {
        tbl.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
      }

      try {
        persistTable(tbl, updateHiveTable, baseMetadataLocation);

      } catch (LockException le) {
        throw new CommitStateUnknownException(
            "Failed to heartbeat for hive lock while "
                + "committing changes. This can lead to a concurrent commit attempt be able to overwrite this commit. "
                + "Please check the commit history. If you are running into this issue, try reducing "
                + "iceberg.hive.lock-heartbeat-interval-ms.",
            le);
      } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
        throw new AlreadyExistsException(e, "View already exists: %s.%s", database, tableName);

      } catch (InvalidObjectException e) {
        throw new ValidationException(e, "Invalid Hive object for %s.%s", database, tableName);

      } catch (CommitFailedException | CommitStateUnknownException e) {
        throw e;

      } catch (Throwable e) {
        if (e.getMessage()
            .contains(
                "The table has been modified. The parameter value for key '"
                    + METADATA_LOCATION_PROP
                    + "' is")) {
          throw new CommitFailedException(
              e, "The table %s.%s has been modified concurrently", database, tableName);
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
            database,
            tableName,
            e);
      }
    } catch (TException e) {
      throw new RuntimeException(
          String.format("Metastore operation failed for %s.%s", database, tableName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } catch (LockException e) {
      throw new CommitFailedException(e);
    }

    LOG.info(
        "Committed to view {} with the new metadata location {}", fullName, newMetadataLocation);
  }

  void persistTable(Table hmsTable, boolean updateHiveTable, String expectedMetadataLocation)
      throws TException, InterruptedException {
    if (updateHiveTable) {
      metaClients.run(
          client -> {
            MetastoreUtil.alterTable(
                client,
                database,
                tableName,
                hmsTable,
                expectedMetadataLocation != null
                    ? ImmutableMap.of(METADATA_LOCATION_PROP, expectedMetadataLocation)
                    : ImmutableMap.of());
            return null;
          });
    } else {
      metaClients.run(
          client -> {
            client.createTable(hmsTable);
            return null;
          });
    }
  }

  Table loadHmsTable() throws TException, InterruptedException {
    try {
      return metaClients.run(client -> client.getTable(database, tableName));
    } catch (NoSuchObjectException nte) {
      LOG.trace("Table not found {}", fullName, nte);
      return null;
    }
  }

  private StorageDescriptor storageDescriptor(ViewMetadata metadata) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(HiveSchemaUtil.convert(metadata.schema()));
    storageDescriptor.setLocation(metadata.location());
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setParameters(Maps.newHashMap());
    storageDescriptor.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
    storageDescriptor.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private Table newHmsTable(ViewMetadata metadata) {
    Preconditions.checkNotNull(metadata, "'metadata' parameter can't be null");
    final long currentTimeMillis = System.currentTimeMillis();

    Table newTable =
        new Table(
            tableName,
            database,
            HiveHadoopUtil.currentUser(),
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            null,
            Collections.emptyList(),
            Maps.newHashMap(),
            null,
            null,
            TableType.VIRTUAL_VIEW.toString());

    return newTable;
  }

  private void setHmsTableParameters(String newMetadataLocation, Table tbl, ViewMetadata metadata) {
    Map<String, String> parameters =
        Optional.ofNullable(tbl.getParameters()).orElseGet(Maps::newHashMap);

    if (metadata.uuid() != null) {
      parameters.put(TableProperties.UUID, metadata.uuid());
    }

    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    setSchema(metadata, parameters);
    tbl.setParameters(parameters);
  }

  void setSchema(ViewMetadata metadata, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SCHEMA);
    if (metadata.schema() != null) {
      String schema = SchemaParser.toJson(metadata.schema());
      setField(parameters, TableProperties.CURRENT_SCHEMA, schema);
    }
  }

  private void setField(Map<String, String> parameters, String key, String value) {
    if (value.length() <= maxHiveTablePropertySize) {
      parameters.put(key, value);
    } else {
      LOG.warn(
          "Not exposing {} in HMS since it exceeds {} characters", key, maxHiveTablePropertySize);
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String viewName() {
    return fullName;
  }
}
