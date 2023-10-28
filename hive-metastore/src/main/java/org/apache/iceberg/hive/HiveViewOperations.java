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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hive.HiveCatalogUtil.CommitStatus;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
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
  private final String viewName;
  private final FileIO fileIO;
  private final ClientPool<IMetaStoreClient, TException> metaClients;
  private final long maxHiveTablePropertySize;
  private final TableIdentifier identifier;

  HiveViewOperations(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      FileIO fileIO,
      String catalogName,
      TableIdentifier viewIdentifier) {
    this.identifier = viewIdentifier;
    String dbName = viewIdentifier.namespace().level(0);
    this.metaClients = metaClients;
    this.fileIO = fileIO;
    this.fullName = catalogName + "." + dbName + "." + viewIdentifier.name();
    this.database = dbName;
    this.viewName = viewIdentifier.name();
    this.maxHiveTablePropertySize =
        conf.getLong(
            HiveCatalogUtil.HIVE_TABLE_PROPERTY_MAX_SIZE,
            HiveCatalogUtil.HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
  }

  @Override
  public ViewMetadata current() {
    if (HiveCatalogUtil.isTableWithTypeExists(metaClients, identifier, TableType.EXTERNAL_TABLE)) {
      throw new AlreadyExistsException(
          "Table with same name already exists: %s.%s", database, viewName);
    }
    return super.current();
  }

  @Override
  public void doRefresh() {
    String metadataLocation = null;
    try {
      Table table = metaClients.run(client -> client.getTable(database, viewName));
      HiveCatalogUtil.validateTableIsIcebergView(table, fullName);
      metadataLocation =
          table.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException("View does not exist: %s.%s", database, viewName);
      }
    } catch (TException e) {
      String errMsg =
          String.format("Failed to get view info from metastore %s.%s", database, viewName);
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
    boolean newView = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    boolean updateHiveView = false;
    CommitStatus commitStatus = CommitStatus.FAILURE;

    try {

      Optional<Table> hmsTable = Optional.ofNullable(loadHmsTable());
      Table view;

      if (hmsTable.isPresent()) {
        view = hmsTable.get();
        // If we try to create the view but the metadata location is already set, then we had a
        // concurrent commit
        if (newView
            && view.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
                != null) {
          HiveCatalogUtil.matchAndThrowExistenceTypeException(view);
        }

        updateHiveView = true;
        LOG.debug("Committing existing view: {}", fullName);
      } else {
        view = newHmsView(metadata);
        LOG.debug("Committing new view: {}", fullName);
      }

      view.setSd(storageDescriptor(metadata)); // set to pickup any schema changes

      String metadataLocation =
          view.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Base metadata location '%s' is not same as the current view metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, viewName);
      }

      setHmsTableParameters(newMetadataLocation, view, metadata);

      try {
        persistView(view, updateHiveView, baseMetadataLocation);
        commitStatus = CommitStatus.SUCCESS;

      } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
        HiveCatalogUtil.matchAndThrowExistenceTypeException(view);

      } catch (InvalidObjectException e) {
        throw new ValidationException(e, "Invalid Hive object for %s.%s", database, viewName);

      } catch (CommitFailedException | CommitStateUnknownException e) {
        throw e;

      } catch (Throwable e) {
        if (e.getMessage()
            .contains(
                "The view has been modified. The parameter value for key '"
                    + BaseMetastoreTableOperations.METADATA_LOCATION_PROP
                    + "' is")) {
          throw new CommitFailedException(
              e, "The view %s.%s has been modified concurrently", database, viewName);
        }

        LOG.error(
            "Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
            database,
            viewName,
            e);
        commitStatus = checkCommitStatus(newMetadataLocation);
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
          String.format("Metastore operation failed for %s.%s", database, viewName), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } catch (LockException e) {
      throw new CommitFailedException(e);
    } finally {
      cleanupMetadata(commitStatus, newMetadataLocation);
    }

    LOG.info(
        "Committed to view {} with the new metadata location {}", fullName, newMetadataLocation);
  }

  private void cleanupMetadata(CommitStatus commitStatus, String metadataLocation) {
    try {
      if (commitStatus == CommitStatus.FAILURE) {
        // If we are sure the commit failed, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed to cleanup metadata file at {}", metadataLocation, e);
    }
  }

  private CommitStatus checkCommitStatus(String newMetadataLocation) {
    AtomicReference<CommitStatus> status = new AtomicReference<>(CommitStatus.UNKNOWN);

    Tasks.foreach(newMetadataLocation)
        .retry(5)
        .suppressFailureWhenFinished()
        .exponentialBackoff(100, 1000, 10000, 2.0)
        .onFailure(
            (location, checkException) ->
                LOG.error("Cannot check if commit to {} exists.", viewName(), checkException))
        .run(
            location -> {
              ViewMetadata metadata = refresh();
              String currentMetadataFileLocation = metadata.metadataFileLocation();
              boolean commitSuccess = newMetadataLocation.equals(currentMetadataFileLocation);
              if (commitSuccess) {
                LOG.info(
                    "Commit status check: Commit to {} of {} succeeded",
                    viewName(),
                    newMetadataLocation);
                status.set(CommitStatus.SUCCESS);
              } else {
                LOG.warn(
                    "Commit status check: Commit to {} of {} unknown, new metadata location is not current "
                        + "or in history",
                    viewName(),
                    newMetadataLocation);
              }
            });

    if (status.get() == CommitStatus.UNKNOWN) {
      LOG.error(
          "Cannot determine commit state to {}. Failed during checking {} times. "
              + "Treating commit state as unknown.",
          viewName(),
          5);
    }
    return status.get();
  }

  void persistView(Table hmsTable, boolean updateHiveTable, String expectedMetadataLocation)
      throws TException, InterruptedException {
    if (updateHiveTable) {
      metaClients.run(
          client -> {
            MetastoreUtil.alterTable(
                client,
                database,
                viewName,
                hmsTable,
                expectedMetadataLocation != null
                    ? ImmutableMap.of(
                        BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                        expectedMetadataLocation)
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
      return metaClients.run(client -> client.getTable(database, viewName));
    } catch (NoSuchObjectException nte) {
      LOG.trace("View not found {}", fullName, nte);
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

  private Table newHmsView(ViewMetadata metadata) {
    Preconditions.checkNotNull(metadata, "'metadata' parameter can't be null");
    final long currentTimeMillis = System.currentTimeMillis();

    Table newView =
        new Table(
            viewName,
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

    return newView;
  }

  private void setHmsTableParameters(String newMetadataLocation, Table tbl, ViewMetadata metadata) {
    Map<String, String> parameters =
        Optional.ofNullable(tbl.getParameters()).orElseGet(Maps::newHashMap);

    if (metadata.uuid() != null) {
      parameters.put(TableProperties.UUID, metadata.uuid());
    }

    parameters.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(
          BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
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
