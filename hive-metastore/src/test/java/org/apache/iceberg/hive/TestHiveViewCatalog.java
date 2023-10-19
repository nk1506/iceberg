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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.thrift.TException;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestHiveViewCatalog extends ViewCatalogTests<HiveCatalog> {

  private HiveCatalog catalog;

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().build();

  @BeforeEach
  public void before() throws TException {
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                HIVE_METASTORE_EXTENSION.hiveConf());
  }

  @AfterEach
  public void cleanup() throws Exception {
    HIVE_METASTORE_EXTENSION.metastore().reset();
  }

  @Override
  protected HiveCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  // TODO: This test should be removed after fix of https://github.com/apache/iceberg/issues/9289.
  @Test
  public void renameTableTargetAlreadyExistsAsView() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    // With fix of issues#9289,it should match with ViewCatalogTests and expect
    // AlreadyExistsException.class
    // and message should contain as "Cannot rename ns.table to ns.view. View already exists"
    assertThatThrownBy(() -> tableCatalog().renameTable(tableIdentifier, viewIdentifier))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("new table ns.view already exists");
  }
}
