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

import java.util.Collections;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.view.ViewCatalogTests;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHiveViewCatalog extends ViewCatalogTests<HiveCatalog> {

  private HiveMetastoreSetup hiveMetastoreSetup;

  @BeforeEach
  public void before() throws Exception {
    hiveMetastoreSetup = new HiveMetastoreSetup(Collections.emptyMap());
  }

  @AfterEach
  public void after() throws Exception {
    hiveMetastoreSetup.stopMetastore();
  }

  @Override
  protected HiveCatalog catalog() {
    return hiveMetastoreSetup.catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return hiveMetastoreSetup.catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  // Override few tests which are using AlreadyExistsException instead of NoSuchViewException

  @Override
  @Test
  public void replaceTableViaTransactionThatAlreadyExistsAsView() {
    Assumptions.assumeThat(catalog()).as("Only valid for catalogs that support tables").isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildTable(viewIdentifier, SCHEMA)
                    .replaceTransaction()
                    .commitTransaction())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View with same name already exists: ns.view");

    assertThat(catalog().dropView(viewIdentifier)).isTrue();
    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();
  }

  @Override
  @Test
  public void replaceViewThatAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(tableIdentifier)
                    .withSchema(OTHER_SCHEMA)
                    .withDefaultNamespace(tableIdentifier.namespace())
                    .withQuery("spark", "select * from ns.tbl")
                    .replace())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table with same name already exists: ns.table");
  }

  @Override
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

    assertThatThrownBy(() -> tableCatalog().renameTable(tableIdentifier, viewIdentifier))
        .hasMessageContaining("new table ns.view already exists");
  }

  @Override
  @Test
  public void createTableViaTransactionThatAlreadyExistsAsView() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier viewIdentifier = TableIdentifier.of("ns", "view");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(viewIdentifier.namespace());
    }

    assertThat(catalog().viewExists(viewIdentifier)).as("View should not exist").isFalse();

    Transaction transaction = tableCatalog().buildTable(viewIdentifier, SCHEMA).createTransaction();

    catalog()
        .buildView(viewIdentifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(viewIdentifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    assertThat(catalog().viewExists(viewIdentifier)).as("View should exist").isTrue();

    assertThatThrownBy(transaction::commitTransaction)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table already exists: ns.view");
  }
}
