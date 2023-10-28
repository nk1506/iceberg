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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.view.ViewCatalogTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestHiveViewCatalog extends ViewCatalogTests<HiveCatalog> {

  private HiveCatalog catalog;

  @BeforeEach
  public void before() throws Exception {
    HiveMetastoreTest.startMetastore(Collections.emptyMap());
    this.catalog = HiveMetastoreTest.catalog;
  }

  @AfterEach
  public void after() throws Exception {
    HiveMetastoreTest.stopMetastore();
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
}
