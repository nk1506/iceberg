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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class HiveMetastoreExtension implements AfterEachCallback, BeforeEachCallback {

  static HiveCatalog catalog;
  static HiveMetaStoreClient metastoreClient;
  static TestHiveMetastore metastore;
  static HiveConf hiveConf;
  private final Map<String, String> hiveConfOverride;
  static final String DB_NAME = "hivedb";

  public HiveMetastoreExtension(Map<String, String> hiveConfOverride) {
    this.hiveConfOverride = hiveConfOverride;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    metastore = new TestHiveMetastore();
    HiveConf hiveConfWithOverrides = new HiveConf(TestHiveMetastore.class);
    if (hiveConfOverride != null) {
      for (Map.Entry<String, String> kv : hiveConfOverride.entrySet()) {
        hiveConfWithOverrides.set(kv.getKey(), kv.getValue());
      }
    }

    metastore.start(hiveConfWithOverrides);
    hiveConf = metastore.hiveConf();
    metastoreClient = new HiveMetaStoreClient(hiveConfWithOverrides);

    String dbPath = metastore.getDatabasePath(DB_NAME);
    Database db = new Database(DB_NAME, "description", dbPath, Maps.newHashMap());
    metastoreClient.createDatabase(db);

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                hiveConfWithOverrides);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    catalog = null;
    metastoreClient.close();
    metastoreClient = null;
    metastore.stop();
    metastore = null;
  }
}
