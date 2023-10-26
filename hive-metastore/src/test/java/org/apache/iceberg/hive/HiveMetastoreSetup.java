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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Setup HiveMetastore. It does not create any database. All the tests should create a database
 * accordingly. It should replace the existing setUp class {@link HiveMetastoreTest}
 */
class HiveMetastoreSetup {

  protected HiveMetaStoreClient metastoreClient;
  protected TestHiveMetastore metastore;
  protected HiveConf hiveConf;
  HiveCatalog catalog;

  HiveMetastoreSetup(Map<String, String> hiveConfOverride) throws Exception {
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

  void stopMetastore() throws Exception {
    try {
      metastoreClient.close();
      metastore.stop();
    } finally {
      catalog = null;
      metastoreClient = null;
      metastore = null;
    }
  }
}
