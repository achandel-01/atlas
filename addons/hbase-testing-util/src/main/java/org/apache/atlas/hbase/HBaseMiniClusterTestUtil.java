/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.hbase;

import org.apache.hadoop.conf.Configuration;

/**
 * Shared {@link org.apache.hadoop.hbase.HBaseTestingUtility} settings for Atlas integration tests.
 */
public final class HBaseMiniClusterTestUtil {
    private static final String LOCALHOST = "127.0.0.1";

    private HBaseMiniClusterTestUtil() {
    }

    /**
     * Applies configuration required for a stable local mini-cluster on HBase 2.6+ with Hadoop 3.4+.
     * Do not pre-assign master/RS/ZK ports; HBaseTestingUtility assigns random local ports by default.
     */
    public static void applyMiniClusterDefaults(Configuration configuration) {
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        configuration.setBoolean("hbase.table.sanity.checks", false);
        // HBase 2.6.x + Hadoop 3.4: default asyncfs WAL breaks mini-cluster (HBASE-28908 / HBASE-28721)
        configuration.set("hbase.wal.provider", "filesystem");
        // Use loopback so master/RS RPC works on macOS and similar environments (avoids LAN IP connect timeouts)
        configuration.set("hbase.master.hostname", LOCALHOST);
        configuration.set("hbase.regionserver.hostname", LOCALHOST);
    }
}
