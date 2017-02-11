/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.kudu;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.hydrator.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Plugin {@link Config} for Apache Kudu
 */
public class KuduConfig extends ReferencePluginConfig {
  @Description("Name of the Kudu table")
  @Macro
  public String tableName;

  @Description("Comma-separated list of hostname:port for Kudu masters")
  @Macro
  public String masterAddresses;

  @Description("Schema of the Record to be emitted (in case of Source) or received (in case of Sink)")
  public String schema;

  @Description("Timeout for Kudu operations in milliseconds. Defaults to 30000 ms.")
  @Nullable
  public long operationTimeoutMs;

  public KuduConfig() {
    this("kudu");

  }

  public KuduConfig(String referenceName) {
    super(referenceName);
    this.operationTimeoutMs = 30000;
  }
}
