/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.kudu;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import com.google.common.base.Strings;
import org.apache.kudu.client.shaded.com.google.common.base.Preconditions;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Configuration class for the plugin.
 */
public final class KuduSourceConfig extends PluginConfig {
  // Required Fields.

  @Name("referenceName")
  @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
  public String referenceName;

  @Name("master")
  @Description("Comma separated list of <hostname>:<port>[,<hostanme>:<port>]* of Apache Kudu Masters.")
  @Macro
  public String optMasterAddresses;

  @Name("name")
  @Description("Name of the Kudu table.")
  @Macro
  public String optTableName;

  @Name("schema")
  @Description("Output schema from Kudu source")
  public String optSchema;

  // Optional Fields

  @Name("columns")
  @Description("Columns to be projected from the table")
  @Nullable
  public String optColumnProjection;

  @Name("opt-timeout")
  @Description("Specifies the user operation timeout in milliseconds.")
  @Nullable
  public String optOperationTimeout;

  /**
   * Validates the configuration fields.
   *
   * @throws IllegalArgumentException throw when there is issue with the configuration.
   */
  public void validate() throws IllegalArgumentException {
    if (!containsMacro("master")) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(optMasterAddresses),
                                  "Kudu Master Server address list is empty.");
    }

    if (! containsMacro("table")) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(optTableName),
                                  "Kudu Table is not specified.");
    }
  }

  /**
   * @return {@link Schema} object of the JSON.
   */
  public Schema getSchema() {
    try {
      return Schema.parseJson(optSchema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse schema '%s'. Reason: %s", optSchema, e.getMessage())
      );
    }
  }
}
