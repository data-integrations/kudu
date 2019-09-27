/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KuduSourceConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final KuduSourceConfig VALID_CONFIG = new KuduSourceConfig(
    "kuduSource",
    "host1:8080,host2:8081",
    "kuduTable",
    "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"t\"," +
      "\"type\":[\"string\",\"null\"]},{\"name\":\"b\",\"type\":[\"long\",\"null\"]}]}",
    null,
    null
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateIncorrectReferenceName() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    KuduSourceConfig config = KuduSourceConfig.builder(VALID_CONFIG)
      .setReferenceName("!!!")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(KuduSourceConfig.REFERENCE_NAME)
    );

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateIncorrectSchema() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    KuduSourceConfig config = KuduSourceConfig.builder(VALID_CONFIG)
      .setOptSchema("\"name\":\"etlSchemaBody\",\"schema\":{\"type\":\"record\",\"name\":\"etlSchemaBody\"," +
                      "\"fields\":[{\"name\":\"t\",\"type\":[\"string\",\"null\"]},{\"name\":\"b\"," +
                      "\"type\":[\"long\",\"null\"]}]}")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(KuduSourceConfig.SCHEMA));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateIncorrectMasterAddresses() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    KuduSourceConfig config = KuduSourceConfig.builder(VALID_CONFIG)
      .setOptMasterAddresses("host1:8080,host2,8082,host3:8083")
      .build();
    List<List<String>> paramNames = Arrays.asList(
      Collections.singletonList(KuduSourceConfig.MASTER),
      Collections.singletonList(KuduSourceConfig.MASTER)
    );

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramNames);
  }
}
