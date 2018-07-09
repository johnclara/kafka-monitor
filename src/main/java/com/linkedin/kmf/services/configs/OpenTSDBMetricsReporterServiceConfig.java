/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.services.configs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Arrays;
import java.util.Map;

public class OpenTSDBMetricsReporterServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String REPORT_METRICS_CONFIG = CommonServiceConfig.REPORT_METRICS_CONFIG;
  public static final String REPORT_METRICS_DOC = CommonServiceConfig.REPORT_METRICS_DOC;

  public static final String OPEN_TSDB_METRICS_CONFIG = "report.metrics.opentsdb.list";
  public static final String OPEN_TSDB_METRICS_DOC = "The openTSDB name of the metric. "
       + "This must be the same length of REPORT_METRICS_CONFIG and corresponds by index.";

  public static final String REPORT_INTERVAL_SEC_CONFIG = CommonServiceConfig.REPORT_INTERVAL_SEC_CONFIG;
  public static final String REPORT_INTERVAL_SEC_DOC = CommonServiceConfig.REPORT_INTERVAL_SEC_DOC;

  static {
    CONFIG = new ConfigDef()
      .define(REPORT_METRICS_CONFIG,
              ConfigDef.Type.LIST,
              Arrays.asList(),
              ConfigDef.Importance.MEDIUM,
              REPORT_METRICS_DOC)
      .define(OPEN_TSDB_METRICS_CONFIG,
              ConfigDef.Type.LIST,
              Arrays.asList(),
              ConfigDef.Importance.MEDIUM,
              OPEN_TSDB_METRICS_DOC)
      .define(REPORT_INTERVAL_SEC_CONFIG,
              ConfigDef.Type.INT,
              1,
              ConfigDef.Importance.LOW,
              REPORT_INTERVAL_SEC_DOC);

  }

  public OpenTSDBMetricsReporterServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}