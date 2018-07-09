/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.services;

import static com.linkedin.kmf.common.Utils.getMBeanAttributeValues;

import com.linkedin.kmf.common.MbeanAttributeValue;
import com.linkedin.kmf.services.configs.OpenTSDBMetricsReporterServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OpenTSDBMetricsReporterService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(OpenTSDBMetricsReporterService.class);
  private final String _name;
  private final List<String> _metricNames;
  private final List<String> _openTSDBMetricNames;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _executor;

  public OpenTSDBMetricsReporterService(Map<String, Object> props, String name) {
    _name = name;
    OpenTSDBMetricsReporterServiceConfig config = new OpenTSDBMetricsReporterServiceConfig(props);
    _metricNames = config.getList(OpenTSDBMetricsReporterServiceConfig.REPORT_METRICS_CONFIG);
    _openTSDBMetricNames = config.getList(OpenTSDBMetricsReporterServiceConfig.OPEN_TSDB_METRICS_CONFIG);
    _reportIntervalSec = config.getInt(OpenTSDBMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public synchronized void start() {
    _executor.scheduleAtFixedRate(
      new Runnable() {
        @Override
        public void run() {
          try {
            reportMetrics();
          } catch (Exception e) {
            LOG.error(_name + "/OpenTSDBMetricsReporterService failed to report metrics", e);
          }
        }
      }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS
    );
    LOG.info("{}/OpenTSDBMetricsReporterService started", _name);
  }

  @Override
  public synchronized void stop() {
    _executor.shutdown();
    LOG.info("{}/OpenTSDBMetricsReporterService stopped", _name);
  }

  @Override
  public boolean isRunning() {
    return !_executor.isShutdown();
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted when waiting for {}/OpenTSDBMetricsReporterService to shutdown", _name);
    }
    LOG.info("{}/OpenTSDBMetricsReporterService shutdown completed", _name);
  }

  private void reportMetrics() {
    for (int i = 0; i < _metricNames.size(); i++) {
      String metricName = _metricNames.get(i);
      String openTSDBMetricName = _openTSDBMetricNames.get(i);
      String mbeanExpr = metricName.substring(0, metricName.lastIndexOf(":"));
      String attributeExpr = metricName.substring(metricName.lastIndexOf(":") + 1);
      List<MbeanAttributeValue> attributeValues = getMBeanAttributeValues(mbeanExpr, attributeExpr);
      for (MbeanAttributeValue attributeValue: attributeValues) {
        LOG.info("put {} {} {}", openTSDBMetricName, System.currentTimeMillis(), attributeValue.value());
      }
    }
  }
}
