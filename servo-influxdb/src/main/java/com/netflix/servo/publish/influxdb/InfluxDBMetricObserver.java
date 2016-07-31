/**
 * Copyright 2013 Netflix, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.servo.publish.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import com.google.common.collect.ImmutableMap;
import com.netflix.servo.Metric;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.tag.TagList;

/**
 * An Observer that forwards metrics to an InfluxDB instance.
 *
 * @author Abdelmonaim Remani, {@literal @}PolymathicCoder <PolymathicCoder@gmail.com>
 */
public class InfluxDBMetricObserver implements MetricObserver {

    private final InfluxDBConfiguration influxDBConfiguration;
    private final InfluxDB influxDB;

    private final TagList commonTags;

    /**
     * This constructor creates an observer that forwards data in batches to an
     * InfluxDB instance according to a configuration. If no database exists
     * with the name said in the configuration, it will be automatically
     * created.
     *
     * @param influxDBConfiguration
     *            The InfluxDB configuration
     * @param commonTags
     *            The list of tags to be appended to all metrics forwarded by
     *            this Observer.
     */
    public InfluxDBMetricObserver(final InfluxDBConfiguration influxDBConfiguration,
            final TagList commonTags) {
        this.influxDBConfiguration = influxDBConfiguration;

        // Configure client
        influxDB = InfluxDBFactory.connect(influxDBConfiguration.getUrl(),
                influxDBConfiguration.getUsername(), influxDBConfiguration.getPassword());
        // Enable batching
        influxDB.enableBatch(influxDBConfiguration.getBatchPolicy().getFlushEveryPoints(),
                influxDBConfiguration.getBatchPolicy().getFlushAtLeastEvery(),
                influxDBConfiguration.getBatchPolicy().getFlushAtLeastEveryTimeUnit());
        // Create databases if not already created
        influxDB.createDatabase(influxDBConfiguration.getDatabaseName());

        this.commonTags = commonTags;
    }

    @Override
    public void update(final List<Metric> metrics) {
        final BatchPoints batchPoints = BatchPoints
                .database(influxDBConfiguration.getDatabaseName()).retentionPolicy("default")
                .consistency(ConsistencyLevel.ALL).build();

        for (final Metric metric : metrics) {
            final Point point = Point.measurement(metric.getConfig().getName())
                    .time(metric.getTimestamp(), TimeUnit.MILLISECONDS)
                    .tag(ImmutableMap.<String, String>builder()
                            .putAll(metric.getConfig().getTags().asMap()).putAll(commonTags.asMap())
                            .build())
                    .addField(metric.getConfig().getName(), metric.getNumberValue()).build();

            batchPoints.point(point);
        }

        influxDB.write(batchPoints);
    }

    @Override
    public String getName() {
        return "influx";
    }
}
