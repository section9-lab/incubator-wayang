/*
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

package org.apache.wayang.apps.spatial;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spatial.data.WayangGeometry;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;


public class SpatialFilterPostgis {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();

        String tableName = "boxes_100k_1";
        String postgresUrl = args[2];

        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://cx23:5432/spatialdb"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "wayang_user");

        configuration.setProperty("wayang.postgres.jdbc.password", "wayang");


        WayangContext wayangContext = new WayangContext(configuration)
                .with(Java.basicPlugin())
                .with(Postgres.plugin());

        // Set up WayangContext.
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 10);

        SpatialGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))"
        );

        // Execute the job: keep only even numbers.
        final Collection<Long> outputcount = builder
                .readTable(new PostgresTableSource(tableName, "ST_AsText(geom)"))
                .spatialFilter(
                        (input -> WayangGeometry.fromStringInput(input.getString(0))),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                )
                .withTargetPlatform(Postgres.platform())
                .count()
                .collect();

        System.out.println("Spatial Postgres Filter (intersects): " + outputcount);
    }
}