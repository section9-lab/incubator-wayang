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
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.java.Java;
import org.apache.wayang.spatial.Spatial;

import java.util.Collection;

public class ComplexSpatialFilterSpark {

    public static void main(String[] args) {


        //// Debugging might be useful, set level to "FINEST" to see actual db query strings
        System.out.println( ">>> Test a Filter Operator");

        //// Db Connection, local db credentials!
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/spatialdb"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spatial.javaPlugin())
                ;
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Filter Test")
                .withUdfJarOf(ComplexSpatialFilterSpark.class);

        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((12.777099609375 52.219050335542484, 13.991088867187502 52.219050335542484, 13.991088867187502 52.71766191466581, 12.777099609375 52.71766191466581, 12.777099609375 52.219050335542484))"
        );

        final Collection<Long> outputValues = planBuilder
                .readTextFile("file:///sc/home/maximilian.speer/wayang/cemetery.csv")

                .spatialFilter(
                        (input -> {
                            WayangGeometry geom =  WayangGeometry.fromStringInput((input.split("\",")[0]).replace("\"", ""));
                            return geom;
                        }),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                ).withTargetPlatform(Java.platform())
                .withName("Spatial Filter (intersects)")
                .count()
                .collect();

        System.out.println("Spatial Filter (intersects): " + outputValues);

        return;

    }
}