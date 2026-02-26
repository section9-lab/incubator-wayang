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

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.UnarySourceDataQuantaBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spatial.data.WayangGeometry;

import java.util.Arrays;
import java.util.Collection;

public class SpatialJoin {

    public static void main(String[] args) {

        System.out.println(Arrays.toString(args));

        if (args.length <= 3) {
            System.err.print("Missing Paths: <input file1 URL> <input file2 URL> <platform>");
            System.exit(1);
        }

        WayangContext wayangContext = new WayangContext(new Configuration());

        String platform = args[2];
        switch (platform) {
            case "java":
                System.out.println("Activate only Java plugin");
                wayangContext.withPlugin(Java.basicPlugin());
                break;
            case "spark":
                System.out.println("Activate only Spark plugin");
                wayangContext.withPlugin(Spark.basicPlugin());
                break;
            default:
                System.out.println("Activate both Java and Spark plugin");
                wayangContext.withPlugin(Java.basicPlugin());
                wayangContext.withPlugin(Spark.basicPlugin());

        }

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Filter Test")
                .withUdfJarOf(SpatialJoin.class)
                .withUdfJarOf(JavaPlatform.class);


        String file1Url = args[1];
        String file2Url = args[2];
        DataQuantaBuilder<UnarySourceDataQuantaBuilder<?, String>, String> table1 = planBuilder.readTextFile(file1Url);
        DataQuantaBuilder<UnarySourceDataQuantaBuilder<?, String>, String> table2 = planBuilder.readTextFile(file2Url);


        // Query Berlin
        WayangGeometry queryGeometry = WayangGeometry.fromStringInput(
//                "POLYGON((-84.07287597656251 37.16644514778088, -81.79870605468751 37.16644514778088, -81.79870605468751 38.15788469869244, -84.07287597656251 38.15788469869244, -84.07287597656251 37.16644514778088))"
                "POLYGON((12.777099609375 52.219050335542484, 13.991088867187502 52.219050335542484, 13.991088867187502 52.71766191466581, 12.777099609375 52.71766191466581, 12.777099609375 52.219050335542484))"
//                "POLYGON((13.054504394531252 52.305791671751265, 13.23577880859375 52.33433208908722, 13.342895507812502 52.359499525558654, 13.521423339843752 52.37459311076614, 13.609313964843752 52.33433208908722, 13.669738769531252 52.320903597434054, 13.746643066406252 52.371239426380214, 13.787841796875002 52.40476481199653, 13.807067871093752 52.44830975509531, 13.807067871093752 52.48679443193377, 13.675231933593752 52.503516406073174, 13.686218261718752 52.54028236828442, 13.634033203125002 52.58035560366049, 13.537902832031252 52.612054291512536, 13.54339599609375 52.66372397759699, 13.47198486328125 52.69536233532457, 13.430786132812502 52.67871342471301, 13.359375000000002 52.645396558286066, 13.31817626953125 52.67371751370322, 13.24676513671875 52.67871342471301, 13.191833496093752 52.64872938781106, 13.16986083984375 52.612054291512536, 13.114929199218752 52.612054291512536, 13.09295654296875 52.57201003157308, 13.09295654296875 52.54529352469354, 13.08197021484375 52.50853175834131, 13.136901855468752 52.50853175834131, 13.065490722656252 52.473412273757006, 13.08197021484375 52.44663574493768, 13.046264648437502 52.40308914740344, 13.065490722656252 52.362854101276355, 13.054504394531252 52.305791671751265))"
        );

        Collection<Long> outputcount = table1
                .spatialJoin(
                        (line -> WayangGeometry.fromStringInput(line.split("\",")[0].replace("\"", ""))),
//                        line -> WGeometry.fromStringInput(line),
                        table2,
//                        line -> WGeometry.fromStringInput(line),
                        (line -> WayangGeometry.fromStringInput(line.split("\",")[0].replace("\"", ""))),
                        SpatialPredicate.INTERSECTS
                )
                .withTargetPlatform(Spark.platform())
                .count()
		.withTargetPlatform(Spark.platform())
                .collect();
//                .collect();
//                .map(pair -> pair.field0 + "\n" + pair.field1)//left.getWKT() + "\n" + right.getWKT();
//                .collect();
//                .writeTextFile("/incubator-wayang/wayang-applications/src/main/java/org/apache/wayang/applications/output.txt", input -> input, "join results");
        System.out.println("Spatial Join (intersects): " + outputcount);
    }
}
