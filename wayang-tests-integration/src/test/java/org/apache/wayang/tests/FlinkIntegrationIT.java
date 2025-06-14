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

package org.apache.wayang.tests;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.java.Java;
import org.apache.wayang.tests.platform.MyMadeUpPlatform;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test the Spark integration with Wayang.
 */
class FlinkIntegrationIT {

    private static final String JAVA  = "JAVA";
    private static final String FLINK = "JAVA";
    private static final String BOTH  = "BOTH";

    private WayangContext makeContext(String plugin){
        WayangContext wayangContext = new WayangContext();
        if (plugin == JAVA || plugin == BOTH)
            wayangContext.with(Java.basicPlugin());
        if (plugin == FLINK || plugin == BOTH)
            wayangContext.with(Flink.basicPlugin());
        return wayangContext;
    }

    private void makeAndRun(WayangPlan plan, String plugin){
        WayangContext wayangContext = this.makeContext(plugin);
        wayangContext.execute(plan);
    }

    @Test
    void testReadAndWrite() throws Exception {
        // Build a Wayang plan.
        List<String> collector = new LinkedList<>();

        makeAndRun(WayangPlans.readWrite(WayangPlans.FILE_SOME_LINES_TXT, collector), FLINK);

        // Verify the plan result.
        final List<String> lines = Files.lines(Paths.get(WayangPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        assertEquals(lines, collector);
    }


    @Test
    void testReadAndTransformAndWrite() {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        makeAndRun(wayangPlan, FLINK);
    }

    @Test
    void testCartesianOperator() {

        List<Tuple2<String, String>> collector = new ArrayList<>();
        final WayangPlan wayangPlan = WayangPlansOperators.cartesian(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT, collector);
        makeAndRun(wayangPlan, FLINK);

        // Run in java for test result
        List<Tuple2<String, String>> collectorJava = new ArrayList<>();
        final WayangPlan wayangPlanJava = WayangPlansOperators.cartesian(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testCoGroupOperator() {
        List<Tuple2<?, ?>> collector = new ArrayList<>();
        final WayangPlan wayangPlan = WayangPlansOperators.coGroup(WayangPlans.FILE_WITH_KEY_1, WayangPlans.FILE_WITH_KEY_2, collector);
        makeAndRun(wayangPlan, FLINK);

        // Run in java for test result
        List<Tuple2<?, ?>> collectorJava = new ArrayList<>();
        final WayangPlan wayangPlanJava = WayangPlansOperators.coGroup(WayangPlans.FILE_WITH_KEY_1, WayangPlans.FILE_WITH_KEY_2, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testCollectionSource() {
        List<String> input = makeList();
        List<String> collector = new ArrayList<>();

        WayangPlan wayangplan = WayangPlansOperators.collectionSourceOperator(input, collector);
        makeAndRun(wayangplan, FLINK);

        assertEquals(input, collector);
    }

    @Test
    void testCountOperator() {
        List<String> input = makeList();
        List<Long> collector = new ArrayList<>();

        WayangPlan wayangPlan = WayangPlansOperators.count(input, collector);
        makeAndRun(wayangPlan, FLINK);

        assertEquals(input.size(), (long) collector.get(0));
    }

    @Test
    void testDistinctOperator() {
        List<String> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.distinct(WayangPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(wayangPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.distinct(WayangPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertArrayEquals(collectorJava.stream().sorted().toArray(), collector.stream().sorted().toArray());
    }

    @Test
    void testFilterOperator() {
        List<String> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.filter(WayangPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(wayangPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.filter(WayangPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testFlapMapOperator() {
        List<String> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.flatMap(WayangPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(wayangPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.flatMap(WayangPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testJoinOperator() {
        List<Tuple2<?, ?>> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.join(WayangPlans.FILE_WITH_KEY_1, WayangPlans.FILE_WITH_KEY_2, collector);
        makeAndRun(wayangPlan, FLINK);

        List<Tuple2<?, ?>> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.join(WayangPlans.FILE_WITH_KEY_1, WayangPlans.FILE_WITH_KEY_2, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testReduceByOperator() {
        List<Tuple2<?, ?>> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.reduceBy(WayangPlans.FILE_WITH_KEY_1, collector);
        makeAndRun(wayangPlan, FLINK);

        List<Tuple2<?, ?>> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.reduceBy(WayangPlans.FILE_WITH_KEY_1, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testSortOperator() {
        List<String> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.sort(WayangPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(wayangPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.sort(WayangPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testTextFileSink() throws Exception {
        File temp = File.createTempFile("tempfile", ".tmp");

        temp.delete();

        WayangPlan wayangPlan = WayangPlansOperators.textFileSink(WayangPlans.FILE_SOME_LINES_TXT, temp.toURI());
        makeAndRun(wayangPlan, FLINK);

        final List<String> lines = Files.lines(Paths.get(WayangPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        final List<String> linesFlink = Files.lines(Paths.get(temp.toURI())).collect(Collectors.toList());


        assertEquals(lines, linesFlink);

        temp.delete();
    }

    @Test
    void testUnionOperator() {
        List<String> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.union(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT, collector);
        makeAndRun(wayangPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.union(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    @Test
    void testZipWithIdOperator() {
        List<Tuple2<Long, String>> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlansOperators.zipWithId(WayangPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(wayangPlan, FLINK);

        List<Tuple2<Long, String>> collectorJava = new ArrayList<>();
        WayangPlan wayangPlanJava = WayangPlansOperators.zipWithId(WayangPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(wayangPlanJava, JAVA);

        assertEquals(collectorJava, collector);
    }

    private List<String> makeList() {
        return Arrays.asList(
                "word1",
                "word2",
                "word3",
                "word4",
                "word5",
                "word6",
                "word7",
                "word8",
                "word9",
                "word10"
        );
    }

    @Test
    void testReadAndTransformAndWriteWithIllegalConfiguration1() {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);
        // ILLEGAL: This platform is not registered, so this operator will find no implementation.
        wayangPlan.getSinks().forEach(sink -> sink.addTargetPlatform(MyMadeUpPlatform.getInstance()));

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = makeContext(FLINK);

        assertThrows(WayangException.class, () ->
            // Have Wayang execute the plan.
            wayangContext.execute(wayangPlan));
    }

    @Test
    void testReadAndTransformAndWriteWithIllegalConfiguration2() {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);

        WayangContext wayangContext = new WayangContext();
        // ILLEGAL: This dummy platform is not sufficient to execute the plan.
        wayangContext.register(MyMadeUpPlatform.getInstance());

        assertThrows(WayangException.class, () ->
            // Have Wayang execute the plan.
            wayangContext.execute(wayangPlan));
    }

    @Test
    void testReadAndTransformAndWriteWithIllegalConfiguration3() {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = makeContext(FLINK);

        // Have Wayang execute the plan.
        final Job job = wayangContext.createJob(null, wayangPlan);
        // ILLEGAL: We blacklist the Spark platform, although we need it.
        job.getConfiguration().getPlatformProvider().addToBlacklist(Flink.platform());
        job.getConfiguration().getPlatformProvider().addToWhitelist(MyMadeUpPlatform.getInstance());
        assertThrows(WayangException.class, job::execute);
    }

    @Test
    void testMultiSourceAndMultiSink() {
        // Define some input data.
        final List<String> collection1 = Arrays.asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.asList("This is source 2.", "This is source 2, too.");
        List<String> collector1 = new LinkedList<>();
        List<String> collector2 = new LinkedList<>();
        final WayangPlan wayangPlan = WayangPlans.multiSourceMultiSink(collection1, collection2, collector1, collector2);

        makeAndRun(wayangPlan, FLINK);

        // Check the results in both sinks.
        List<String> expectedOutcome1 = Stream.concat(collection1.stream(), collection2.stream())
                .map(String::toUpperCase)
                .collect(Collectors.toList());
        List<String> expectedOutcome2 = Stream.concat(collection1.stream(), collection2.stream())
                .collect(Collectors.toList());
        Collections.sort(expectedOutcome1);
        Collections.sort(expectedOutcome2);
        Collections.sort(collector1);
        Collections.sort(collector2);
        assertEquals(expectedOutcome1, collector1);
        assertEquals(expectedOutcome2, collector2);
    }

    @Test
    void testMultiSourceAndHoleAndMultiSink() {
        // Define some input data.
        final List<String> collection1 = Arrays.asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.asList("This is source 2.", "This is source 2, too.");
        List<String> collector1 = new LinkedList<>();
        List<String> collector2 = new LinkedList<>();
        final WayangPlan wayangPlan = WayangPlans.multiSourceHoleMultiSink(collection1, collection2, collector1, collector2);


        makeAndRun(wayangPlan, FLINK);

        // Check the results in both sinks.
        List<String> expectedOutcome = Stream.concat(collection1.stream(), collection2.stream())
                .flatMap(string -> Stream.of(string.toLowerCase(), string.toUpperCase())).sorted().collect(Collectors.toList());
        Collections.sort(collector1);
        Collections.sort(collector2);
        assertEquals(expectedOutcome, collector1);
        assertEquals(expectedOutcome, collector2);
    }

    @Test
    void testGlobalMaterializedGroup() {
        // Build the WayangPlan.
        List<Iterable<Integer>> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.globalMaterializedGroup(collector, 1, 2, 3);

        // Instantiate Wayang and activate the Java backend.
        makeAndRun(wayangPlan, FLINK);

        assertEquals(1, collector.size());
        assertEquals(WayangCollections.asSet(1, 2, 3), WayangCollections.asCollection(collector.get(0), HashSet::new));
    }

    @Test
    void testIntersect() {
        // Build the WayangPlan.
        List<Integer> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.intersectSquares(collector, 0, 1, 2, 3, 3, -1, -1, -2, -3, -3, -4);

        // Instantiate Wayang and activate the Java backend.
        makeAndRun(wayangPlan, FLINK);

        assertEquals(WayangCollections.asSet(1, 4, 9), WayangCollections.asSet(collector));
    }

    //TODO validate this test is required
    @Disabled
    @Test
    void testPageRankWithGraphBasic() {
        // Build the WayangPlan.
        List<Tuple2<Long, Long>> edges = Arrays.asList(
                new Tuple2<>(0L, 1L),
                new Tuple2<>(0L, 2L),
                new Tuple2<>(0L, 3L),
                new Tuple2<>(1L, 2L),
                new Tuple2<>(1L, 3L),
                new Tuple2<>(2L, 3L),
                new Tuple2<>(3L, 0L)
        );
        List<Tuple2<Long, Float>> pageRanks = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.pageRank(edges, pageRanks);

        // Execute the plan with a certain backend.
        WayangContext wayangContext = new WayangContext()
                .with(Flink.basicPlugin());
        wayangContext.execute(wayangPlan);


        // Check the results.
        pageRanks.sort((r1, r2) -> Float.compare(r2.getField1(), r1.getField1()));
        final List<Long> vertexOrder = pageRanks.stream().map(Tuple2::getField0).collect(Collectors.toList());
        assertEquals(
                Arrays.asList(3L, 0L, 2L, 1L),
                vertexOrder
        );
    }


    @Test
    void testMapPartitions() {
        // Execute the Wayang plan.
        final Collection<Tuple2<String, Integer>> result = new ArrayList<>();

        WayangPlan wayangPlan = WayangPlansOperators.mapPartitions(result, 0, 1, 1, 3, 3, 4, 4, 5, 5, 6);

        makeAndRun(wayangPlan, FLINK);

        assertEquals(
                WayangCollections.asSet(new Tuple2<>("even", 4), new Tuple2<>("odd", 6)),
                WayangCollections.asSet(result)
        );
    }

    @Test
    void testZipWithId() {
        // Build the WayangPlan.
        List<Long> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.zipWithId(collector, 0, 10, 20, 30, 30);

        // Instantiate Wayang and activate the Java backend.
        makeAndRun(wayangPlan, FLINK);

        assertEquals(1, collector.size());
        assertEquals(Long.valueOf(5L), collector.get(0));
    }

    @Test
    void testDiverseScenario1() {
        // Build the WayangPlan.
        WayangPlan wayangPlan = WayangPlans.diverseScenario1(WayangPlans.FILE_SOME_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        makeAndRun(wayangPlan, FLINK);
    }

    @Test
    void testDiverseScenario2() {
        // Build the WayangPlan.
        WayangPlan wayangPlan = WayangPlans.diverseScenario2(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        makeAndRun(wayangPlan, FLINK);
    }

    @Test
    void testDiverseScenario3() {
        // Build the WayangPlan.
        //TODO: need implement the loop for running this test
        //WayangPlan wayangPlan = WayangPlans.diverseScenario3(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        //makeAndRun(wayangPlan, FLINK);
    }

    @Test
    void testDiverseScenario4() {
        // Build the WayangPlan.
        //TODO: need implement the loop for running this test
        //WayangPlan wayangPlan = WayangPlans.diverseScenario4(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Wayang and activate the Java backend.
        //makeAndRun(wayangPlan, FLINK);
    }


    @Test
    void testSample() {
        // Build the WayangPlan.
        final List<Integer> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.simpleSample(3, collector, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Instantiate Wayang and activate the Java backend.
        makeAndRun(wayangPlan, FLINK);

        System.out.println(collector);
    }

}
