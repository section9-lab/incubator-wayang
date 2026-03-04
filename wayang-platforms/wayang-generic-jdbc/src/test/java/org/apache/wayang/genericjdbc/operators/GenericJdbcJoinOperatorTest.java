package org.apache.wayang.genericjdbc.operators;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.genericjdbc.GenericJdbc;
import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;
import org.apache.wayang.java.platform.JavaPlatform;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GenericJdbcJoinOperatorTest {

    @Test
    void testJoinExecution() throws Exception {

        String url = "jdbc:hsqldb:mem:wayang_test_db;DB_CLOSE_DELAY=-1";

        Class.forName("org.hsqldb.jdbcDriver");

        try (Connection conn = DriverManager.getConnection(url, "SA", "")) {
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE T1 (A INT, VAL1 VARCHAR(20));");
            stmt.execute("INSERT INTO T1 VALUES (1, 'Apache');");

            stmt.execute("CREATE TABLE T2 (A INT, VAL2 INT);");
            stmt.execute("INSERT INTO T2 VALUES (1, 2026);");
        }

        Configuration config = new Configuration();
        config.setProperty("wayang.genericjdbc.jdbc.url", url);
        config.setProperty("wayang.genericjdbc.jdbc.user", "SA");
        config.setProperty("wayang.genericjdbc.jdbc.password", "");
        config.setProperty("wayang.genericjdbc.jdbc.driverName", "org.hsqldb.jdbcDriver");

        GenericJdbcTableSource source1 = new GenericJdbcTableSource("T1", "A", "VAL1");
        GenericJdbcTableSource source2 = new GenericJdbcTableSource("T2", "A", "VAL2");

        TransformationDescriptor<Record, Integer> keyExtractor0 =
                new TransformationDescriptor<>(
                        r -> (Integer) r.getField(0),
                        Record.class,
                        Integer.class
                ).withSqlImplementation("T1", "A");

        TransformationDescriptor<Record, Integer> keyExtractor1 =
                new TransformationDescriptor<>(
                        r -> (Integer) r.getField(0),
                        Record.class,
                        Integer.class
                ).withSqlImplementation("T2", "A");

        JoinOperator<Record, Record, Integer> join =
                new JoinOperator<>(keyExtractor0, keyExtractor1);

        List<Tuple2<Record, Record>> results = new ArrayList<>();

        @SuppressWarnings("unchecked")
        LocalCallbackSink<Tuple2<Record, Record>> sink =
                LocalCallbackSink.createCollectingSink(
                        results,
                        (Class<Tuple2<Record, Record>>) (Class<?>) Tuple2.class
                );

        source1.addTargetPlatform(GenericJdbcPlatform.getInstance());
        source2.addTargetPlatform(GenericJdbcPlatform.getInstance());
        join.addTargetPlatform(JavaPlatform.getInstance());
        sink.addTargetPlatform(JavaPlatform.getInstance());

        source1.connectTo(0, join, 0);
        source2.connectTo(0, join, 1);
        join.connectTo(0, sink, 0);

        WayangContext ctx = new WayangContext(config)
                .with(GenericJdbc.plugin())
                .with(org.apache.wayang.java.Java.basicPlugin());

        ctx.execute(new WayangPlan(sink));

        assertEquals(1, results.size());
    }
}