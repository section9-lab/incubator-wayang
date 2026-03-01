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

package org.apache.wayang.basic.util;

import org.apache.calcite.sql.SqlDialect;
import org.apache.wayang.basic.data.Record;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlTypeUtilsTest {

    @Test
    public void testDetectProduct() {
        assertEquals(SqlDialect.DatabaseProduct.POSTGRESQL,
                SqlTypeUtils.detectProduct("jdbc:postgresql://localhost:5432/db"));
        assertEquals(SqlDialect.DatabaseProduct.MYSQL, SqlTypeUtils.detectProduct("jdbc:mysql://localhost:3306/db"));
        assertEquals(SqlDialect.DatabaseProduct.ORACLE,
                SqlTypeUtils.detectProduct("jdbc:oracle:thin:@localhost:1521:xe"));
        assertEquals(SqlDialect.DatabaseProduct.H2, SqlTypeUtils.detectProduct("jdbc:h2:mem:test"));
        assertEquals(SqlDialect.DatabaseProduct.DERBY,
                SqlTypeUtils.detectProduct("jdbc:derby:memory:test;create=true"));
        assertEquals(SqlDialect.DatabaseProduct.MSSQL,
                SqlTypeUtils.detectProduct("jdbc:sqlserver://localhost:1433;databaseName=db"));
        assertEquals(SqlDialect.DatabaseProduct.UNKNOWN, SqlTypeUtils.detectProduct("jdbc:unknown:db"));
    }

    @Test
    public void testGetSqlTypeDefault() {
        SqlDialect.DatabaseProduct product = SqlDialect.DatabaseProduct.UNKNOWN;
        assertEquals("INT", SqlTypeUtils.getSqlType(Integer.class, product));
        assertEquals("INT", SqlTypeUtils.getSqlType(int.class, product));
        assertEquals("BIGINT", SqlTypeUtils.getSqlType(Long.class, product));
        assertEquals("DOUBLE", SqlTypeUtils.getSqlType(Double.class, product));
        assertEquals("VARCHAR(255)", SqlTypeUtils.getSqlType(String.class, product));
        assertEquals("DATE", SqlTypeUtils.getSqlType(Date.class, product));
        assertEquals("TIMESTAMP", SqlTypeUtils.getSqlType(Timestamp.class, product));
    }

    @Test
    public void testGetSqlTypePostgres() {
        SqlDialect.DatabaseProduct product = SqlDialect.DatabaseProduct.POSTGRESQL;
        assertEquals("INT", SqlTypeUtils.getSqlType(Integer.class, product));
        assertEquals("DOUBLE PRECISION", SqlTypeUtils.getSqlType(Double.class, product));
        assertEquals("DOUBLE PRECISION", SqlTypeUtils.getSqlType(double.class, product));
        assertEquals("VARCHAR(255)", SqlTypeUtils.getSqlType(String.class, product));
    }

    @Test
    public void testGetSchema() {
        List<SqlTypeUtils.SchemaField> schema = SqlTypeUtils.getSchema(TestPojo.class,
                SqlDialect.DatabaseProduct.POSTGRESQL);
        assertEquals(3, schema.size());

        assertEquals("id", schema.get(0).getName());
        assertEquals("INT", schema.get(0).getSqlType());

        assertEquals("name", schema.get(1).getName());
        assertEquals("VARCHAR(255)", schema.get(1).getSqlType());

        assertEquals("value", schema.get(2).getName());
        assertEquals("DOUBLE PRECISION", schema.get(2).getSqlType());
    }

    @Test
    public void testGetSchemaRecord() {
        Record record = new Record(1, "test", 1.5);
        List<SqlTypeUtils.SchemaField> schema = SqlTypeUtils.getSchema(record, SqlDialect.DatabaseProduct.POSTGRESQL,
                null);

        assertEquals(3, schema.size());
        assertEquals("c_0", schema.get(0).getName());
        assertEquals("INT", schema.get(0).getSqlType());
        assertEquals(Integer.class, schema.get(0).getJavaClass());

        assertEquals("c_1", schema.get(1).getName());
        assertEquals("VARCHAR(255)", schema.get(1).getSqlType());
        assertEquals(String.class, schema.get(1).getJavaClass());

        assertEquals("c_2", schema.get(2).getName());
        assertEquals("DOUBLE PRECISION", schema.get(2).getSqlType());
        assertEquals(Double.class, schema.get(2).getJavaClass());
    }

    @Test
    public void testGetSchemaRecordWithNames() {
        Record record = new Record(1, "test");
        String[] names = { "id", "description" };
        List<SqlTypeUtils.SchemaField> schema = SqlTypeUtils.getSchema(record, SqlDialect.DatabaseProduct.POSTGRESQL,
                names);

        assertEquals(2, schema.size());
        assertEquals("id", schema.get(0).getName());
        assertEquals("description", schema.get(1).getName());
    }

    public static class TestPojo {
        public int id;
        public String name;
        public Double value;
    }
}
