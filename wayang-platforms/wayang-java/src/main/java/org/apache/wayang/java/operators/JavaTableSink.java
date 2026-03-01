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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.util.SqlTypeUtils;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class JavaTableSink<T> extends TableSink<T> implements JavaExecutionOperator {

    private void setRecordValue(PreparedStatement ps, int index, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.NULL);
        } else if (value instanceof Integer) {
            ps.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            ps.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            ps.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            ps.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            ps.setBoolean(index, (Boolean) value);
        } else if (value instanceof java.sql.Date) {
            ps.setDate(index, (java.sql.Date) value);
        } else if (value instanceof java.sql.Timestamp) {
            ps.setTimestamp(index, (java.sql.Timestamp) value);
        } else {
            ps.setString(index, value.toString());
        }
    }

    public JavaTableSink(Properties props, String mode, String tableName) {
        this(props, mode, tableName, null);
    }

    public JavaTableSink(Properties props, String mode, String tableName, String... columnNames) {
        super(props, mode, tableName, columnNames);

    }

    public JavaTableSink(Properties props, String mode, String tableName, String[] columnNames, DataSetType<T> type) {
        super(props, mode, tableName, columnNames, type);

    }

    public JavaTableSink(TableSink<T> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 0;
        JavaChannelInstance input = (JavaChannelInstance) inputs[0];

        // The stream is converted to an Iterator so that we can read the first element
        // w/o consuming the entire stream.
        Iterator<T> recordIterator = input.<T>provideStream().iterator();

        if (!recordIterator.hasNext()) {
            return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
        }

        // We read the first element to derive the Record schema.
        T firstElement = recordIterator.next();
        Class<?> typeClass = this.getType().getDataUnitType().getTypeClass();

        String url = this.getProperties().getProperty("url");
        org.apache.calcite.sql.SqlDialect.DatabaseProduct product = SqlTypeUtils.detectProduct(url);

        List<SqlTypeUtils.SchemaField> schemaFields;
        if (typeClass != Record.class) {
            schemaFields = SqlTypeUtils.getSchema(typeClass, product);
        } else {
            schemaFields = SqlTypeUtils.getSchema((Record) firstElement, product, this.getColumnNames());
        }

        String[] currentColumnNames = this.getColumnNames();
        if (currentColumnNames == null || currentColumnNames.length == 0) {
            currentColumnNames = new String[schemaFields.size()];
            for (int i = 0; i < schemaFields.size(); i++) {
                currentColumnNames[i] = schemaFields.get(i).getName();
            }
            this.setColumnNames(currentColumnNames);
        }

        String[] sqlTypes = new String[currentColumnNames.length];
        for (int i = 0; i < currentColumnNames.length; i++) {
            sqlTypes[i] = "VARCHAR(255)"; // Default
            for (SqlTypeUtils.SchemaField field : schemaFields) {
                if (field.getName().equals(currentColumnNames[i])) {
                    sqlTypes[i] = field.getSqlType();
                    break;
                }
            }
        }

        final String[] finalColumnNames = currentColumnNames;
        final String[] finalSqlTypes = sqlTypes;

        this.getProperties().setProperty("streamingBatchInsert", "True");

        Connection conn;
        try {
            Class.forName(this.getProperties().getProperty("driver"));
            conn = DriverManager.getConnection(this.getProperties().getProperty("url"), this.getProperties());
            conn.setAutoCommit(false);

            Statement stmt = conn.createStatement();

            // Drop existing table if the mode is 'overwrite'.
            if (this.getMode().equals("overwrite")) {
                stmt.execute("DROP TABLE IF EXISTS " + this.getTableName());
            }

            // Create a new table if the specified table name does not exist yet.
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE IF NOT EXISTS ").append(this.getTableName()).append(" (");
            String separator = "";
            for (int i = 0; i < finalColumnNames.length; i++) {
                sb.append(separator).append("\"").append(finalColumnNames[i]).append("\" ").append(finalSqlTypes[i]);
                separator = ", ";
            }
            sb.append(")");
            stmt.execute(sb.toString());

            // Create a prepared statement to insert value from the recordIterator.
            sb = new StringBuilder();
            sb.append("INSERT INTO ").append(this.getTableName()).append(" (");
            separator = "";
            for (int i = 0; i < finalColumnNames.length; i++) {
                sb.append(separator).append("\"").append(finalColumnNames[i]).append("\"");
                separator = ", ";
            }
            sb.append(") VALUES (");
            separator = "";
            for (int i = 0; i < finalColumnNames.length; i++) {
                sb.append(separator).append("?");
                separator = ", ";
            }
            sb.append(")");
            PreparedStatement ps = conn.prepareStatement(sb.toString());

            // The schema Record has to be pushed to the database too.
            this.pushToStatement(ps, firstElement, typeClass, finalColumnNames);
            ps.addBatch();

            // Iterate through all remaining records and add them to the prepared statement
            recordIterator.forEachRemaining(
                    r -> {
                        try {
                            this.pushToStatement(ps, r, typeClass, finalColumnNames);
                            ps.addBatch();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });

            ps.executeBatch();
            conn.commit();
            conn.close();
        } catch (ClassNotFoundException e) {
            System.out.println("Please specify a correct database driver.");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    private void pushToStatement(PreparedStatement ps, T element, Class<?> typeClass, String[] columnNames)
            throws SQLException {
        if (typeClass == Record.class) {
            Record r = (Record) element;
            for (int i = 0; i < columnNames.length; i++) {
                setRecordValue(ps, i + 1, r.getField(i));
            }
        } else {
            for (int i = 0; i < columnNames.length; i++) {
                Object val = ReflectionUtils.getProperty(element, columnNames[i]);
                setRecordValue(ps, i + 1, val);
            }
        }
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.tablesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

}
