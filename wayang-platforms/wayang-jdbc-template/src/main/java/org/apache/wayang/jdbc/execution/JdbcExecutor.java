package org.apache.wayang.jdbc.execution;
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
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.ExecutorTemplate;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.*;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class JdbcExecutor extends ExecutorTemplate {

    private final JdbcPlatformTemplate platform;
    private final Connection connection;
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public JdbcExecutor(final JdbcPlatformTemplate platform, final Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection();
    }

    @Override
    public void execute(final ExecutionStage stage,
                        final OptimizationContext optimizationContext,
                        final ExecutionState executionState) {

        final Tuple2<String, SqlQueryChannel.Instance> pair =
                JdbcExecutor.createSqlQuery(stage, optimizationContext, this);

        final String query = pair.field0;
        final SqlQueryChannel.Instance queryChannel = pair.field1;

        queryChannel.setSqlQuery(query);
        executionState.register(queryChannel);
    }

    /**
     * Safe version (removes WayangCollections.getSingle crash)
     */
    private static ExecutionTask findJdbcExecutionOperatorTaskInStage(
            final ExecutionTask task,
            final ExecutionStage stage) {

        assert task.getNumOuputChannels() == 1;

        final Channel outputChannel = task.getOutputChannel(0);

        if (outputChannel.getConsumers().size() != 1) {
            return null;
        }

        final ExecutionTask consumer = outputChannel.getConsumers().iterator().next();

        return consumer.getStage() == stage &&
                consumer.getOperator() instanceof JdbcExecutionOperator
                ? consumer
                : null;
    }

    private static SqlQueryChannel.Instance instantiateOutboundChannel(
            final ExecutionTask task,
            final OptimizationContext optimizationContext,
            final JdbcExecutor jdbcExecutor) {

        assert task.getNumOuputChannels() == 1;
        assert task.getOutputChannel(0) instanceof SqlQueryChannel;

        final SqlQueryChannel outputChannel = (SqlQueryChannel) task.getOutputChannel(0);

        final OptimizationContext.OperatorContext operatorContext =
                optimizationContext.getOperatorContext(task.getOperator());

        return outputChannel.createInstance(jdbcExecutor, operatorContext, 0);
    }

    private static SqlQueryChannel.Instance instantiateOutboundChannel(
            final ExecutionTask task,
            final OptimizationContext optimizationContext,
            final SqlQueryChannel.Instance predecessorChannelInstance,
            final JdbcExecutor jdbcExecutor) {

        final SqlQueryChannel.Instance newInstance =
                instantiateOutboundChannel(task, optimizationContext, jdbcExecutor);

        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());

        return newInstance;
    }

    /**
     * Main SQL builder
     */
    protected static Tuple2<String, SqlQueryChannel.Instance> createSqlQuery(
            final ExecutionStage stage,
            final OptimizationContext context,
            final JdbcExecutor jdbcExecutor) {

        final Collection<?> startTasks = stage.getStartTasks();
        final Collection<?> termTasks = stage.getTerminalTasks();

        if (startTasks.isEmpty()) {
            throw new WayangException("Invalid jdbc stage: no sources found");
        }

        final ExecutionTask startTask = (ExecutionTask) startTasks.iterator().next();

        if (termTasks.size() != 1) {
            throw new WayangException("Invalid JDBC stage: multiple terminal tasks not supported.");
        }

        final JdbcTableSource tableOp = (JdbcTableSource) startTask.getOperator();

        SqlQueryChannel.Instance tipChannelInstance =
                instantiateOutboundChannel(startTask, context, jdbcExecutor);

        final Collection<JdbcFilterOperator> filterTasks = new ArrayList<>(4);
        JdbcProjectionOperator projectionTask = null;
        final Collection<JdbcJoinOperator<?>> joinTasks = new ArrayList<>();

        final Set<ExecutionTask> allTasks = stage.getAllTasks();

        ExecutionTask nextTask =
                findJdbcExecutionOperatorTaskInStage(startTask, stage);

        while (nextTask != null) {

            if (nextTask.getOperator() instanceof JdbcFilterOperator filterOperator) {
                filterTasks.add(filterOperator);
            } else if (nextTask.getOperator() instanceof JdbcProjectionOperator projectionOperator) {
                projectionTask = projectionOperator;
            } else if (nextTask.getOperator() instanceof JdbcJoinOperator joinOperator) {
                joinTasks.add(joinOperator);
            } else {
                throw new WayangException("Unsupported JDBC execution task " + nextTask);
            }

            tipChannelInstance =
                    instantiateOutboundChannel(nextTask, context, tipChannelInstance, jdbcExecutor);

            nextTask =
                    findJdbcExecutionOperatorTaskInStage(nextTask, stage);
        }

        final StringBuilder query =
                createSqlString(jdbcExecutor, tableOp, filterTasks, projectionTask, joinTasks);

        return new Tuple2<>(query.toString(), tipChannelInstance);
    }

    public static StringBuilder createSqlString(
            final JdbcExecutor jdbcExecutor,
            final JdbcTableSource tableOp,
            final Collection<JdbcFilterOperator> filterTasks,
            JdbcProjectionOperator projectionTask,
            final Collection<JdbcJoinOperator<?>> joinTasks) {

        final String tableName =
                tableOp.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);

        final Collection<String> conditions =
                filterTasks.stream()
                        .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler))
                        .collect(Collectors.toList());

        final String projection =
                projectionTask == null
                        ? "*"
                        : projectionTask.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);

        final Collection<String> joins =
                joinTasks.stream()
                        .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler))
                        .collect(Collectors.toList());

        final StringBuilder sb = new StringBuilder(1000);

        sb.append("SELECT ").append(projection).append(" FROM ").append(tableName);

        for (final String join : joins) {
            sb.append(" ").append(join);
        }

        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            String separator = "";
            for (final String condition : conditions) {
                sb.append(separator).append(condition);
                separator = " AND ";
            }
        }

        sb.append(';');

        return sb;
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (final SQLException e) {
            this.logger.error("Could not close JDBC connection correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    private void saveResult(final FileChannel.Instance outputFileChannelInstance,
                            final ResultSet rs)
            throws IOException, SQLException {

        final FileSystem outFs =
                FileSystems.getFileSystem(outputFileChannelInstance.getSinglePath()).get();

        try (final OutputStreamWriter writer =
                     new OutputStreamWriter(outFs.create(outputFileChannelInstance.getSinglePath()))) {

            while (rs.next()) {

                final ResultSetMetaData rsmd = rs.getMetaData();

                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) writer.write('\t');
                }

                if (!rs.isLast()) writer.write('\n');
            }

        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }
}