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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.plan.analyze.schema;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

class ClusterSchemaFetchExecutor {

  private final Coordinator coordinator;
  private final Supplier<Long> queryIdProvider;
  private final BiFunction<Long, Statement, ExecutionResult> statementExecutor;
  private final Function<PartialPath, Map<Integer, Template>> templateSetInfoProvider;

  private final Map<PartialPath, DeviceSchemaFetchTaskExecutor> deviceSchemaFetchTaskExecutorMap =
      new ConcurrentHashMap<>();

  ClusterSchemaFetchExecutor(
      Coordinator coordinator,
      Supplier<Long> queryIdProvider,
      BiFunction<Long, Statement, ExecutionResult> statementExecutor,
      Function<PartialPath, Map<Integer, Template>> templateSetInfoProvider) {
    this.coordinator = coordinator;
    this.queryIdProvider = queryIdProvider;
    this.statementExecutor = statementExecutor;
    this.templateSetInfoProvider = templateSetInfoProvider;
  }

  ClusterSchemaTree fetchSchemaOfOneDevice(PartialPath devicePath, List<String> measurements) {
    ClusterSchemaTree result =
        deviceSchemaFetchTaskExecutorMap
            .compute(
                devicePath,
                (key, value) -> {
                  if (value == null) {
                    value = new DeviceSchemaFetchTaskExecutor();
                    value.incReferenceCount();
                  }
                  return value;
                })
            .execute(devicePath, measurements);
    deviceSchemaFetchTaskExecutorMap.compute(
        devicePath,
        (key, value) -> {
          if (value == null || value.decAndGetReferenceCount() == 0) {
            return null;
          }
          return value;
        });
    return result;
  }

  ClusterSchemaTree executeSchemaFetchQuery(SchemaFetchStatement schemaFetchStatement) {
    long queryId = queryIdProvider.get();
    try {
      ExecutionResult executionResult = statementExecutor.apply(queryId, schemaFetchStatement);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            String.format(
                "cannot fetch schema, status is: %s, msg is: %s",
                executionResult.status.getCode(), executionResult.status.getMessage()));
      }
      try (SetThreadName threadName = new SetThreadName(executionResult.queryId.getId())) {
        ClusterSchemaTree result = new ClusterSchemaTree();
        Set<String> databaseSet = new HashSet<>();
        while (coordinator.getQueryExecution(queryId).hasNextResult()) {
          // The query will be transited to FINISHED when invoking getBatchResult() at the last time
          // So we don't need to clean up it manually
          Optional<TsBlock> tsBlock;
          try {
            tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
          } catch (IoTDBException e) {
            throw new RuntimeException("Fetch Schema failed. ", e);
          }
          if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
            break;
          }
          Column column = tsBlock.get().getColumn(0);
          for (int i = 0; i < column.getPositionCount(); i++) {
            parseFetchedData(column.getBinary(i), result, databaseSet);
          }
        }
        result.setDatabases(databaseSet);
        return result;
      }
    } finally {
      coordinator.cleanupQueryExecution(queryId);
    }
  }

  private void parseFetchedData(
      Binary data, ClusterSchemaTree resultSchemaTree, Set<String> databaseSet) {
    InputStream inputStream = new ByteArrayInputStream(data.getValues());
    try {
      byte type = ReadWriteIOUtils.readByte(inputStream);
      if (type == 0) {
        int size = ReadWriteIOUtils.readInt(inputStream);
        for (int i = 0; i < size; i++) {
          databaseSet.add(ReadWriteIOUtils.readString(inputStream));
        }
      } else if (type == 1) {
        resultSchemaTree.mergeSchemaTree(ClusterSchemaTree.deserialize(inputStream));
      } else {
        throw new RuntimeException(
            new MetadataException("Failed to fetch schema because of unrecognized data"));
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
  }

  private class DeviceSchemaFetchTaskExecutor {

    // buffer all the requests, waiting for execution
    private volatile DeviceSchemaFetchTask newTask;

    // task on executing
    private volatile DeviceSchemaFetchTask executingTask;

    // used for concurrent control of newTask R/W operation
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    // thread-safe, protected by deviceSchemaFetchTaskExecutorMap.compute
    private int referenceCount = 0;

    private ClusterSchemaTree execute(PartialPath devicePath, List<String> measurements) {
      DeviceSchemaFetchTask task = this.executingTask;
      // check whether the executing task can cover this request, if so, just waiting for this task
      if (task == null || !task.canCoverRequest(measurements)) {
        readWriteLock.readLock().lock();
        try {
          if ((task = this.newTask) == null) {
            synchronized (this) {
              if ((task = this.newTask) == null) {
                task = this.newTask = new DeviceSchemaFetchTask();
              }
            }
          }
          // this operation shall be blocked by task submitting operation
          // since once the task has been submitted, the info new added requests will be invalid
          task.addRequest(measurements);
        } finally {
          readWriteLock.readLock().unlock();
        }
        if (!task.isSubmitting()) {
          if (task.markSubmitting()) {
            // only one thread will execute this block
            while (true) {
              // waiting for execution
              if (this.executingTask == null) {
                // block all request adding operation
                readWriteLock.writeLock().lock();
                try {
                  // make this task as executing state for new request check
                  this.executingTask = task;
                  // make this field free for buffering new task
                  this.newTask = null;
                  break;
                } finally {
                  readWriteLock.writeLock().unlock();
                }
              }
            }
            // do execution and save fetched result
            task.saveResult(
                executeSchemaFetchQuery(
                    new SchemaFetchStatement(
                        task.generatePatternTree(devicePath),
                        templateSetInfoProvider.apply(devicePath),
                        false)));
            // release this field for new task execution
            this.executingTask = null;
          }
        }
      }

      // each request takes needed result from the fetched schema tree
      return task.extractResult(devicePath, measurements);
    }

    private void incReferenceCount() {
      referenceCount++;
    }

    private int decAndGetReferenceCount() {
      return --referenceCount;
    }
  }

  private static class DeviceSchemaFetchTask {

    private final Set<String> measurementSet = new HashSet<>();

    private volatile boolean hasSubmitThread = false;

    private volatile ClusterSchemaTree taskResult;

    private boolean canCoverRequest(List<String> measurements) {
      if (measurementSet.size() < measurements.size()) {
        return false;
      }
      for (String measurement : measurements) {
        if (!measurementSet.contains(measurement)) {
          return false;
        }
      }

      return true;
    }

    private void addRequest(List<String> measurements) {
      measurementSet.addAll(measurements);
    }

    private boolean isSubmitting() {
      return hasSubmitThread;
    }

    private boolean markSubmitting() {
      if (hasSubmitThread) {
        return false;
      }
      synchronized (this) {
        if (hasSubmitThread) {
          return false;
        }
        hasSubmitThread = true;
        return true;
      }
    }

    private PathPatternTree generatePatternTree(PartialPath devicePath) {
      PathPatternTree patternTree = new PathPatternTree();
      for (String measurement : measurementSet) {
        patternTree.appendFullPath(devicePath, measurement);
      }
      return patternTree;
    }

    private void saveResult(ClusterSchemaTree clusterSchemaTree) {
      taskResult = clusterSchemaTree;
    }

    private ClusterSchemaTree extractResult(PartialPath devicePath, List<String> measurements) {
      while (true) {
        if (taskResult != null) {
          break;
        }
      }
      return taskResult.extractDeviceSubTree(devicePath, measurements);
    }
  }
}
