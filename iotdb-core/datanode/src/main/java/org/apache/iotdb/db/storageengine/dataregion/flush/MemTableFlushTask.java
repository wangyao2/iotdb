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
package org.apache.iotdb.db.storageengine.dataregion.flush;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IDeviceID;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * flush task to flush one memtable using a pipeline model to flush, which is sort memtable ->
 * encoding -> write to disk (io task)
 */
public class MemTableFlushTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);
    private static final FlushSubTaskPoolManager SUB_TASK_POOL_MANAGER =
            FlushSubTaskPoolManager.getInstance();
    private static final WritingMetrics WRITING_METRICS = WritingMetrics.getInstance();
    private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    //private final Future<?> encodingTaskFuture;//这几个地方都是私有的，看来修改不用逃出这个类
    private final Future<?> ioTaskFuture;
    private RestorableTsFileIOWriter writer;

    private final LinkedBlockingQueue<Object> encodingTaskQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Object> ioTaskQueue =
            (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo())
                    ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
                    : new LinkedBlockingQueue<>();

    private String storageGroup;
    private String dataRegionId;

    private IMemTable memTable;

    private volatile long memSerializeTime = 0L;
    private volatile long ioTime = 0L;

    /**
     * @param memTable     the memTable to flush
     * @param writer       the writer where memTable will be flushed to (current tsfile writer or vm writer)
     * @param storageGroup current database
     */
    public MemTableFlushTask(
            IMemTable memTable,
            RestorableTsFileIOWriter writer,
            String storageGroup,
            String dataRegionId) {
        this.memTable = memTable;
        this.writer = writer;
        this.storageGroup = storageGroup;
        this.dataRegionId = dataRegionId;
        //this.encodingTaskFuture = SUB_TASK_POOL_MANAGER.submit(encodingTask);//卧槽，在构造方法的时候，就先把两个任务启动了
        //修改的时候，应该就不再需要启动这个编码任务了
        this.ioTaskFuture = SUB_TASK_POOL_MANAGER.submit(ioTask);
        LOGGER.debug(
                "flush task of database {} memtable is created, flushing to file {}.",
                storageGroup,
                writer.getFile().getName());
    }

    /**
     * the function for flushing memtable.
     */
    @SuppressWarnings("squid:S3776")
    public void syncFlushMemTable() throws ExecutionException, InterruptedException {
        long avgSeriesPointsNum =
                memTable.getSeriesNumber() == 0
                        ? 0
                        : memTable.getTotalPointsNum() / memTable.getSeriesNumber();
        LOGGER.info(
                "The memTable size of SG {} is {}, the avg series points num in chunk is {}, total timeseries number is {}",
                storageGroup,
                memTable.memSize(),
                avgSeriesPointsNum,
                memTable.getSeriesNumber());
        WRITING_METRICS.recordFlushingMemTableStatus(
                storageGroup,
                memTable.memSize(),
                memTable.getSeriesNumber(),
                memTable.getTotalPointsNum(),
                avgSeriesPointsNum);

        long estimatedTemporaryMemSize = 0L;
        if (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo()) {
            estimatedTemporaryMemSize =
                    memTable.getSeriesNumber() == 0
                            ? 0
                            : memTable.memSize()
                            / memTable.getSeriesNumber()
                            * config.getIoTaskQueueSizeForFlushing();
            SystemInfo.getInstance().applyTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
        }
        long start = System.currentTimeMillis();
        long sortTime = 0;

        // for map do not use get(key) to iterate
        Map<IDeviceID, IWritableMemChunkGroup> memTableMap = memTable.getMemTableMap();
        List<IDeviceID> deviceIDList = new ArrayList<>(memTableMap.keySet());
        // sort the IDeviceID in lexicographical order
        deviceIDList.sort(Comparator.comparing(IDeviceID::toStringID));//按照字典序给他排序
        for (IDeviceID deviceID : deviceIDList) {
            final Map<String, IWritableMemChunk> value = memTableMap.get(deviceID).getMemChunkMap();//拿到每一个设备中的所有序列
            // skip the empty device/chunk group
            if (memTableMap.get(deviceID).count() == 0 || value.isEmpty()) {
                continue;
            }
            //原本是放入到encoding,编码中的，直接发给到io当中
            //encodingTaskQueue.put(new StartFlushGroupIOTask(deviceID.toStringID()));

            try {
                ioTaskQueue.put(new StartFlushGroupIOTask(deviceID.toStringID()));
            } catch (InterruptedException e) {
                LOGGER.error(
                        "Database {} memtable flushing to file {}, encoding task is interrupted.",
                        storageGroup,
                        writer.getFile().getName(),
                        e);
                // generally it is because the thread pool is shutdown so the task should be aborted
                break;
            }

            List<String> seriesInOrder = new ArrayList<>(value.keySet());//拿到一个设备中的所有序列名
            seriesInOrder.sort((String::compareTo));//给序列名字排序
            for (String seriesId : seriesInOrder) {
                long startTime = System.currentTimeMillis();
                IWritableMemChunk series = value.get(seriesId);//获得一个Device中一个measurement的对应的memchunk
                if (series.count() == 0) {
                    continue;
                }
                /*
                 * sort task (first task of flush pipeline)
                 */
                series.sortTvListForFlush();//这个命名做的很差，series对应的是一个序列memchunk，这个方法进去之后就是WritableMemChunk里面负责排序的方法了
                //排序的最小单位是一个measurement，排序完之后，直接去encoding

                //现在我要做的就是把编码也放在这个地方，好好构思怎么处理这一块内容
                long subTaskTime = System.currentTimeMillis() - startTime;
                sortTime += subTaskTime;
                WRITING_METRICS.recordFlushSubTaskCost(WritingMetrics.SORT_TASK, subTaskTime);
                //encodingTaskQueue.put(series);//排序好了之后，放入到编码队列当中，等待编码线程的处理吗
                //现在我们不放入到队列中，直接在这个线程中完成这个 measurement的编码处理
                //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^新添加的编码处理代码
                long starTime = System.currentTimeMillis();
                IWritableMemChunk writableMemChunk = series;
                IChunkWriter seriesWriter = writableMemChunk.createIChunkWriter();
                writableMemChunk.encode(seriesWriter);//这一句话是进行编码的,series排序后直接编码
                seriesWriter.sealCurrentPage();
                seriesWriter.clearPageWriter();
                System.out.println("请注意，已经成功完成了memtable的排序后刷写工作！");
                try {
                    ioTaskQueue.put(seriesWriter);
                } catch (InterruptedException e) {
                    LOGGER.error("Put task into ioTaskQueue Interrupted");
                    Thread.currentThread().interrupt();
                }
                long subTaskTime2 = System.currentTimeMillis() - starTime;
                WRITING_METRICS.recordFlushSubTaskCost(WritingMetrics.ENCODING_TASK, subTaskTime2);
                memSerializeTime += subTaskTime2;
                //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^新添加的编码处理代码
            }
            //encodingTaskQueue.put(new EndChunkGroupIoTask());
            try {//为什么加try catch目前还不清楚，但是在其他地方反正是都加了
                ioTaskQueue.put(new EndChunkGroupIoTask());
            } catch (InterruptedException e) {
                LOGGER.error(
                        "Database {} memtable flushing to file {}, encoding task is interrupted.",
                        storageGroup,
                        writer.getFile().getName(),
                        e);
            }
        }
        ioTaskQueue.put(new TaskEnd());

        LOGGER.debug(
                "Database {} memtable flushing into file {}: data sort time cost {} ms.",
                storageGroup,
                writer.getFile().getName(),
                sortTime);
        WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_SORT, sortTime);

//    try {//编码不需要了，直接删掉这一部分
//      encodingTaskFuture.get();
//    } catch (InterruptedException | ExecutionException e) {
//      ioTaskFuture.cancel(true);
//      if (e instanceof InterruptedException) {
//        Thread.currentThread().interrupt();
//      }
//      throw e;
//    }

        ioTaskFuture.get();

        try {
            long writePlanIndicesStartTime = System.currentTimeMillis();
            writer.writePlanIndices();
            WRITING_METRICS.recordFlushCost(
                    WritingMetrics.WRITE_PLAN_INDICES,
                    System.currentTimeMillis() - writePlanIndicesStartTime);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }

        if (config.isEnableMemControl()) {
            if (estimatedTemporaryMemSize != 0) {
                SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
            }
            SystemInfo.getInstance().setEncodingFasterThanIo(ioTime >= memSerializeTime);
        }

        MetricService.getInstance()
                .timer(
                        System.currentTimeMillis() - start,
                        TimeUnit.MILLISECONDS,
                        Metric.COST_TASK.toString(),
                        MetricLevel.CORE,
                        Tag.NAME.toString(),
                        "flush");

        LOGGER.info(
                "Database {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
                storageGroup,
                memTable,
                System.currentTimeMillis() - start);
    }

    /**
     * io task (third task of pipeline)
     */
    @SuppressWarnings("squid:S135")
    private Runnable ioTask =
            () -> {
                LOGGER.debug(
                        "Database {} memtable flushing to file {} start io.",
                        storageGroup,
                        writer.getFile().getName());
                while (true) {
                    Object ioMessage = null;
                    try {
                        ioMessage = ioTaskQueue.take();
                    } catch (InterruptedException e1) {
                        LOGGER.error("take task from ioTaskQueue Interrupted");
                        Thread.currentThread().interrupt();
                        break;
                    }
                    long starTime = System.currentTimeMillis();
                    try {
                        if (ioMessage instanceof StartFlushGroupIOTask) {//判断一下获取的数据类型是什么
                            this.writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
                        } else if (ioMessage instanceof TaskEnd) {
                            break;
                        } else if (ioMessage instanceof EndChunkGroupIoTask) {
                            this.writer.setMinPlanIndex(memTable.getMinPlanIndex());
                            this.writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
                            this.writer.endChunkGroup();
                        } else {
                            ((IChunkWriter) ioMessage).writeToFileWriter(this.writer);//就这一行是用来写入的TSfile文件的
                        }
                    } catch (IOException e) {
                        LOGGER.error(
                                "Database {} memtable {}, io task meets error.", storageGroup, memTable, e);
                        return;
                    }
                    long subTaskTime = System.currentTimeMillis() - starTime;
                    ioTime += subTaskTime;
                    WRITING_METRICS.recordFlushSubTaskCost(WritingMetrics.IO_TASK, subTaskTime);
                }
                LOGGER.debug(
                        "flushing a memtable to file {} in database {}, io cost {}ms",
                        writer.getFile().getName(),
                        storageGroup,
                        ioTime);
                WRITING_METRICS.recordFlushTsFileSize(storageGroup, writer.getFile().length());
                WRITING_METRICS.recordFlushCost(WritingMetrics.FLUSH_STAGE_IO, ioTime);
            };

    static class TaskEnd {

        TaskEnd() {
        }
    }

    static class EndChunkGroupIoTask {

        EndChunkGroupIoTask() {
        }
    }

    static class StartFlushGroupIOTask {

        private final String deviceId;

        StartFlushGroupIOTask(String deviceId) {
            this.deviceId = deviceId;
        }
    }
}
