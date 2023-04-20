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
package org.apache.iotdb.db.engine.flush.tasks;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

public class FlushDeviceContext {
  private IDeviceID deviceID;
  private List<String> measurementIds;
  private IChunkWriter[] chunkWriters;
  private AtomicInteger encodedCounter = new AtomicInteger();
  private Map<String, Integer> seriesIndexMap;


  public AtomicInteger getEncodedCounter() {
    return encodedCounter;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  public void setDeviceID(IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  public List<String> getMeasurementIds() {
    return measurementIds;
  }

  public void setMeasurementIds(List<String> measurementIds) {
    this.measurementIds = measurementIds;
  }

  public IChunkWriter[] getChunkWriters() {
    return chunkWriters;
  }

  public void setChunkWriters(IChunkWriter[] chunkWriters) {
    this.chunkWriters = chunkWriters;
  }

  public Map<String, Integer> getSeriesIndexMap() {
    return seriesIndexMap;
  }

  public void setSeriesIndexMap(Map<String, Integer> seriesIndexMap) {
    this.seriesIndexMap = seriesIndexMap;
  }

  public boolean isFullyEncoded() {
    return encodedCounter.get() == measurementIds.size();
  }
}