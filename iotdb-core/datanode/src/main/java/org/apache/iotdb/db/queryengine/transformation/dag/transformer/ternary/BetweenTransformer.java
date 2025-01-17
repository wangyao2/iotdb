/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.ternary;

import org.apache.iotdb.db.queryengine.transformation.api.LayerPointReader;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TransformUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

public class BetweenTransformer extends CompareTernaryTransformer {

  boolean isNotBetween;

  public BetweenTransformer(
      LayerPointReader firstPointReader,
      LayerPointReader secondPointReader,
      LayerPointReader thirdPointReader,
      boolean isNotBetween) {
    super(firstPointReader, secondPointReader, thirdPointReader);
    this.isNotBetween = isNotBetween;
  }

  @Override
  protected Evaluator constructNumberEvaluator() {
    return () ->
        ((Double.compare(
                        castCurrentValueToDoubleOperand(firstPointReader, firstPointReaderDataType),
                        castCurrentValueToDoubleOperand(
                            secondPointReader, secondPointReaderDataType))
                    >= 0)
                && (Double.compare(
                        castCurrentValueToDoubleOperand(firstPointReader, firstPointReaderDataType),
                        castCurrentValueToDoubleOperand(thirdPointReader, thirdPointReaderDataType))
                    <= 0))
            ^ isNotBetween;
  }

  @Override
  protected Evaluator constructTextEvaluator() {
    return () ->
        ((TransformUtils.compare(
                        firstPointReader
                            .currentBinary()
                            .getStringValue(TSFileConfig.STRING_CHARSET),
                        secondPointReader
                            .currentBinary()
                            .getStringValue(TSFileConfig.STRING_CHARSET))
                    >= 0)
                && (TransformUtils.compare(
                        firstPointReader
                            .currentBinary()
                            .getStringValue(TSFileConfig.STRING_CHARSET),
                        thirdPointReader
                            .currentBinary()
                            .getStringValue(TSFileConfig.STRING_CHARSET))
                    <= 0))
            ^ isNotBetween;
  }
}
