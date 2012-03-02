/**
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
package org.apache.flume.sink;

import java.util.List;
import java.util.Map;

import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkProcessorType;
import org.apache.flume.conf.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkProcessorFactory {
  private static final Logger logger = LoggerFactory
      .getLogger(SinkProcessorFactory.class);

  private static final String TYPE = "type";

  @SuppressWarnings("unchecked")
  public static SinkProcessor getProcessor(Context context, List<Sink> sinks) {
    Map<String, String> params = context.getParameters();
    SinkProcessor processor;
    String typeStr = params.get(TYPE);
    SinkProcessorType type = SinkProcessorType.DEFAULT;
    try {
      type = SinkProcessorType.valueOf(typeStr.toUpperCase());
    } catch (Exception ex) {
      logger.warn("Sink type {} does not exist, using default", typeStr);
    }

    Class<? extends SinkProcessor> processorClass = null;
    try {
      processorClass =
          (Class<? extends SinkProcessor>) Class.forName(type
              .getSinkProcessorClassName());
    } catch (Exception ex) {
      throw new FlumeException("Unable to load sink processor type: " + typeStr
          + ", class: " + type.getSinkProcessorClassName(), ex);
    }
    try {
      processor = processorClass.newInstance();
    } catch (Exception e) {
      throw new FlumeException("Unable to create processor, type: " + typeStr
          + ", class: " + type.getSinkProcessorClassName(), e);
    }

    processor.setSinks(sinks);
    processor.configure(context);
    return processor;
  }
}
