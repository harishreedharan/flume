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

import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Context;

/**
 * Configuration concept for handling multiple sinks working together.
 * 
 */
public class SinkGroup implements Configurable {
  private static final String PROCESSOR_PREFIX = "processor.";
  List<Sink> sinks;
  SinkProcessor processor;

  public SinkGroup(List<Sink> groupSinks) {
    sinks = groupSinks;
  }

  @Override
  public void configure(Context context) {
    Context processorContext = new Context();
    Map<String, String> subparams = context.getSubProperties(PROCESSOR_PREFIX);
    processorContext.putAll(subparams);
    processor = SinkProcessorFactory.getProcessor(processorContext, sinks);
  }

  public SinkProcessor getProcessor() {
    return processor;
  }
}
