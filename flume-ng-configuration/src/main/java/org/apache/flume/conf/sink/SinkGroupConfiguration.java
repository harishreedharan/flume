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
package org.apache.flume.conf.sink;

import java.util.Map;

import org.apache.flume.conf.Context;
import org.apache.flume.conf.AbstractComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;

public class SinkGroupConfiguration extends AbstractComponentConfiguration {
  private static final String PROCESSOR_PREFIX = "processor.";
  private Context processorContext;

  public SinkGroupConfiguration(Context context) throws ConfigurationException {
    super(context);
    configure();
  }

  @Override
  public void configure() throws ConfigurationException {
    Map<String, String> params = context.getSubProperties(PROCESSOR_PREFIX);
    processorContext = new Context();
    processorContext.putAll(params);

  }

}
