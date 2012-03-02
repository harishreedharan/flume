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

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.conf.Context;
import org.apache.flume.conf.AbstractComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;

import com.google.common.collect.ImmutableMap;

public class FailoverSinkProcessorConfiguration extends
    AbstractComponentConfiguration {

  private static final String PRIORITY_PREFIX = "priority.";
  private Map<String, Integer> priorityMap;

  public FailoverSinkProcessorConfiguration(Context context)
      throws ConfigurationException {
    super(context);
    configure();

  }

  @Override
  public void configure() throws ConfigurationException {
    ImmutableMap<String, String> priorities =
        context.getSubProperties(PRIORITY_PREFIX);
    priorityMap = new HashMap<String, Integer>();
    for (String sink : priorities.keySet()) {
      try {
        priorityMap.put(sink, Integer.parseInt(priorities.get(sink)));
      } catch (NumberFormatException e) {
        throw new ConfigurationException("Invalid priority!");
      }
    }
  }
}
