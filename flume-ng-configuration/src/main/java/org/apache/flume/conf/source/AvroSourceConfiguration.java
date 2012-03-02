/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.conf.source;

import org.apache.flume.conf.Context;
import org.apache.flume.conf.AbstractComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;

public class AvroSourceConfiguration extends AbstractComponentConfiguration {

  private int port;
  private String bind;

  public AvroSourceConfiguration(Context context) throws ConfigurationException {
    super(context);
    configure();
  }

  @Override
  public void configure() throws ConfigurationException {
    try {
      port = Integer.parseInt(context.getString("port"));
    } catch (NumberFormatException e) {
      throw new ConfigurationException(
          "Port number is invalid for AvroSource!", e);
    }
    bind = context.getString("bind");
    if (bind == null || bind.isEmpty()) {
      throw new ConfigurationException("BindAddress cannot be empty");

    }
  }

}
