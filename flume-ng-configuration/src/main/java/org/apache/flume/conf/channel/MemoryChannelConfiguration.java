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

package org.apache.flume.conf.channel;

import org.apache.flume.conf.Context;
import org.apache.flume.conf.AbstractComponentConfiguration;

public class MemoryChannelConfiguration extends AbstractComponentConfiguration {
  private static final Integer defaultCapacity = 100;
  private static final Integer defaultTransCapacity = 100;
  private static final Integer defaultKeepAlive = 3;
  private int capacity;
  private int transactionCapacity;
  private int keepAlive;

  public MemoryChannelConfiguration(Context context) {
    super(context);
    configure();

  }

  @Override
  public void configure() {
    String strCapacity = context.getString("capacity");
    capacity = 0;
    transactionCapacity = 0;
    keepAlive = 0;
    if (strCapacity == null) {
      capacity = defaultCapacity;
    } else {
      try {
        capacity = Integer.parseInt(strCapacity);
      } catch (NumberFormatException e) {
        capacity = defaultCapacity;
      }
    }

    String strTransCapacity = context.getString("transactionCapacity");
    if (strTransCapacity == null) {
      transactionCapacity = defaultTransCapacity;
    } else {
      try {
        transactionCapacity = Integer.parseInt(strTransCapacity);
      } catch (NumberFormatException e) {
        transactionCapacity = defaultTransCapacity;
      }
    }
    if (transactionCapacity > capacity)
      this.transactionCapacity = capacity;

    String strKeepAlive = context.getString("keep-alive");
    if (strKeepAlive == null) {
      keepAlive = defaultKeepAlive;
    } else {
      keepAlive = Integer.parseInt(strKeepAlive);
    }

  }
}
