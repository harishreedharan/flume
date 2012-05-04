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
package org.apache.flume.channel;


import java.io.BufferedWriter;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.TransactionListener;

/**
 * A basic implementation of BasicTransactionListener, simply increments
 * counters every time an event occurs
 *
 */
public class DummyTransactionListener implements TransactionListener {

  BufferedWriter writer;
  public static String COMMITTED = "committed";
  public static String PUT = "put";
  public static String TOOK = "took";
  public static CounterGroup counterGroup = new CounterGroup();
  private String factoryName;

  public DummyTransactionListener(String factory){
    this.factoryName = factory;
  }

  public static void resetCounters(){
    counterGroup = new CounterGroup();
  }
  @Override
  public void onTransactionCommit() {
    System.out.println(factoryName + "-" + COMMITTED);
    counterGroup.incrementAndGet(factoryName + "-" + COMMITTED);
  }

  @Override
  public void onPut(Map<String, String> eventHeaders) {
    System.out.println(factoryName + "-" + PUT);

    counterGroup.incrementAndGet(factoryName + "-" + PUT);
  }


  @Override
  public void onTake(Map<String, String> eventHeaders) {
    System.out.println(factoryName + "-" + TOOK);

    counterGroup.incrementAndGet(factoryName + "-" + TOOK);
  }

  @Override
  public void configure(Context context) {
    // TODO Auto-generated method stub

  }


}
