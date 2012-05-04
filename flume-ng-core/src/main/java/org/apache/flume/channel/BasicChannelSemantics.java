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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.TransactionListenerFactory;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * An implementation of basic {@link Channel} semantics, including the
 * implied thread-local semantics of the {@link Transaction} class,
 * which is required to extend {@link BasicTransactionSemantics}.
 * </p>
 */
public abstract class BasicChannelSemantics extends AbstractChannel {

  private ThreadLocal<BasicTransactionSemantics> currentTransaction
      = new ThreadLocal<BasicTransactionSemantics>();

  private Map<String, Set<TransactionListenerFactory>> headerFactoryMap =
      new HashMap<String, Set<TransactionListenerFactory>>();

  private Set<TransactionListenerFactory> callOnPut =
      new HashSet<TransactionListenerFactory>();
  private Set<TransactionListenerFactory> callOnTake =
      new HashSet<TransactionListenerFactory>();

  private static Logger LOGGER = LoggerFactory.getLogger(
      BasicChannelSemantics.class);

  private boolean initialized = false;


  /**
   * <p>
   * Called upon first getTransaction() request, while synchronized on
   * this {@link Channel} instance.  Use this method to delay the
   * initializization resources until just before the first
   * transaction begins.
   * </p>
   */
  protected void initialize() {}

  /**
   * <p>
   * Called to create new {@link Transaction} objects, which must
   * extend {@link BasicTransactionSemantics}.  Each object is used
   * for only one transaction, but is stored in a thread-local and
   * retrieved by <code>getTransaction</code> for the duration of that
   * transaction.
   * </p>
   */
  protected abstract BasicTransactionSemantics createTransaction();

  /**
   * <p>
   * Ensures that a transaction exists for this thread and then
   * delegates the <code>put</code> to the thread's {@link
   * BasicTransactionSemantics} instance.
   * </p>
   */
  @Override
  public void put(Event event) throws ChannelException {
    BasicTransactionSemantics transaction = currentTransaction.get();
    Preconditions.checkState(transaction != null,
        "No transaction exists for this thread");
    transaction.put(event);
  }

  /**
   * <p>
   * Ensures that a transaction exists for this thread and then
   * delegates the <code>take</code> to the thread's {@link
   * BasicTransactionSemantics} instance.
   * </p>
   */
  @Override
  public Event take() throws ChannelException {
    BasicTransactionSemantics transaction = currentTransaction.get();
    Preconditions.checkState(transaction != null,
        "No transaction exists for this thread");
    return transaction.take();
  }

  /**
   * <p>
   * Initializes the channel if it is not already, then checks to see
   * if there is an open transaction for this thread, creating a new
   * one via <code>createTransaction</code> if not.
   * @return the current <code>Transaction</code> object for the
   *     calling thread
   * </p>
   */
  @Override
  public Transaction getTransaction() {

    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          initialize();
          initialized = true;
        }
      }
    }

    BasicTransactionSemantics transaction = currentTransaction.get();
    if (transaction == null || transaction.getState().equals(
        BasicTransactionSemantics.State.CLOSED)) {
      transaction = createTransaction();
      transaction.setFactoriesToCallOnPut(callOnPut);
      transaction.setFactoriesToCallOnTake(callOnTake);
      transaction.setHeaderFactoryMap(headerFactoryMap);
      currentTransaction.set(transaction);
    }
    return transaction;
  }

  /**
   * Accepts basic configuration for the transaction listener and other
   * channel specific properties. The transaction listener properties
   * are specified with the <tt>txnlistener.</tt> and requires at least <tt>
   * type</tt> parameter, and a <tt>listenerheaders</tt>.
   * The <tt>listenerheaders</tt> parameter must have a space separated
   * list of event headers for which the transaction listener will be called.
   * Without these a transaction listener will not be created.
   * This accepts configuration parameters in the following way:
   *
   * <pre>
   * agent.channels.channel.txnlistener.type = fully.qualified.classname (or) alias from TransactionListenerType
   * agent.channels.channel.listenerheaders = space separated list of headers
   * agent.channels.channel.txnlistener.callat = put/take/both
   * </pre><p>
   * Example: <p>
   * <pre>
   * agent.channels.channel.txnlistener.type = org.apache.channel.memory.DummyTransactionListener
   * agent.channels.channel.txnlistener.callon = both
   * agent.channels.channel.listenerheaders = test1 test5
   * <pre> <p>
   *
   * <bold> All subclasses must call <tt>super.configure()</tt> in their
   * configure methods, for transaction listener support. If this method is not
   * called transaction listeners will not be instantiated or configured.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Context context) {
    int i = 0;
    String factoryNames = context.getString("listenerfactories");
    if(factoryNames != null && !factoryNames.isEmpty()){
      String[] factories = factoryNames.split("\\s+");
      for(String currentFactory:factories){
        Map<String, String> currentFactoryProps = context.getSubProperties(
            "listenerfactories"+"."+currentFactory+".");
        String factoryType = currentFactoryProps.get("type").trim();
        Context currentFactoryContext = new Context();
        currentFactoryContext.putAll(currentFactoryProps);
        TransactionListenerFactory instance = null;
        if(factoryType != null && !factoryType.isEmpty()){
          try {
            Class<? extends TransactionListenerFactory> factory =
                (Class<? extends TransactionListenerFactory>)
                Class.forName(factoryType);
            instance = factory.newInstance();
            instance.setName(currentFactory);
            instance.initialize(currentFactoryContext);
          } catch (Exception e) {
            throw new ConfigurationException("Could not create Transaction" +
                "Listener Factory. Exception follows.", e);
          }
          String currentHeaders = currentFactoryContext.getString("headers");
          if(currentHeaders != null && !currentHeaders.isEmpty()){
            String[] headers = currentHeaders.split("\\s+");
            for(String header:headers){
              Set<TransactionListenerFactory> factoriesToCall = null;
              if(this.headerFactoryMap.containsKey(header)){
                factoriesToCall = headerFactoryMap.get(header);
                System.out.println(" Found in header factory map");
              } else {
                factoriesToCall = new HashSet<TransactionListenerFactory>();
                headerFactoryMap.put(header, factoriesToCall);
              }
              factoriesToCall.add(instance);
            }
          }
        }
        String when = currentFactoryContext.getString("callon");
        if(when != null && when.isEmpty()) {
          if(when.equals("put")){
            callOnPut.add(instance);
          } else {
            callOnTake.add(instance);
          }
        } else {
          callOnTake.add(instance);
        }
      }
    }
    System.out.println("hdrfactory map: " + i++ +" " + headerFactoryMap);

  }
}