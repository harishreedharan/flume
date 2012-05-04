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
package org.apache.flume;


public interface TransactionListenerFactory extends NamedComponent{

  /**
   * Initialize the transaction listener factory using the context
   * supplied to this factory via the configuration file. This method
   * initializes all state which should be supplied to all transaction
   * listeners created using this factory.
   * @param context
   */
  public void initialize(Context context);

  /**
   * Return a Transaction Listener which is already configured with any
   * configuration information it needs from the factory itself. This method
   * does not keep a copy of the transaction listener it is returning, to
   * allow it to get garbage collected, and can be used as a thread local.
   * The transaction listener is meant to be discarded after the transaction
   * is completed.
   * <strong> This method is thread safe, and multiple threads should be
   * able to call this method at the same time.</strong>
   * @return an object of a subclass of BasicTransactionListener.
   */
  public TransactionListener createTransactionListener();

  /**
   * Close the factory. This should does any clean up, if required. This method
   * is thread safe and safe to call over and over.
   */
  public void close();

}
