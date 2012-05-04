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
package org.apache.flume;

import java.util.Map;

import org.apache.flume.conf.Configurable;
/**
 * Interface for transaction listener. A listener's onPut
 * will be called whenever a put happens in the transaction, and onTake whenever
 * a take happens. If at least a put/take has happened then the
 * onTransactionCommit or onTransactionRollback is called at the end of the
 * transaction based on if the transaction was committed or rolled back.
 *
 */
public interface TransactionListener extends Configurable {

  public void onTransactionCommit();

  public void onTransactionRollback();
  /**
   * This will be called with a Map that does not support modification.
   * Any attempt to modify it will cause an Exception to be thrown.
   * @param eventHeaders - the headers of the event that caused this listener
   * to be called. The listener will be called exactly once, even if more
   * than one header matches.
   */
  public void onPut(Map<String, String> eventHeaders);

  public void onTake(Map<String, String> eventHeaders);

}
