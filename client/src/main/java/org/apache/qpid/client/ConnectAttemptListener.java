/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.qpid.client;

import java.net.URI;
import java.util.function.BiFunction;

import javax.jms.JMSException;

/**
 * An implementation of ConnectAttemptListener can be set on a concrete implementation of {@link AbstractConnectionFactory}
 * in order to notify messaging application about every successful and unsuccessful connectivity attempt.
 *
 * The {@link #connectAttemptFailed(URI, JMSException)} can be used as a trigger to rotate expired credentials, if those
 * are set via extension mechanism {@link AbstractConnectionFactory#setExtension(String, BiFunction)}.
 */
public interface ConnectAttemptListener
{
    /**
     * Invoked when connect attempt to the given broker URI failed with a given exception.
     * This method can be used to rotate the credentials and re-attempt the connection to the broker with new
     * credentials which can be set using extension mechanism {@link AbstractConnectionFactory#setExtension(String, BiFunction)}.
     *
     * The method can return true, if connection attempt needs to be repeated to the same broker immediately
     * and without incrementing a failover re-try counter.
     * Otherwise, the connection would be attempted as per failover settings.
     *
     * @param brokerURI target broker URI
     * @param e         exception thrown on connect attempt
     * @return true if connect attempt to the given broker URI needs to be repeated again
     */
    boolean connectAttemptFailed(URI brokerURI, JMSException e);

    /**
     * Invoked when connection is established successfully to the broker with a given URI
     *
     * @param brokerURI target broker URI
     */
    void connectAttemptSucceeded(URI brokerURI);
}
