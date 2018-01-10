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

package org.apache.qpid.systest.core;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import javax.jms.Connection;
import javax.jms.JMSException;

import com.google.common.util.concurrent.ListenableFuture;

@SuppressWarnings("unused")
public interface BrokerAdmin
{
    void create(final Class testClass);
    void start(final Class testClass, final Method method);
    void stop(final Class testClass, final Method method);
    void destroy(final Class testClass);
    ListenableFuture<Void> restart();

    InetSocketAddress getBrokerAddress(PortType portType);
    boolean supportsPersistence();

    String getValidUsername();
    String getValidPassword();

    String getType();
    BrokerType getBrokerType();

    Connection getConnection() throws JMSException;

    enum PortType
    {
        ANONYMOUS_AMQP,
        AMQP
    }

    enum BrokerType
    {
        BROKERJ,
        CPP
    }
}
