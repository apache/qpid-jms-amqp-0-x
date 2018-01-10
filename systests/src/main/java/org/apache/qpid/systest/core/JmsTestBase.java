/*
 *
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assume.assumeThat;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.naming.NamingException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class JmsTestBase extends BrokerAdminUsingTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JmsTestBase.class);

    @Rule
    public final TestName _testName = new TestName();

    @Before
    public void setUpTestBase()
    {
        assumeThat(String.format("BrokerAdmin is not available. Skipping the test %s#%s",
                                 getClass().getName(),
                                 _testName.getMethodName()),
                   getBrokerAdmin(), is(notNullValue()));
        LOGGER.debug("Test receive timeout is {} milliseconds", getReceiveTimeout());
    }


    protected Connection getConnection() throws JMSException, NamingException
    {
        assumeThat(String.format("BrokerAdmin is not available. Skipping the test %s#%s",
                                 getClass().getName(),
                                 _testName.getMethodName()),
                   getBrokerAdmin(), is(notNullValue()));

        return getBrokerAdmin().getConnection();
    }

    protected static long getReceiveTimeout()
    {
        return Long.getLong("qpid.test_receive_timeout", 1000L);
    }

    protected String getTestName()
    {
        return _testName.getMethodName();
    }


    protected TopicConnection getTopicConnection() throws JMSException, NamingException
    {
        return (TopicConnection) getConnection();
    }

    protected QueueConnection getQueueConnection() throws JMSException, NamingException
    {
        return (QueueConnection) getConnection();
    }
}
