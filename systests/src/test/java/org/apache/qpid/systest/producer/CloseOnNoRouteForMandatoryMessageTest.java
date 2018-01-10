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
 */
package org.apache.qpid.systest.producer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.producer.noroute.UnroutableMessageTestExceptionListener;

public class CloseOnNoRouteForMandatoryMessageTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CloseOnNoRouteForMandatoryMessageTest.class);

    private UnroutableMessageTestExceptionListener _testExceptionListener;

    @Before
    public void setUp()
    {
        _testExceptionListener = new UnroutableMessageTestExceptionListener();

        assumeThat("Feature 'close on no route' is only implemented Broker-J",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

        assumeThat("Feature 'close on no route' is only implemented for AMQP 0-8..0-9-1",
                   getProtocol(),
                   is(not(equalTo("0-10"))));
    }

    @Test
    public void testNoRoute_brokerClosesConnection() throws Exception
    {
        Connection connection = createConnectionWithCloseWhenNoRoute(true);
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            String testQueueName = getTestQueueName();
            MessageProducer mandatoryProducer = ((AMQSession<?, ?>) transactedSession).createProducer(
                    transactedSession.createQueue(testQueueName),
                    true, // mandatory
                    false); // immediate

            Message message = transactedSession.createMessage();
            mandatoryProducer.send(message);

            _testExceptionListener.assertReceivedNoRoute(testQueueName);

            try
            {
                transactedSession.commit();
                fail("Expected exception not thrown");
            }
            catch (IllegalStateException ise)
            {
                LOGGER.debug("Caught exception", ise);
                //The session was marked closed even before we had a chance to call commit on it
                assertTrue("ISE did not indicate closure", ise.getMessage().contains("closed"));
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testNoRouteForNonMandatoryMessage_brokerKeepsConnectionOpenAndCallsExceptionListener() throws Exception
    {
        Connection connection = createConnectionWithCloseWhenNoRoute(true);
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            String testQueueName = getTestQueueName();
            MessageProducer nonMandatoryProducer = ((AMQSession<?, ?>) transactedSession).createProducer(
                    transactedSession.createQueue(testQueueName),
                    false, // mandatory
                    false); // immediate

            Message message = transactedSession.createMessage();
            nonMandatoryProducer.send(message);

            // should succeed - the message is simply discarded
            transactedSession.commit();

            _testExceptionListener.assertNoException();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testNoRouteOnNonTransactionalSession_brokerKeepsConnectionOpenAndCallsExceptionListener() throws Exception
    {
        Connection connection = createConnectionWithCloseWhenNoRoute(true);
        try
        {
            Session nonTransactedSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String testQueueName = getTestQueueName();
            MessageProducer mandatoryProducer = ((AMQSession<?, ?>) nonTransactedSession).createProducer(
                    nonTransactedSession.createQueue(testQueueName),
                    true, // mandatory
                    false); // immediate

            Message message = nonTransactedSession.createMessage();
            mandatoryProducer.send(message);

            _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, testQueueName);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClientDisablesCloseOnNoRoute_brokerKeepsConnectionOpenAndCallsExceptionListener() throws Exception
    {
        Connection connection = createConnectionWithCloseWhenNoRoute(false);
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            String testQueueName = getTestQueueName();
            MessageProducer mandatoryProducer = ((AMQSession<?, ?>) transactedSession).createProducer(
                    transactedSession.createQueue(testQueueName),
                    true, // mandatory
                    false); // immediate

            Message message = transactedSession.createMessage();
            mandatoryProducer.send(message);
            transactedSession.commit();
            _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, testQueueName);
        }
        finally
        {
            connection.close();
        }
    }

    private Connection createConnectionWithCloseWhenNoRoute(boolean closeWhenNoRoute) throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put(ConnectionURL.OPTIONS_CLOSE_WHEN_NO_ROUTE, Boolean.toString(closeWhenNoRoute));
        Connection connection = getConnection(options);
        connection.setExceptionListener(_testExceptionListener);
        return connection;
    }


}
