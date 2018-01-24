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
package org.apache.qpid.systest.message;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.core.brokerj.AmqpManagementFacade;
import org.apache.qpid.url.URLSyntaxException;

public class MessageCompressionTest extends JmsTestBase
{
    private static final int MIN_MESSAGE_PAYLOAD_SIZE = 2048 * 1024;
    private AmqpManagementFacade _managementFacade;

    @Before
    public void setUp()
    {
        assumeThat("Test suite requires amqp management",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));
        _managementFacade = new AmqpManagementFacade();
    }

    @Test
    public void testSenderCompressesReceiverUncompresses() throws Exception
    {
        doTestCompression(true, true, true);
    }

    @Test
    public void testSenderCompressesOnly() throws Exception
    {
        doTestCompression(true, false, true);
    }

    @Test
    public void testReceiverUncompressesOnly() throws Exception
    {
        doTestCompression(false, true, true);
    }

    @Test
    public void testNoCompression() throws Exception
    {
        doTestCompression(false, false, true);
    }

    @Test
    public void testDisablingCompressionAtBroker() throws Exception
    {
        enableMessageCompression(false);
        try
        {
            doTestCompression(true, true, false);
        }
        finally
        {
            enableMessageCompression(true);
        }
    }


    private void doTestCompression(final boolean senderCompresses,
                                   final boolean receiverUncompresses,
                                   final boolean brokerCompressionEnabled) throws Exception
    {

        String messageText = createMessageText();
        Connection senderConnection = getConnection(senderCompresses);
        String queueName = getTestName();
        Queue testQueue;
        try
        {
            Session senderSession = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            testQueue = createTestQueue(senderSession, queueName);

            publishMessage(senderConnection, messageText, testQueue);

            Map<String, Object> statistics = getVirtualHostStatistics("bytesIn");
            int bytesIn = ((Number) statistics.get("bytesIn")).intValue();

            if (senderCompresses && brokerCompressionEnabled)
            {
                assertTrue("Message was not sent compressed", bytesIn < messageText.length());
            }
            else
            {
                assertFalse("Message was incorrectly sent compressed", bytesIn < messageText.length());
            }
        }
        finally
        {
            senderConnection.close();
        }

        // receive the message
        Connection consumerConnection = getConnection(receiverUncompresses);
        try
        {
            Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(testQueue);
            consumerConnection.start();

            TextMessage message = (TextMessage) consumer.receive(getReceiveTimeout() * 2);
            assertNotNull("Message was not received", message);
            assertEquals("Message was corrupted", messageText, message.getText());
            assertEquals("Header was corrupted", "foo", message.getStringProperty("bar"));

            Map<String, Object> statistics = getVirtualHostStatistics("bytesOut");
            int bytesOut = ((Number) statistics.get("bytesOut")).intValue();

            if (receiverUncompresses && brokerCompressionEnabled)
            {
                assertTrue("Message was not received compressed", bytesOut < messageText.length());
            }
            else
            {
                assertFalse("Message was incorrectly received compressed", bytesOut < messageText.length());
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    private void publishMessage(final Connection senderConnection, final String messageText, final Queue testQueue)
            throws JMSException, org.apache.qpid.QpidException
    {
        Session session = senderConnection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = session.createProducer(testQueue);
        TextMessage sentMessage = session.createTextMessage(messageText);
        sentMessage.setStringProperty("bar", "foo");

        producer.send(sentMessage);
        session.commit();
    }

    private String createMessageText()
    {
        StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.length() < MIN_MESSAGE_PAYLOAD_SIZE)
        {
            stringBuilder.append("This should compress easily. ");
        }
        return stringBuilder.toString();
    }

    private Connection getConnection(final boolean compress) throws Exception
    {
        Map<String, String> options = new HashMap<>();
        options.put(ConnectionURL.OPTIONS_COMPRESS_MESSAGES, String.valueOf(compress));
        return getConnection(options);
    }

    private Queue createTestQueue(final Session session, final String queueName) throws JMSException
    {
        Queue queue = session.createQueue(queueName);
        session.createConsumer(queue).close();
        return queue;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getVirtualHostStatistics(final String... statisticsName)
            throws JMSException, NamingException
    {
        Object value = Arrays.asList(statisticsName);
        Map<String, Object> arguments = Collections.singletonMap("statistics", value);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Object statistics =
                    _managementFacade.performOperationUsingAmqpManagement(getBrokerAdmin().getVirtualHostName(),
                                                                          "org.apache.qpid.VirtualHost",
                                                                          "getStatistics",
                                                                          arguments,
                                                                          session);

            assertNotNull("Statistics is null", statistics);
            assertTrue("Statistics is not map", statistics instanceof Map);

            return (Map<String, Object>) statistics;
        }
        finally
        {
            connection.close();
        }
    }

    private void enableMessageCompression(final boolean value) throws Exception
    {
        Connection connection = getBrokerManagementConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            try
            {
                final Map<String, Object> attributes =
                        Collections.<String, Object>singletonMap("messageCompressionEnabled", value);
                _managementFacade.updateEntityUsingAmqpManagement("Broker",
                                                                  "org.apache.qpid.Broker",
                                                                  attributes,
                                                                  session);
            }
            finally
            {
                session.close();
            }
        }
        finally
        {
            connection.close();
        }
    }
}
