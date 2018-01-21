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
package org.apache.qpid.systest.destination;

import static org.apache.qpid.systest.core.util.Utils.sendMessages;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.CustomJMSXProperty;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;

/**
 * From the API Docs getJMSDestination:
 *
 * When a message is received, its JMSDestination value must be equivalent to
 * the value assigned when it was sent.
 */
public class JMSDestinationTest extends JmsTestBase
{
    private Connection _connection;
    private Session _session;

    @Before
    public void setUp() throws Exception
    {
        _connection = getConnection();

        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception
    {
        if (_connection != null)
        {
            _connection.close();
        }
    }

    /**
     * Test a message received without the JMS_QPID_DESTTYPE can be resent
     * and correctly have the property set.
     *
     * To do this we need to create a 0-10 connection and send a message
     * which is then received by a 0-8/9 client.
     */
    @Test
    public void testQpidDestTypeHandling() throws Exception
    {
        assumeThat("c++ broker doesn't implement 0-8",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

        final String originalAmqpVersion = System.getProperty(ClientProperties.AMQP_VERSION);

        System.setProperty(ClientProperties.AMQP_VERSION, "0-10");
        try
        {
            Connection connection010 = getConnection();

            Session session010 = connection010.createSession(true, Session.SESSION_TRANSACTED);

            // Create queue for testing
            Queue queue = session010.createQueue(getTestQueueName());

            // Ensure queue exists
            session010.createConsumer(queue).close();

            sendMessages(session010, queue, 1);

            // Close the 010 connection
            connection010.close();

            // Create a 0-8 Connection and receive message
            System.setProperty(ClientProperties.AMQP_VERSION, "0-8");

            Connection connection08 = getConnection();

            Session session08 = connection08.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer = session08.createConsumer(queue);

            connection08.start();

            Message message = consumer.receive(getReceiveTimeout());

            assertNotNull("Didn't receive 0-10 message.", message);

            // Validate that JMS_QPID_DESTTYPE is not set
            try
            {
                message.getIntProperty(CustomJMSXProperty.JMS_QPID_DESTTYPE.toString());
                fail("JMS_QPID_DESTTYPE should not be set, so should throw NumberFormatException");
            }
            catch (NumberFormatException nfe)
            {
                // PASS
            }

            // Resend message back to queue and validate that
            // a) getJMSDestination works without the JMS_QPID_DESTTYPE
            // b) we can actually send without a BufferOverFlow.

            MessageProducer producer = session08.createProducer(queue);
            producer.send(message);

            message = consumer.receive(getReceiveTimeout());

            assertNotNull("Didn't receive recent 0-8 message.", message);

            // Validate that JMS_QPID_DESTTYPE is not set
            assertEquals("JMS_QPID_DESTTYPE should be set to a Queue", AMQDestination.QUEUE_TYPE,
                         message.getIntProperty(CustomJMSXProperty.JMS_QPID_DESTTYPE.toString()));
        }
        finally
        {
            if (originalAmqpVersion == null)
            {
                System.clearProperty(ClientProperties.AMQP_VERSION);
            }
            else
            {
                System.setProperty(ClientProperties.AMQP_VERSION, originalAmqpVersion);
            }
        }
    }

    @Test
    public void testQueueWithBindingUrlUsingCustomExchange() throws Exception
    {
        String exchangeName = "exch_" + getTestQueueName();
        String queueName = "queue_" + getTestQueueName();
        
        String address = String.format("direct://%s/%s/%s?routingkey='%s'", exchangeName, queueName, queueName, queueName);
        sendReceive(address);
    }

    @Test
    public void testQueueWithBindingUrlUsingAmqDirectExchange() throws Exception
    {
        String queueName = getTestQueueName();
        String address = String.format("direct://amq.direct/%s/%s?routingkey='%s'", queueName, queueName, queueName);
        sendReceive(address);
    }

    @Test
    public void testQueueWithBindingUrlUsingDefaultExchange() throws Exception
    {
        String queueName = getTestQueueName();
        String address = String.format("direct:///%s/%s?routingkey='%s'", queueName, queueName, queueName);
        sendReceive(address);
    }

    private void sendReceive(String address) throws Exception
    {
        Destination dest = _session.createQueue(address);
        MessageConsumer consumer = _session.createConsumer(dest);

        _connection.start();

        sendMessages(_session, dest, 1);

        Message receivedMessage = consumer.receive(getReceiveTimeout());

        assertNotNull("Message should not be null", receivedMessage);

        Destination receivedDestination = receivedMessage.getJMSDestination();

        assertNotNull("JMSDestination should not be null", receivedDestination);
        assertEquals("JMSDestination should match that sent", address, receivedDestination.toString());
    }
}
