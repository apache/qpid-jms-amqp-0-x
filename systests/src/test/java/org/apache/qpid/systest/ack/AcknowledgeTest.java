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
package org.apache.qpid.systest.ack;

import static org.apache.qpid.systest.core.util.Utils.INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.core.util.Utils;

public class AcknowledgeTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgeTest.class);

    private static final int SENT_COUNT = 4;

    private Connection _connection;
    private Session _consumerSession;
    private MessageConsumer _consumer;

    @Before
    public void setUp() throws Exception
    {
        initTest();

    }

    @After
    public void tearDown() throws Exception
    {
        if (_connection != null)
        {
            _connection.close();
        }
    }

    private void initTest() throws Exception
    {
        _connection = getConnection();

        _consumerSession = _connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = _consumerSession.createQueue(getTestQueueName());

        _consumer = _consumerSession.createConsumer(queue);

        LOGGER.info("Sending four messages");
        Utils.sendMessages(_connection.createSession(false, Session.AUTO_ACKNOWLEDGE), queue, SENT_COUNT);
        LOGGER.info("Starting connection");
        _connection.start();
    }

    private Message validateNextMessages(int nextCount, int startIndex) throws JMSException
    {
        Message message = null;

        for (int index = 0; index < nextCount; index++)
        {
            message = _consumer.receive(getReceiveTimeout());
            assertEquals(startIndex + index, message.getIntProperty(INDEX));
        }
        return message;
    }

    private void validateRemainingMessages(int remaining) throws JMSException
    {
        int index = SENT_COUNT - remaining;

        Message message = null;
        while (index != SENT_COUNT)
        {
            message =  _consumer.receive(getReceiveTimeout());
            assertNotNull(message);
            int expected = index++;
            assertEquals("Message has unexpected index", expected, message.getIntProperty(INDEX));
        }

        if (message != null)
        {
            LOGGER.info("Received redelivery of three messages. Acknowledging last message");
            message.acknowledge();
        }

        LOGGER.info("Calling acknowledge with no outstanding messages");
        // all acked so no messages to be delivered
        _consumerSession.recover();

        message = _consumer.receiveNoWait();
        assertNull(message);
        LOGGER.info("No messages redelivered as is expected");
    }

    @Test
    public void testAckThis() throws Exception
    {
        Message message = validateNextMessages(2, 0);
        message.acknowledge();
        LOGGER.info("Received 2 messages, acknowledge() first message, should acknowledge both");

        _consumer.receive();
        _consumer.receive();
        LOGGER.info("Received all four messages. Calling recover with two outstanding messages");
        _consumerSession.recover();

        Message message2 = _consumer.receive(getReceiveTimeout());
        assertNotNull(message2);
        assertEquals(2, message2.getIntProperty(INDEX));

        Message message3 = _consumer.receive(getReceiveTimeout());
        assertNotNull(message3);
        assertEquals(3, message3.getIntProperty(INDEX));

        LOGGER.info("Received redelivery of two messages. calling acknolwedgeThis() first of those message");
        ((org.apache.qpid.jms.Message) message2).acknowledgeThis();

        LOGGER.info("Calling recover");
        _consumerSession.recover();

        message3 = _consumer.receive(getReceiveTimeout());
        assertNotNull(message3);
        assertEquals(3, message3.getIntProperty(INDEX));
        ((org.apache.qpid.jms.Message) message3).acknowledgeThis();

        // all acked so no messages to be delivered
        validateRemainingMessages(0);
    }
}
