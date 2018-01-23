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
package org.apache.qpid.systest.ack;

import static org.apache.qpid.systest.core.util.Utils.INDEX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.core.util.Utils;

public class MaxDeliveryTest extends JmsTestBase
{
    private static final int MAX_DELIVERY_ATTEMPTS = 2;
    private static final String DLQ_ADDRESS =
            "ADDR:%s; {create: always, node : {type : queue, x-bindings: [{ exchange: 'amq.fanout', key: %s }]}}";
    private static final String QUEUEU_ADDRESS = "ADDR:%s; {create: always, node : {type : queue,"
                                        + " x-declare:{alternate-exchange:'amq.fanout',"
                                        + " arguments:{x-qpid-maximum-delivery-count: %d}},"
                                        + " x-bindings:[{ exchange:'amq.direct',key:'%s'}]}}";

    @Before
    public void setUp()
    {
        assumeThat("Test suite tests Broker-j specific feature",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

    }

    @Test
    public void maximumDelivery() throws Exception
    {
        String queueName = getTestName();
        String dlqName = getTestName() + "_DLQ";

        int numberOfMessages = 5;
        Connection connection = getTestConnection();
        try
        {
            final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            final Destination dlqDestination = session.createQueue(String.format(DLQ_ADDRESS, dlqName, dlqName));
            final Destination queue =
                    session.createQueue(String.format(QUEUEU_ADDRESS, queueName, MAX_DELIVERY_ATTEMPTS, queueName));
            session.createConsumer(dlqDestination).close();

            final MessageConsumer consumer = session.createConsumer(queue);

            Utils.sendMessages(connection, queue, numberOfMessages);

            connection.start();

            int expectedMessageIndex = 0;
            int deliveryAttempt = 0;
            int deliveryCounter = 0;
            do
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message '%d' was not received in delivery attempt %d",
                                            expectedMessageIndex,
                                            deliveryAttempt), message);
                int index = message.getIntProperty(INDEX);
                assertEquals(String.format("Unexpected message index (delivery attempt %d)", deliveryAttempt),
                             expectedMessageIndex,
                             index);

                deliveryCounter++;

                // dlq all even messages
                if (index % 2 == 0)
                {
                    session.recover();
                    if (deliveryAttempt < MAX_DELIVERY_ATTEMPTS - 1)
                    {
                        deliveryAttempt++;
                    }
                    else
                    {
                        deliveryAttempt = 0;
                        expectedMessageIndex++;
                    }
                }
                else
                {
                    message.acknowledge();
                    deliveryAttempt = 0;
                    expectedMessageIndex++;
                }
            }
            while (expectedMessageIndex != numberOfMessages);

            int numberOfEvenMessages = numberOfMessages / 2 + 1;
            assertEquals("Unexpected total delivery counter",
                         numberOfEvenMessages * MAX_DELIVERY_ATTEMPTS + (numberOfMessages - numberOfEvenMessages),
                         deliveryCounter);

            verifyDeadLetterQueueMessages(connection, dlqName, numberOfEvenMessages);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void maximumDeliveryWithinMessageListenerAndClientAcknowledge() throws Exception
    {
        maximumDeliveryWithinMessageListener(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void maximumDeliveryWithinMessageListenerAndAutoAcknowledge() throws Exception
    {
        maximumDeliveryWithinMessageListener(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void maximumDeliveryWithinMessageListenerAndDupsOkAcknowledge() throws Exception
    {
        maximumDeliveryWithinMessageListener(Session.DUPS_OK_ACKNOWLEDGE);
    }

    private void maximumDeliveryWithinMessageListener(final int acknowledgeMode) throws Exception
    {
        String queueName = getTestName();
        String dlqName = getTestName() + "_DLQ";

        int numberOfMessages = 5;
        Connection connection = getTestConnection();
        try
        {
            final Session session = connection.createSession(false, acknowledgeMode);

            final Destination dlqDestination = session.createQueue(String.format(DLQ_ADDRESS, dlqName, dlqName));
            final Destination queue =
                    session.createQueue(String.format(QUEUEU_ADDRESS, queueName, MAX_DELIVERY_ATTEMPTS, queueName));
            session.createConsumer(dlqDestination).close();

            final MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(connection, queue, numberOfMessages);

            connection.start();

            int numberOfEvenMessages = numberOfMessages / 2 + 1;
            int expectedNumberOfDeliveries =
                    numberOfEvenMessages * MAX_DELIVERY_ATTEMPTS + (numberOfMessages - numberOfEvenMessages);
            final CountDownLatch deliveryLatch = new CountDownLatch(expectedNumberOfDeliveries);
            final AtomicReference<Throwable> messageListenerThrowable = new AtomicReference<>();
            final AtomicInteger deliveryAttempt = new AtomicInteger();
            final AtomicInteger expectedMessageIndex = new AtomicInteger();
            consumer.setMessageListener(new MessageListener()
            {

                @Override
                public void onMessage(final Message message)
                {

                    try
                    {
                        int index = message.getIntProperty(INDEX);
                        assertEquals(String.format("Unexpected message index (delivery attempt %d)",
                                                   deliveryAttempt.get()),
                                     expectedMessageIndex.get(),
                                     index);

                        // dlq all even messages
                        if (index % 2 == 0)
                        {
                            session.recover();
                            if (deliveryAttempt.get() < MAX_DELIVERY_ATTEMPTS - 1)
                            {
                                deliveryAttempt.incrementAndGet();
                            }
                            else
                            {
                                deliveryAttempt.set(0);
                                expectedMessageIndex.incrementAndGet();
                            }
                        }
                        else
                        {
                            message.acknowledge();
                            deliveryAttempt.set(0);
                            expectedMessageIndex.incrementAndGet();
                        }
                    }
                    catch (Throwable t)
                    {
                        messageListenerThrowable.set(t);
                    }
                    finally
                    {
                        deliveryLatch.countDown();
                    }
                }
            });

            assertTrue("Messages were not received in timely manner",
                       deliveryLatch.await(expectedNumberOfDeliveries * getReceiveTimeout(), TimeUnit.MILLISECONDS));
            assertNull("Unexpected throwable in MessageListener", messageListenerThrowable.get());

            verifyDeadLetterQueueMessages(connection, dlqName, numberOfEvenMessages);
        }
        finally
        {
            connection.close();
        }
    }

    private void verifyDeadLetterQueueMessages(final Connection connection,
                                               final String dlqName,
                                               final int numberOfEvenMessages) throws Exception
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(dlqName);

        assertEquals("Unexpected number of total messages",
                     numberOfEvenMessages,
                     ((AMQSession<?, ?>) session).getQueueDepth((AMQDestination) queue));

        MessageConsumer consumer = session.createConsumer(queue);

        for (int i = 0; i < numberOfEvenMessages; i++)
        {
            Message message = consumer.receive(getReceiveTimeout());
            assertEquals("Unexpected DQL message index", i * 2, message.getIntProperty(INDEX));
        }
    }

    private Connection getTestConnection() throws JMSException
    {
        Map<String, String> options = new HashMap<>();
        options.put("rejectbehaviour", "server");
        options.put("maxprefetch", "0");
        return getConnection(options);
    }
}
