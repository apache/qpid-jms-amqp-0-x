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
import static org.junit.Assume.assumeThat;

import java.util.Collections;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.client.AMQSession;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.producer.noroute.UnroutableMessageTestExceptionListener;

public class ImmediateAndMandatoryPublishingTest extends JmsTestBase
{
    private UnroutableMessageTestExceptionListener _testExceptionListener;
    private Connection _connection;

    @Before
    public void setUp() throws JMSException
    {
        assumeThat("AMQP 0-8..0-9-1 specific behaviour",
                   getProtocol(),
                   is(not(equalTo("0-10"))));

        _testExceptionListener = new UnroutableMessageTestExceptionListener();
        _connection = getConnection(Collections.singletonMap(ConnectionURL.OPTIONS_CLOSE_WHEN_NO_ROUTE, Boolean.toString(false)));
        _connection.setExceptionListener(_testExceptionListener);
    }

    @After
    public void tearDown() throws JMSException
    {
        if (_connection != null)
        {
            _connection.close();
        }
    }

    @Test
    public void testPublishP2PWithNoConsumerAndImmediateOnAndAutoAck() throws Exception
    {
        publishIntoExistingDestinationWithNoConsumerAndImmediateOn(Session.AUTO_ACKNOWLEDGE, false);
    }

    @Test
    public void testPublishP2PWithNoConsumerAndImmediateOnAndTx() throws Exception
    {
        publishIntoExistingDestinationWithNoConsumerAndImmediateOn(Session.SESSION_TRANSACTED, false);
    }

    @Test
    public void testPublishPubSubWithDisconnectedDurableSubscriberAndImmediateOnAndAutoAck() throws Exception
    {
        publishIntoExistingDestinationWithNoConsumerAndImmediateOn(Session.AUTO_ACKNOWLEDGE, true);
    }

    @Test
    public void testPublishPubSubWithDisconnectedDurableSubscriberAndImmediateOnAndTx() throws Exception
    {
        publishIntoExistingDestinationWithNoConsumerAndImmediateOn(Session.SESSION_TRANSACTED, true);
    }

    @Test
    public void testPublishP2PIntoNonExistingDesitinationWithMandatoryOnAutoAck() throws Exception
    {
        publishWithMandatoryOnImmediateOff(Session.AUTO_ACKNOWLEDGE, false);
    }

    @Test
    public void testPublishP2PIntoNonExistingDesitinationWithMandatoryOnAndTx() throws Exception
    {
        publishWithMandatoryOnImmediateOff(Session.SESSION_TRANSACTED, false);
    }

    @Test
    public void testPubSubMandatoryAutoAck() throws Exception
    {
        publishWithMandatoryOnImmediateOff(Session.AUTO_ACKNOWLEDGE, true);
    }

    @Test
    public void testPubSubMandatoryTx() throws Exception
    {
        publishWithMandatoryOnImmediateOff(Session.SESSION_TRANSACTED, true);
    }

    @Test
    public void testP2PNoMandatoryAutoAck() throws Exception
    {
        publishWithMandatoryOffImmediateOff(Session.AUTO_ACKNOWLEDGE, false);
    }

    @Test
    public void testP2PNoMandatoryTx() throws Exception
    {
        publishWithMandatoryOffImmediateOff(Session.SESSION_TRANSACTED, false);
    }

    @Test
    public void testPubSubWithImmediateOnAndAutoAck() throws Exception
    {
        consumerCreateAndClose(true, false);

        Message message = produceMessage(Session.AUTO_ACKNOWLEDGE, true, false, true);
        _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, getTestQueueName());
    }

    private void publishIntoExistingDestinationWithNoConsumerAndImmediateOn(int acknowledgeMode, boolean pubSub)
            throws Exception
    {
        consumerCreateAndClose(pubSub, true);

        Message message = produceMessage(acknowledgeMode, pubSub, false, true);

        _testExceptionListener.assertReceivedNoConsumersWithReturnedMessage(message);
    }

    private void publishWithMandatoryOnImmediateOff(int acknowledgeMode, boolean pubSub) throws Exception
    {
        Message message = produceMessage(acknowledgeMode, pubSub, true, false);
        _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, getTestQueueName());
    }

    private void publishWithMandatoryOffImmediateOff(int acknowledgeMode, boolean pubSub) throws Exception
    {
        produceMessage(acknowledgeMode, pubSub, false, false);

        _testExceptionListener.assertNoException();
    }

    private void consumerCreateAndClose(boolean pubSub, boolean durable) throws JMSException
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination;
        MessageConsumer consumer;
        if (pubSub)
        {
            destination = session.createTopic(getTestQueueName());
            if (durable)
            {
                consumer = session.createDurableSubscriber((Topic) destination, getTestName());
            }
            else
            {
                consumer = session.createConsumer(destination);
            }
        }
        else
        {
            destination = session.createQueue(getTestQueueName());
            consumer = session.createConsumer(destination);
        }
        consumer.close();
    }

    private Message produceMessage(int acknowledgeMode, boolean pubSub, boolean mandatory, boolean immediate)
            throws JMSException
    {
        Session session = _connection.createSession(acknowledgeMode == Session.SESSION_TRANSACTED, acknowledgeMode);
        Destination destination;
        if (pubSub)
        {
            destination = session.createTopic(getTestQueueName());
        }
        else
        {
            destination = session.createQueue(getTestQueueName());
        }

        MessageProducer producer = ((AMQSession<?, ?>) session).createProducer(destination, mandatory, immediate);
        Message message = session.createMessage();
        producer.send(message);
        if (session.getTransacted())
        {
            session.commit();
        }
        return message;
    }

    public void testMandatoryAndImmediateDefaults() throws Exception
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // publish to non-existent queue - should get mandatory failure
        MessageProducer producer = session.createProducer(session.createQueue(getTestQueueName()));
        Message message = session.createMessage();
        producer.send(message);

        _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, getTestQueueName());

        producer = session.createProducer(null);
        message = session.createMessage();
        producer.send(session.createQueue(getTestQueueName()), message);

        _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, getTestQueueName());

        // publish to non-existent topic - should get no failure
        producer = session.createProducer(session.createTopic(getTestQueueName()));
        message = session.createMessage();
        producer.send(message);

        _testExceptionListener.assertNoException();

        producer = session.createProducer(null);
        message = session.createMessage();
        producer.send(session.createTopic(getTestQueueName()), message);

        _testExceptionListener.assertNoException();

        session.close();
    }

    public void testMandatoryAndImmediateSystemProperties() throws Exception
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        System.setProperty("qpid.default_mandatory", "true");
        try
        {

            // publish to non-existent topic - should get mandatory failure

            MessageProducer producer = session.createProducer(session.createTopic(getTestQueueName()));
            Message message = session.createMessage();
            producer.send(message);

            _testExceptionListener.assertReceivedNoRouteWithReturnedMessage(message, getTestQueueName());
        }
        finally
        {
            System.clearProperty("qpid.default_mandatory");
        }

        // now set topic specific system property to false - should no longer get mandatory failure on new producer
        System.setProperty("qpid.default_mandatory_topic", "false");
        try
        {
            MessageProducer producer = session.createProducer(null);
            Message message = session.createMessage();
            producer.send(session.createTopic(getTestQueueName()), message);

            _testExceptionListener.assertNoException();
        }
        finally
        {
            System.clearProperty("qpid.default_mandatory");
        }
    }
}
