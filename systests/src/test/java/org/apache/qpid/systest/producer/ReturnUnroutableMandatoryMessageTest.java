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

package org.apache.qpid.systest.producer;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQHeadersExchange;
import org.apache.qpid.client.AMQNoRouteException;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.url.AMQBindingURL;
import org.apache.qpid.url.BindingURL;

public class ReturnUnroutableMandatoryMessageTest extends JmsTestBase implements ExceptionListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReturnUnroutableMandatoryMessageTest.class);

    private Message _bouncedMessage;
    private CountDownLatch _bounceMessageWaiter;

    @Before
    public void setUp() throws JMSException
    {
        assumeThat("AMQP 0-8..0-9-1 specific behaviour",
                   getProtocol(),
                   is(not(equalTo("0-10"))));
        _bounceMessageWaiter = new CountDownLatch(1);
    }

    @Test
    public void testReturnUnroutableMandatoryMessage_HEADERS() throws Exception
    {
        MessageConsumer consumer;
        AMQSession producerSession;
        AMQHeadersExchange queue;
        Connection con = getConnection();
        try
        {

            AMQSession consumerSession = (AMQSession) con.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            queue = new AMQHeadersExchange(new AMQBindingURL(ExchangeDefaults.HEADERS_EXCHANGE_CLASS
                                                             + "://"
                                                             + ExchangeDefaults.HEADERS_EXCHANGE_NAME
                                                             + "/test/queue1?"
                                                             + BindingURL.OPTION_ROUTING_KEY
                                                             + "='F0000=1'"));

            Map<String, Object> ft = new HashMap<>();
            ft.put("F1000", "1");
            consumerSession.declareAndBind(queue, ft);

            consumer = consumerSession.createConsumer(queue);

            Connection con2 = getConnection();
            try
            {
                con2.setExceptionListener(this);
                producerSession = (AMQSession) con2.createSession(false, Session.CLIENT_ACKNOWLEDGE);

                // Need to start the "producer" connection in order to receive bounced messages
                LOGGER.info("Starting producer connection");
                con2.start();
                try
                {
                    MessageProducer nonMandatoryProducer = producerSession.createProducer(queue, false, false);
                    MessageProducer mandatoryProducer = producerSession.createProducer(queue);

                    // First test - should neither be bounced nor routed
                    LOGGER.info("Sending non-routable non-mandatory message");
                    TextMessage msg1 = producerSession.createTextMessage("msg1");
                    nonMandatoryProducer.send(msg1);

                    // Second test - should be bounced
                    LOGGER.info("Sending non-routable mandatory message");
                    TextMessage msg2 = producerSession.createTextMessage("msg2");
                    mandatoryProducer.send(msg2);

                    // Third test - should be routed
                    LOGGER.info("Sending routable message");
                    TextMessage msg3 = producerSession.createTextMessage("msg3");
                    msg3.setStringProperty("F1000", "1");
                    mandatoryProducer.send(msg3);

                    LOGGER.info("Starting consumer connection");
                    con.start();
                    TextMessage tm = (TextMessage) consumer.receive(getReceiveTimeout());

                    assertTrue("No message routed to receiver", tm != null);
                    assertTrue("Wrong message routed to receiver: " + tm.getText(), "msg3".equals(tm.getText()));
                    assertTrue("Message is not bounced",
                               _bounceMessageWaiter.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
                    assertNotNull(_bouncedMessage);
                    Message m = _bouncedMessage;
                    assertTrue("Wrong message bounced: " + m.toString(), m.toString().contains("msg2"));
                }
                catch (JMSException jmse)
                {
                    // pass
                }
            }
            finally
            {
                con2.close();
            }
        }
        finally
        {
            con.close();
        }
    }

    @Test
    public void testReturnUnroutableMandatoryMessage_QUEUE() throws Exception
    {
        Connection con = getConnection();
        try
        {

            AMQSession consumerSession = (AMQSession) con.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            AMQQueue valid_queue =
                    new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_CLASS, "testReturnUnroutableMandatoryMessage_QUEUE");
            AMQQueue invalid_queue = new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_CLASS,
                                                  "testReturnUnroutableMandatoryMessage_QUEUE_INVALID");
            MessageConsumer consumer = consumerSession.createConsumer(valid_queue);

            Connection con2 = getConnection();
            try
            {
                con2.setExceptionListener(this);
                AMQSession producerSession = (AMQSession) con2.createSession(false, Session.CLIENT_ACKNOWLEDGE);

                // Need to start the "producer" connection in order to receive bounced messages
                LOGGER.info("Starting producer connection");
                con2.start();

                MessageProducer nonMandatoryProducer = producerSession.createProducer(valid_queue, false, false);
                MessageProducer mandatoryProducer = producerSession.createProducer(invalid_queue);

                // First test - should be routed
                LOGGER.info("Sending non-mandatory message");
                TextMessage msg1 = producerSession.createTextMessage("msg1");
                nonMandatoryProducer.send(msg1);

                // Second test - should be bounced
                LOGGER.info("Sending non-routable mandatory message");
                TextMessage msg2 = producerSession.createTextMessage("msg2");
                mandatoryProducer.send(msg2);

                LOGGER.info("Starting consumer connection");
                con.start();
                TextMessage tm = (TextMessage) consumer.receive(getReceiveTimeout());

                assertTrue("No message routed to receiver", tm != null);
                assertTrue("Wrong message routed to receiver: " + tm.getText(), "msg1".equals(tm.getText()));

                assertTrue("Message is not bounced",
                           _bounceMessageWaiter.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
                assertNotNull(_bouncedMessage);
                Message m = _bouncedMessage;
                assertTrue("Wrong message bounced: " + m.toString(), m.toString().contains("msg2"));
            }
            finally
            {
                con2.close();
            }
        }
        finally
        {
            con.close();
        }
    }

    @Test
    public void testReturnUnroutableMandatoryMessage_TOPIC() throws Exception
    {
        Connection con = getConnection();
        try
        {
            AMQSession consumerSession = (AMQSession) con.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            AMQTopic valid_topic = new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_CLASS,
                                                "test.Return.Unroutable.Mandatory.Message.TOPIC");
            AMQTopic invalid_topic = new AMQTopic(ExchangeDefaults.TOPIC_EXCHANGE_CLASS,
                                                  "test.Return.Unroutable.Mandatory.Message.TOPIC.invalid");
            MessageConsumer consumer = consumerSession.createConsumer(valid_topic);

            Connection con2 = getConnection();
            try
            {
                con2.setExceptionListener(this);
                AMQSession producerSession = (AMQSession) con2.createSession(false, Session.CLIENT_ACKNOWLEDGE);

                // Need to start the "producer" connection in order to receive bounced messages
                LOGGER.info("Starting producer connection");
                con2.start();

                MessageProducer nonMandatoryProducer = producerSession.createProducer(valid_topic, false, false);
                MessageProducer mandatoryProducer = producerSession.createProducer(invalid_topic, false, true);

                // First test - should be routed
                LOGGER.info("Sending non-mandatory message");
                TextMessage msg1 = producerSession.createTextMessage("msg1");
                nonMandatoryProducer.send(msg1);

                // Second test - should be bounced
                LOGGER.info("Sending non-routable mandatory message");
                TextMessage msg2 = producerSession.createTextMessage("msg2");
                mandatoryProducer.send(msg2);

                LOGGER.info("Starting consumer connection");
                con.start();
                TextMessage tm = (TextMessage) consumer.receive(getReceiveTimeout());

                assertTrue("No message routed to receiver", tm != null);
                assertTrue("Wrong message routed to receiver: " + tm.getText(), "msg1".equals(tm.getText()));

                assertTrue("Message is not bounced",
                           _bounceMessageWaiter.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
                assertNotNull(_bouncedMessage);
                Message m = _bouncedMessage;
                assertTrue("Wrong message bounced: " + m.toString(), m.toString().contains("msg2"));
            }
            finally
            {
                con2.close();
            }
        }
        finally
        {
            con.close();
        }
    }

    @Override
    public void onException(JMSException jmsException)
    {
        Exception linkedException;
        linkedException = jmsException.getLinkedException();
        if (linkedException instanceof AMQNoRouteException)
        {
            LOGGER.info("Caught expected NoRouteException");
            AMQNoRouteException noRoute = (AMQNoRouteException) linkedException;
            _bouncedMessage = (Message) noRoute.getUndeliveredMessage();
            _bounceMessageWaiter.countDown();
        }
        else
        {
            LOGGER.warn("Caught exception on producer: ", jmsException);
        }
    }
}
