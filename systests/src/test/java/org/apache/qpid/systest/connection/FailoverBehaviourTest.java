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
package org.apache.qpid.systest.connection;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TransactionRolledBackException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.jms.ConnectionListener;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.jms.FailoverPolicy;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.core.util.Utils;
import org.apache.qpid.url.URLSyntaxException;

/**
 * Test suite to test all possible failover corner cases
 */
public class FailoverBehaviourTest extends JmsTestBase implements ExceptionListener, ConnectionListener
{
    private static final long DEFAULT_FAILOVER_TIME = Long.getLong("FailoverBaseCase.defaultFailoverTime", 10000L);
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverBehaviourTest.class);

    private static final String TEST_MESSAGE_FORMAT = "test message {0}";

    /** Default number of messages to send before failover */
    private static final int DEFAULT_NUMBER_OF_MESSAGES = 40;

    private static final int DEFAULT_MESSAGE_SIZE = 1024;

    /** Actual number of messages to send before failover */
    private int _messageNumber = Integer.getInteger("profile.failoverMsgCount", DEFAULT_NUMBER_OF_MESSAGES);

    /** Test connection */
    private Connection _connection;
    private CountDownLatch _failoverStarted;
    private CountDownLatch _failoverComplete;

    /**
     * Consumer session
     */
    private Session _consumerSession;

    /**
     * Test destination
     */
    private Destination _destination;

    /**
     * Consumer
     */
    private MessageConsumer _consumer;

    /**
     * Producer session
     */
    private Session _producerSession;

    /**
     * Producer
     */
    private MessageProducer _producer;

    @Before
    public void setUp() throws Exception
    {
        _failoverComplete = new CountDownLatch(1);
        _failoverStarted = new CountDownLatch(1);

        _connection = getConnection();
        _connection.setExceptionListener(this);
        ((AMQConnection) _connection).setConnectionListener(this);
    }

    /**
     * Test whether MessageProducer can successfully publish messages after
     * failover and rollback transaction
     */
    @Test
    public void testMessageProducingAndRollbackAfterFailover() throws Exception
    {
        init(Session.SESSION_TRANSACTED, true);
        produceMessages();
        getBrokerAdmin().restart();

        // producer should be able to send messages after failover
        _producer.send(_producerSession.createTextMessage("test message " + _messageNumber));

        // rollback after failover
        _producerSession.rollback();

        // tests whether sending and committing is working after failover
        produceMessages();
        _producerSession.commit();

        // tests whether receiving and committing is working after failover
        consumeMessages();
        _consumerSession.commit();
    }

    /**
     * Test whether {@link TransactionRolledBackException} is thrown on commit
     * of dirty transacted session after failover.
     * <p>
     * Verifies whether second after failover commit is successful.
     */
    @Test
    public void testTransactionRolledBackExceptionThrownOnCommitAfterFailoverOnProducingMessages() throws Exception
    {
        init(Session.SESSION_TRANSACTED, true);
        produceMessages();
        getBrokerAdmin().restart();

        // producer should be able to send messages after failover
        _producer.send(_producerSession.createTextMessage("test message " + _messageNumber));

        try
        {
            _producerSession.commit();
            fail("TransactionRolledBackException is expected on commit after failover with dirty session!");
        }
        catch (JMSException t)
        {
            assertTrue("Expected TransactionRolledBackException but thrown " + t,
                    t instanceof TransactionRolledBackException);
        }

        // simulate process of user replaying the transaction
        produceMessages("replayed test message {0}", _messageNumber, false);

        // no exception should be thrown
        _producerSession.commit();

        // only messages sent after rollback should be received
        consumeMessages("replayed test message {0}", _messageNumber);

        // no exception should be thrown
        _consumerSession.commit();
    }

    /**
     * Tests JMSException is not thrown on commit with a clean session after
     * failover
     */
    @Test
    public void testNoJMSExceptionThrownOnCommitAfterFailoverWithCleanProducerSession() throws Exception
    {
        init(Session.SESSION_TRANSACTED, true);

        restartAndAwaitFailoverCompletion();

        // should not throw an exception for a clean session
        _producerSession.commit();

        // tests whether sending and committing is working after failover
        produceMessages();
        _producerSession.commit();

        // tests whether receiving and committing is working after failover
        consumeMessages();
        _consumerSession.commit();
    }

    /**
     * Tests {@link TransactionRolledBackException} is thrown on commit of dirty
     * transacted session after failover.
     * <p>
     * Verifies whether second after failover commit is successful.
     */
    @Test
    public void testTransactionRolledBackExceptionThrownOnCommitAfterFailoverOnMessageReceiving() throws Exception
    {
        init(Session.SESSION_TRANSACTED, true);
        produceMessages();
        _producerSession.commit();

        // receive messages but do not commit
        consumeMessages();

        restartAndAwaitFailoverCompletion();

        try
        {
            // should throw TransactionRolledBackException
            _consumerSession.commit();
            fail("TransactionRolledBackException is expected on commit after failover");
        }
        catch (Exception t)
        {
            assertTrue("Expected TransactionRolledBackException but thrown " + t,
                    t instanceof TransactionRolledBackException);
        }

        // consume messages successfully
        consumeMessages();
        _consumerSession.commit();
    }

    /**
     * Tests JMSException is not thrown on commit with a clean session after failover
     */
    @Test
    public void testNoJMSExceptionThrownOnCommitAfterFailoverWithCleanConsumerSession() throws Exception
    {
        init(Session.SESSION_TRANSACTED, true);
        produceMessages();
        _producerSession.commit();

        consumeMessages();
        _consumerSession.commit();

        restartAndAwaitFailoverCompletion();

        // should not throw an exception with a clean consumer session
        _consumerSession.commit();
    }

    /**
     * Test that {@link Session#rollback()} does not throw exception after failover
     * and that we are able to consume messages.
     */
    @Test
    public void testRollbackAfterFailover() throws Exception
    {
        init(Session.SESSION_TRANSACTED, true);

        produceMessages();
        _producerSession.commit();

        consumeMessages();

        restartAndAwaitFailoverCompletion();

        _consumerSession.rollback();

        // tests whether receiving and committing is working after failover
        consumeMessages();
        _consumerSession.commit();
    }

    /**
     * Test that {@link Session#rollback()} does not throw exception after receiving further messages
     * after failover, and we can receive published messages after rollback.
     */
    @Test
    public void testRollbackAfterReceivingAfterFailover() throws Exception
    {
        assumeTrue(getBrokerAdmin().supportsPersistence());
        init(Session.SESSION_TRANSACTED, true);

        produceMessages();
        _producerSession.commit();

        consumeMessages();
        getBrokerAdmin().restart();

        consumeMessages();

        _consumerSession.rollback();

        // tests whether receiving and committing is working after failover
        consumeMessages();
        _consumerSession.commit();
    }

    /**
     * Test that {@link Session#recover()} does not throw an exception after failover
     * and that we can consume messages after recover.
     */
    @Test
    public void testRecoverAfterFailover() throws Exception
    {
        init(Session.CLIENT_ACKNOWLEDGE, true);

        produceMessages();

        // consume messages but do not acknowledge them
        consumeMessages();

        restartAndAwaitFailoverCompletion();

        _consumerSession.recover();

        // tests whether receiving and acknowledgment is working after recover
        Message lastMessage = consumeMessages();
        lastMessage.acknowledge();
    }

    /**
     * Test that receiving more messages after failover and then calling
     * {@link Session#recover()} does not throw an exception
     * and that we can consume messages after recover.
     */
    @Test
    public void testRecoverWithConsumedMessagesAfterFailover() throws Exception
    {
        init(Session.CLIENT_ACKNOWLEDGE, true);

        produceMessages();

        // consume messages but do not acknowledge them
        consumeMessages();

        getBrokerAdmin().restart();

        // publishing should work after failover

        // consume messages again on a dirty session
        consumeMessages();

        // recover should successfully restore session
        _consumerSession.recover();

        // tests whether receiving and acknowledgment is working after recover
        Message lastMessage = consumeMessages();
        lastMessage.acknowledge();
    }

    /**
     * Test that first call to {@link Message#acknowledge()} after failover
     * throws a JMSEXception if session is dirty.
     */
    @Test
    public void testAcknowledgeAfterFailover() throws Exception
    {
        LOGGER.debug("KWDEBUG");
        init(Session.CLIENT_ACKNOWLEDGE, true);

        produceMessages();

        // consume messages but do not acknowledge them
        Message lastMessage = consumeMessages();

        restartAndAwaitFailoverCompletion();

        try
        {
            // an implicit recover performed when acknowledge throws an exception due to failover
            lastMessage.acknowledge();
            fail("JMSException should be thrown");
        }
        catch (JMSException t)
        {
            // TODO: assert error code and/or expected exception type
        }

        // tests whether receiving and acknowledgment is working after recover
        lastMessage = consumeMessages();
        lastMessage.acknowledge();
    }

    /**
     * Test that calling acknowledge before failover leaves the session
     * clean for use after failover.
     */
    @Test
    public void testAcknowledgeBeforeFailover() throws Exception
    {
        init(Session.CLIENT_ACKNOWLEDGE, true);

        produceMessages();

        // consume messages and acknowledge them
        Message lastMessage = consumeMessages();
        lastMessage.acknowledge();

        getBrokerAdmin().restart();

        produceMessages();

        // tests whether receiving and acknowledgment is working after recover
        lastMessage = consumeMessages();
        lastMessage.acknowledge();
    }

    /**
     * Test that receiving of messages after failover prior to calling
     * {@link Message#acknowledge()} still results in acknowledge throwing an exception.
     */
    @Test
    public void testAcknowledgeAfterMessageReceivingAfterFailover() throws Exception
    {
        init(Session.CLIENT_ACKNOWLEDGE, true);

        produceMessages();

        // consume messages but do not acknowledge them
        consumeMessages();

        restartAndAwaitFailoverCompletion();

        // consume again on dirty session
        Message lastMessage = consumeMessages();
        try
        {
            // an implicit recover performed when acknowledge throws an exception due to failover
            lastMessage.acknowledge();
            fail("JMSException should be thrown");
        }
        catch (JMSException t)
        {
            // TODO: assert error code and/or expected exception type
        }

        // tests whether receiving and acknowledgment is working on a clean session
        lastMessage = consumeMessages();
        lastMessage.acknowledge();
    }

    @Test
    public void testClientAcknowledgedSessionCloseAfterFailover() throws Exception
    {
        sessionCloseAfterFailoverImpl(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void testTransactedSessionCloseAfterFailover() throws Exception
    {
        sessionCloseAfterFailoverImpl(Session.SESSION_TRANSACTED);
    }

    @Test
    public void testAutoAcknowledgedSessionCloseAfterFailover() throws Exception
    {
        sessionCloseAfterFailoverImpl(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testPublishAutoAcknowledgedWhileFailover() throws Exception
    {
        publishWhileFailingOver(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testPublishClientAcknowledgedWhileFailover() throws Exception
    {
        Message receivedMessage = publishWhileFailingOver(Session.CLIENT_ACKNOWLEDGE);
        receivedMessage.acknowledge();
    }

    @Test
    public void testPublishTransactedAcknowledgedWhileFailover() throws Exception
    {
        publishWhileFailingOver(Session.SESSION_TRANSACTED);
        _consumerSession.commit();
    }

    @Test
    public void testClientAcknowledgedSessionCloseWhileFailover() throws Exception
    {
        sessionCloseWhileFailoverImpl(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void testTransactedSessionCloseWhileFailover() throws Exception
    {
        sessionCloseWhileFailoverImpl(Session.SESSION_TRANSACTED);
    }

    @Test
    public void testAutoAcknowledgedSessionCloseWhileFailover() throws Exception
    {
        sessionCloseWhileFailoverImpl(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testClientAcknowledgedQueueBrowserCloseWhileFailover() throws Exception
    {
        browserCloseWhileFailoverImpl(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void testTransactedQueueBrowserCloseWhileFailover() throws Exception
    {
        browserCloseWhileFailoverImpl(Session.SESSION_TRANSACTED);
    }

    @Test
    public void testAutoAcknowledgedQueueBrowserCloseWhileFailover() throws Exception
    {
        browserCloseWhileFailoverImpl(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testKillBrokerFailoverWhilstPublishingInFlight() throws Exception
    {
        doFailoverWhilstPublishingInFlight();
    }

    @Test
    public void testStopBrokerFailoverWhilstPublishingInFlight() throws Exception
    {
        doFailoverWhilstPublishingInFlight();
    }

    private void doFailoverWhilstPublishingInFlight() throws Exception
    {
        init(Session.SESSION_TRANSACTED, false);

        final int numberOfMessages = 200;

        final CountDownLatch halfWay = new CountDownLatch(1);
        final CountDownLatch allDone = new CountDownLatch(1);
        final AtomicReference<Exception> exception = new AtomicReference<>();

        Runnable producerRunnable = new Runnable()
        {
            @Override
            public void run()
            {
                Thread.currentThread().setName("ProducingThread");

                try
                {
                    for(int i=0; i< numberOfMessages; i++)
                    {
                        boolean success = false;
                        while(!success)
                        {
                            try
                            {
                                Message message = _producerSession.createMessage();
                                message.setIntProperty("msgNum", i);
                                _producer.send(message);
                                _producerSession.commit();
                                success = true;
                            }
                            catch (javax.jms.IllegalStateException e)
                            {
                                // fail - failover should not leave a JMS object in an illegal state
                                throw e;
                            }
                            catch (JMSException e)
                            {
                                // OK we will be failing over
                                LOGGER.debug("Got JMS exception, probably just failing over", e);
                            }
                        }

                        if (i > numberOfMessages / 2 && halfWay.getCount() == 1)
                        {
                            halfWay.countDown();
                        }
                    }

                    allDone.countDown();
                }
                catch (Exception e)
                {
                    exception.set(e);
                }
            }
        };

        Thread producerThread = new Thread(producerRunnable);
        producerThread.start();

        assertTrue("Didn't get to half way within timeout", halfWay.await(30000, TimeUnit.MILLISECONDS));

        getBrokerAdmin().restart();

        if (exception.get() != null)
        {
            LOGGER.error("Unexpected exception from producer thread", exception.get());
        }
        assertNull("Producer thread should not have got an exception", exception.get());

        assertTrue("All producing work was not completed", allDone.await(30000, TimeUnit.MILLISECONDS));

        producerThread.join(30000);

        // Extra work to prove the session still okay
        assertNotNull(_producerSession.createTemporaryQueue());
    }


    private Message publishWhileFailingOver(int autoAcknowledge) throws JMSException, InterruptedException
    {
        setDelayedFailoverPolicy(5);
        init(autoAcknowledge, true);

        String text = MessageFormat.format(TEST_MESSAGE_FORMAT, 0);
        Message message = _producerSession.createTextMessage(text);

        getBrokerAdmin().restart();

        if(!_failoverStarted.await(5, TimeUnit.SECONDS))
        {
            fail("Did not receive notification failover had started");
        }

        _producer.send(message);

        if (_producerSession.getTransacted())
        {
            _producerSession.commit();
        }

        Message receivedMessage = _consumer.receive(1000L);
        assertReceivedMessage(receivedMessage, TEST_MESSAGE_FORMAT, 0);
        return receivedMessage;
    }


    /**
     * This test only tests 0-8/0-9/0-9-1 failover timeout
     */
    @Test
    public void testFailoverHandlerTimeoutExpires() throws Exception
    {

        assumeThat("This test only tests 0-8/0-9/0-9-1 failover timeout",
                   getProtocol(),
                   is(not(equalTo("0-10"))));

        _connection.close();
        _connection = null;

        System.setProperty("qpid.failover_method_timeout", "10000");
        AMQConnection connection = null;
        try
        {
            connection = createConnectionWithFailover();

            // holding failover mutex should prevent the failover from proceeding
            synchronized(connection.getFailoverMutex())
            {
                getBrokerAdmin().restart();

                // sleep interval exceeds failover timeout interval
                Thread.sleep(11000L);
            }

            // allows the failover thread to proceed
            Thread.yield();
            assertFalse("Unexpected failover", _failoverComplete.await(2000L, TimeUnit.MILLISECONDS));
            assertTrue("Failover should not succeed due to timeout", connection.isClosed());
        }
        finally
        {
            System.clearProperty("qpid.failover_method_timeout");
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    @Test
    public void testFailoverHandlerTimeoutReconnected() throws Exception
    {
        _connection.close();
        System.setProperty("qpid.failover_method_timeout", "10000");
        AMQConnection connection = null;
        try
        {
            connection = createConnectionWithFailover();

            // holding failover mutex should prevent the failover from proceeding
            synchronized(connection.getFailoverMutex())
            {
                getBrokerAdmin().restart();
            }

            // allows the failover thread to proceed
            Thread.yield();
            awaitForFailoverCompletion(DEFAULT_FAILOVER_TIME);
            assertFalse("Failover should restore connectivity", connection.isClosed());
        }
        finally
        {
            System.clearProperty("qpid.failover_method_timeout");
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    /**
     * Tests that the producer flow control flag is reset when failover occurs while
     * the producers are being blocked by the broker.
     *
     * Uses Apache Qpid Broker-J specific queue configuration to enabled PSFC.
     */
    @Test
    public void testFlowControlFlagResetOnFailover() throws Exception
    {

        assumeThat("This test only tests 0-8/0-9/0-9-1 failover timeout",
                   getProtocol(),
                   is(not(equalTo("0-10"))));


        // we do not need the connection failing to second broker
        _connection.close();

        // make sure that failover timeout is bigger than flow control timeout
        System.setProperty("qpid.failover_method_timeout", "60000");
        System.setProperty("qpid.flow_control_wait_failure", "10000");

        AMQConnection connection = null;
        try
        {
            connection = createConnectionWithFailover(Collections.singletonMap(ConnectionURL.OPTIONS_SYNC_PUBLISH, "all"));

            final Session producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            final Queue queue = createAndBindQueueWithFlowControlEnabled(producerSession, getTestQueueName(), DEFAULT_MESSAGE_SIZE * 3, DEFAULT_MESSAGE_SIZE * 2);
            final AtomicInteger counter = new AtomicInteger();
            // try to send 5 messages (should block after 4)
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        MessageProducer producer = producerSession.createProducer(queue);
                        for (int i=0; i < 5; i++)
                        {
                            Message next = createNextMessage(producerSession, i);
                            producer.send(next);
                            producerSession.commit();
                            counter.incrementAndGet();
                        }
                    }
                    catch(Exception e)
                    {
                        // ignore
                    }
                }
            }).start();

            long limit= 30000L;
            long start = System.currentTimeMillis();

            // wait  until session is blocked
            while(!((AMQSession<?,?>)producerSession).isFlowBlocked() && System.currentTimeMillis() - start < limit)
            {
                Thread.sleep(100L);
            }

            assertTrue("Flow is not blocked", ((AMQSession<?, ?>) producerSession).isFlowBlocked());

            final int currentCounter = counter.get();
            assertTrue("Unexpected number of sent messages:" + currentCounter, currentCounter >=3);

            getBrokerAdmin().restart();

            // allows the failover thread to proceed
            Thread.yield();
            awaitForFailoverCompletion(60000L);

            assertFalse("Flow is blocked", ((AMQSession<?, ?>) producerSession).isFlowBlocked());
        }
        finally
        {
            System.clearProperty("qpid.failover_method_timeout");
            System.clearProperty("qpid.flow_control_wait_failure");

            if (connection != null)
            {
                connection.close();
            }
        }
    }

    @Test
    public void testFailoverWhenConnectionStopped() throws Exception
    {
        init(Session.SESSION_TRANSACTED, true);

        produceMessages();
        _producerSession.commit();

        final CountDownLatch stopFlag = new CountDownLatch(1);
        final AtomicReference<Exception> exception = new AtomicReference<>();
        final CountDownLatch expectedMessageLatch = new CountDownLatch(_messageNumber);
        final AtomicInteger counter = new AtomicInteger();

        _consumer.setMessageListener(new MessageListener()
        {
            @Override
            public void onMessage(Message message)
            {
                if (stopFlag.getCount() == 1)
                {
                    try
                    {
                        LOGGER.debug("Stopping connection from dispatcher thread");
                        _connection.stop();
                        LOGGER.debug("Connection stopped from dispatcher thread");

                    }
                    catch (Exception e)
                    {
                        exception.set(e);
                    }
                    finally
                    {
                        stopFlag.countDown();
                        getBrokerAdmin().restart();
                    }

                }
                else
                {
                    try
                    {
                        _consumerSession.commit();
                        counter.incrementAndGet();
                        expectedMessageLatch.countDown();
                    }
                    catch (Exception e)
                    {
                        exception.set(e);
                    }
                }
            }
        });


        boolean stopResult = stopFlag.await(2000, TimeUnit.MILLISECONDS);
        assertTrue("Connection was not stopped" + (exception.get() == null ? "." : ":" + exception.get().getMessage()),
                stopResult);
        assertNull("Unexpected exception on stop :" + exception.get(), exception.get());

        // wait for failover to complete
        awaitForFailoverCompletion(DEFAULT_FAILOVER_TIME);

        _producerSession.commit();

        _connection.start();

        assertTrue("Not all messages were delivered. Remaining message number " + expectedMessageLatch.getCount(), expectedMessageLatch.await(11000, TimeUnit.MILLISECONDS));

        Thread.sleep(500L);
        assertEquals("Unexpected messages recieved ", _messageNumber, counter.get());

        _connection.close();
    }

    @Test
    public void testConnectionCloseInterruptsFailover() throws Exception
    {
        _connection.close();

        final AtomicBoolean failoverCompleted = new AtomicBoolean(false);
        final CountDownLatch failoverBegun = new CountDownLatch(1);

        AMQConnection connection = createConnectionWithFailover();
        connection.setConnectionListener(new ConnectionListener()
        {
            @Override
            public void bytesSent(final long count)
            {
            }

            @Override
            public void bytesReceived(final long count)
            {
            }

            @Override
            public boolean preFailover(final boolean redirect)
            {
                failoverBegun.countDown();
                LOGGER.info("Failover started");
                return true;
            }

            @Override
            public boolean preResubscribe()
            {
                return true;
            }

            @Override
            public void failoverComplete()
            {
                failoverCompleted.set(true);
            }
        });

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertNotNull("Session should be created", session);
        getBrokerAdmin().stop();

        boolean failingOver = failoverBegun.await(5000, TimeUnit.MILLISECONDS);
        assertTrue("Failover did not begin with a reasonable time", failingOver);

        // Failover will now be in flight
        connection.close();
        assertTrue("Failover policy is unexpectedly exhausted", connection.getFailoverPolicy().failoverAllowed());
    }

    private Queue createAndBindQueueWithFlowControlEnabled(Session session, String queueName, int capacity, int resumeCapacity) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-qpid-capacity", capacity);
        arguments.put("x-qpid-flow-resume-capacity", resumeCapacity);
        ((AMQSession<?, ?>) session).createQueue(queueName, false, true, false, arguments);
        Queue queue = session.createQueue("direct://amq.direct/" + queueName + "/" + queueName + "?durable='" + true
                + "'&autodelete='" + false + "'");
        ((AMQSession<?, ?>) session).declareAndBind((AMQDestination) queue);
        return queue;
    }

    private AMQConnection createConnectionWithFailover() throws JMSException
    {
        return createConnectionWithFailover(null);
    }

    private AMQConnection createConnectionWithFailover(Map<String,String> connectionOptions) throws JMSException
    {
        String retries = "200";
        String connectdelay = "1000";
        String cycleCount = "2";

        String newUrlFormat = "amqp://username:password@clientid/%s?brokerlist=" +
                              "'tcp://%s:%s?retries='%s'&connectdelay='%s''&failover='singlebroker?cyclecount='%s''";

        int port = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getPort();
        String virtualHostName = getBrokerAdmin().getVirtualHostName();
        StringBuilder newUrl = new StringBuilder(String.format(newUrlFormat,
                                                               virtualHostName,
                                                               "localhost",
                                                               port,
                                                               retries, connectdelay, cycleCount));

        if (connectionOptions != null)
        {
            for (Map.Entry<String,String> option: connectionOptions.entrySet())
            {
                newUrl.append("&").append(option.getKey()).append("='").append(option.getValue()).append("'");
            }
        }
        ConnectionFactory connectionFactory;
        try
        {
            connectionFactory = new AMQConnectionFactory(newUrl.toString());
        }
        catch (URLSyntaxException e)
        {
            throw new RuntimeException(e);
        }
        AMQConnection connection = (AMQConnection) connectionFactory.createConnection(getBrokerAdmin().getValidUsername(),
                                                                                      getBrokerAdmin().getValidPassword());
        connection.setConnectionListener(this);
        return connection;
    }

    /**
     * Tests {@link Session#close()} for session with given acknowledge mode
     * to ensure that close works after failover.
     *
     * @param acknowledgeMode session acknowledge mode
     */
    private void sessionCloseAfterFailoverImpl(int acknowledgeMode) throws JMSException
    {
        init(acknowledgeMode, true);
        produceMessages(TEST_MESSAGE_FORMAT, _messageNumber, false);
        if (acknowledgeMode == Session.SESSION_TRANSACTED)
        {
            _producerSession.commit();
        }

        // intentionally receive message but do not commit or acknowledge it in
        // case of transacted or CLIENT_ACK session
        Message receivedMessage = _consumer.receive(1000L);
        assertReceivedMessage(receivedMessage, TEST_MESSAGE_FORMAT, 0);

        restartAndAwaitFailoverCompletion();

        // for transacted/client_ack session
        // no exception should be thrown but transaction should be automatically
        // rolled back
        _consumerSession.close();
    }

    /**
     * A helper method to instantiate produce and consumer sessions, producer
     * and consumer.
     *
     * @param acknowledgeMode
     *            acknowledge mode
     * @param startConnection
     *            indicates whether connection should be started
     */
    private void init(int acknowledgeMode, boolean startConnection) throws JMSException
    {
        boolean isTransacted = acknowledgeMode == Session.SESSION_TRANSACTED;

        _consumerSession = _connection.createSession(isTransacted, acknowledgeMode);
        _destination = createDestination(_consumerSession);
        _consumer = _consumerSession.createConsumer(_destination);

        if (startConnection)
        {
            _connection.start();
        }

        _producerSession = _connection.createSession(isTransacted, acknowledgeMode);
        _producer = _producerSession.createProducer(_destination);

    }

    private Destination createDestination(Session session) throws JMSException
    {
        return session.createQueue(getTestQueueName() + "_" + System.currentTimeMillis());
    }

    /**
     * Produces a default number of messages with default text content into test
     * queue
     *
     */
    private void produceMessages() throws JMSException
    {
        produceMessages(false);
    }

    private void produceMessages(boolean separateProducer) throws JMSException
    {
        produceMessages(TEST_MESSAGE_FORMAT, _messageNumber, separateProducer);
    }

    /**
     * Consumes a default number of messages and asserts their content.
     *
     * @return last consumed message
     */
    private Message consumeMessages() throws JMSException
    {
        return consumeMessages(TEST_MESSAGE_FORMAT, _messageNumber);
    }

    /**
     * Produces given number of text messages with content matching given
     * content pattern
     *
     * @param messagePattern message content pattern
     * @param messageNumber  number of messages to send
     * @param standaloneProducer whether to use the existing producer or a new one.
     */
    private void produceMessages(String messagePattern, int messageNumber, boolean standaloneProducer) throws JMSException
    {
        Session producerSession;
        MessageProducer producer;

        if(standaloneProducer)
        {
            producerSession = _connection.createSession(true, Session.SESSION_TRANSACTED);
            producer = producerSession.createProducer(_destination);
        }
        else
        {
            producerSession = _producerSession;
            producer = _producer;
        }

        for (int i = 0; i < messageNumber; i++)
        {
            String text = MessageFormat.format(messagePattern, i);
            Message message = producerSession.createTextMessage(text);
            producer.send(message);
            LOGGER.debug("Test message number " + i + " produced with text = " + text + ", and JMSMessageID = " + message.getJMSMessageID());
        }

        if(standaloneProducer)
        {
            producerSession.commit();
        }
    }

    /**
     * Consumes given number of text messages and asserts that their content
     * matches given pattern
     *
     * @param messagePattern
     *            messages content pattern
     * @param messageNumber
     *            message number to received
     * @return last consumed message
     */
    private Message consumeMessages(String messagePattern, int messageNumber) throws JMSException
    {
        Message receivedMesssage = null;
        for (int i = 0; i < messageNumber; i++)
        {
            receivedMesssage = _consumer.receive(1000L);
            assertReceivedMessage(receivedMesssage, messagePattern, i);
        }
        return receivedMesssage;
    }

    /**
     * Asserts received message
     *
     * @param receivedMessage
     *            received message
     * @param messagePattern
     *            messages content pattern
     * @param messageIndex
     *            message index
     */
    private void assertReceivedMessage(Message receivedMessage, String messagePattern, int messageIndex) throws JMSException
    {
        assertNotNull("Expected message [" + messageIndex + "] is not received!", receivedMessage);
        assertTrue("Failure to receive message [" + messageIndex + "], expected TextMessage but received "
                + receivedMessage, receivedMessage instanceof TextMessage);
        String expectedText = MessageFormat.format(messagePattern, messageIndex);
        String receivedText = null;
        try
        {
            receivedText = ((TextMessage) receivedMessage).getText();
        }
        catch (JMSException e)
        {
            fail("JMSException occured while getting message text:" + e.getMessage());
        }
        LOGGER.debug("Test message number " + messageIndex + " consumed with text = " + receivedText + ", and JMSMessageID = " + receivedMessage.getJMSMessageID());
        assertEquals("Failover is broken! Expected [" + expectedText + "] but got [" + receivedText + "]",
                expectedText, receivedText);
    }


    private void restartAndAwaitFailoverCompletion()
    {
        getBrokerAdmin().restart();
        awaitForFailoverCompletion();
    }

    private void awaitForFailoverCompletion()
    {
        awaitForFailoverCompletion(2 * DEFAULT_FAILOVER_TIME);
    }

    private void awaitForFailoverCompletion(long delay)
    {
        LOGGER.info("Awaiting {} ms for failover completion..", delay);
        try
        {
            if (!_failoverComplete.await(delay, TimeUnit.MILLISECONDS))
            {
                fail("Failover did not complete");
            }
        }
        catch (InterruptedException e)
        {
            fail("Test was interrupted:" + e.getMessage());
        }
    }

    @Override
    public void onException(JMSException e)
    {
    }

    @After
    public void tearDown()
    {
        if (_connection != null)
        {
            try
            {
                _connection.close();
            }
            catch (JMSException e)
            {
                // PASS
            }
        }
    }

    public Connection getConnection() throws JMSException
    {
        return createConnectionWithFailover();
    }

    public void failDefaultBroker()
    {
        getBrokerAdmin().stop();
    }

    @Override
    public void bytesSent(long count)
    {
    }

    @Override
    public void bytesReceived(long count)
    {
    }

    @Override
    public boolean preFailover(boolean redirect)
    {
        _failoverStarted.countDown();
        return true;
    }

    @Override
    public boolean preResubscribe()
    {
        return true;
    }

    @Override
    public void failoverComplete()
    {
        _failoverComplete.countDown();
    }

    /**
     * Causes 1 second delay before reconnect in order to test whether JMS
     * methods block while failover is in progress
     */
    private static class DelayingFailoverPolicy extends FailoverPolicy
    {

        private CountDownLatch _suspendLatch;
        private long _delay;

        DelayingFailoverPolicy(AMQConnection connection, long delay)
        {
            super(connection.getConnectionURL(), connection);
            _suspendLatch = new CountDownLatch(1);
            _delay = delay;
        }

        @Override
        public void attainedConnection()
        {
            try
            {
                _suspendLatch.await(_delay, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                // continue
            }
            super.attainedConnection();
        }
    }

    /**
     * Tests {@link Session#close()} for session with given acknowledge mode
     * to ensure that it blocks until failover implementation restores connection.
     *
     * @param acknowledgeMode session acknowledge mode
     */
    private void sessionCloseWhileFailoverImpl(int acknowledgeMode) throws Exception
    {
        initDelayedFailover(acknowledgeMode);

        // intentionally receive message but not commit or acknowledge it in
        // case of transacted or CLIENT_ACK session
        Message receivedMessage = _consumer.receive(1000L);
        assertReceivedMessage(receivedMessage, TEST_MESSAGE_FORMAT, 0);

        getBrokerAdmin().restart();

        // wait until failover is started
        _failoverStarted.await(5, TimeUnit.SECONDS);

        // test whether session#close blocks while failover is in progress
        _consumerSession.close();

        assertTrue("Failover has not completed yet but session was closed", _failoverComplete.await(5, TimeUnit.SECONDS));
    }

    /**
     * A helper method to instantiate {@link QueueBrowser} and publish test messages on a test queue for further browsing.
     *
     * @param acknowledgeMode session acknowledge mode
     * @return queue browser
     */
    private QueueBrowser prepareQueueBrowser(int acknowledgeMode) throws JMSException, QpidException
    {
        init(acknowledgeMode, false);
        _consumer.close();
        _connection.start();

        produceMessages(TEST_MESSAGE_FORMAT, _messageNumber, false);
        if (acknowledgeMode == Session.SESSION_TRANSACTED)
        {
            _producerSession.commit();
        }
        else
        {
            ((AMQSession)_producerSession).sync();
        }

        return _consumerSession.createBrowser((Queue) _destination);
    }

    /**
     * Tests {@link QueueBrowser#close()} for session with given acknowledge mode
     * to ensure that it blocks until failover implementation restores connection.
     *
     * @param acknowledgeMode session acknowledge mode
     */
    private void browserCloseWhileFailoverImpl(int acknowledgeMode) throws Exception
    {
        QueueBrowser browser = prepareQueueBrowser(acknowledgeMode);

        @SuppressWarnings("unchecked")
        Enumeration<Message> messages = browser.getEnumeration();
        Message receivedMessage = messages.nextElement();
        assertReceivedMessage(receivedMessage, TEST_MESSAGE_FORMAT, 0);

        getBrokerAdmin().restart();

        // wait until failover is started
        _failoverStarted.await(5, TimeUnit.SECONDS);

        browser.close();

        assertTrue("Failover has not completed yet but browser was closed", _failoverComplete.await(5, TimeUnit.SECONDS));
    }

    private DelayingFailoverPolicy initDelayedFailover(int acknowledgeMode) throws JMSException
    {
        DelayingFailoverPolicy failoverPolicy = setDelayedFailoverPolicy();
        init(acknowledgeMode, true);
        produceMessages(TEST_MESSAGE_FORMAT, _messageNumber, false);
        if (acknowledgeMode == Session.SESSION_TRANSACTED)
        {
            _producerSession.commit();
        }
        return failoverPolicy;
    }

    private DelayingFailoverPolicy setDelayedFailoverPolicy()
    {
        return setDelayedFailoverPolicy(2);
    }

    private DelayingFailoverPolicy setDelayedFailoverPolicy(long delay)
    {
        AMQConnection amqConnection = (AMQConnection) _connection;
        DelayingFailoverPolicy failoverPolicy = new DelayingFailoverPolicy(amqConnection, delay);
        ((AMQConnection) _connection).setFailoverPolicy(failoverPolicy);
        return failoverPolicy;
    }


    public Message createNextMessage(Session session, int msgCount) throws JMSException
    {
        Message message = createMessage(session, DEFAULT_MESSAGE_SIZE);
        message.setIntProperty(Utils.INDEX, msgCount);

        return message;
    }

    public Message createMessage(Session session, int messageSize) throws JMSException
    {
        String payload = new String(new byte[messageSize]);

        TextMessage message = session.createTextMessage();
        message.setText(payload);
        return message;
    }
}
