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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.util.Collections;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.core.brokerj.AmqpManagementFacade;

public class ProducerFlowControlTest extends JmsTestBase
{
    private Connection _producerConnection;
    private MessageProducer _producer;
    private BytesMessage _message;
    private AmqpManagementFacade _managementFacade;

    @Before
    public void setUp() throws Exception
    {
        assumeThat("Test suite requires amqp management",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));
        _managementFacade = new AmqpManagementFacade();

        System.setProperty("qpid.flow_control_wait_failure", "3000");
        System.setProperty("qpid.flow_control_wait_notify_period", "1000");

        _producerConnection = getConnection(Collections.singletonMap("sync_publish", "all"));
        _producerConnection.start();
        final Session producerSession = _producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String queueName = getTestQueueName();
        final Queue queue = createAndBindQueueWithFlowControlEnabled(producerSession, queueName, 1000, 800);

        _producer = producerSession.createProducer(queue);
        _message = producerSession.createBytesMessage();
        _message.writeBytes(new byte[1100]);
    }

    @After
    public void tearDown() throws Exception
    {
        System.clearProperty("qpid.flow_control_wait_failure");
        System.clearProperty("qpid.flow_control_wait_notify_period");
        if (_producerConnection != null)
        {
            _producerConnection.close();
        }
    }

    @Test
    public void testClientLogMessages() throws Exception
    {
        _producer.send(_message);

        final long timeout = System.currentTimeMillis() + getReceiveTimeout();
        awaitFlowBlocked(timeout);

        // Ensure that the client has processed the incoming flow/messagestop
        _producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE).close();
        try
        {
            _producer.send(_message);
            fail("Producer should be blocked by flow control");
        }
        catch (JMSException e)
        {
            final String expectedMsg =
                    "0-10".equals(getProtocol()) ? "Exception when sending message:timed out waiting for message credit"
                            : "Unable to send message for 3 seconds due to broker enforced flow control";
            assertEquals("Unexpected exception reason", expectedMsg, e.getMessage());
        }
    }

    private void awaitFlowBlocked(final long timeout) throws Exception
    {
        Connection con = getConnection();
        con.start();
        Session session = con.createSession(true, Session.SESSION_TRANSACTED);
        try
        {
            boolean queueFlowStopped;
            do
            {
                Map<String, Object> response = _managementFacade.readEntityUsingAmqpManagement(getTestQueueName(),
                                                                                               "org.apache.qpid.Queue",
                                                                                               false,
                                                                                               session);

                queueFlowStopped = Boolean.parseBoolean(String.valueOf(response.get("queueFlowStopped")));
            }
            while (!queueFlowStopped || System.currentTimeMillis() > timeout);
            assertTrue("Flow did not become blocked within timeout", queueFlowStopped);
        }
        finally
        {
            con.close();
        }
    }

    private Queue createAndBindQueueWithFlowControlEnabled(Session session,
                                                           String queueName,
                                                           int capacity,
                                                           int resumeCapacity) throws Exception
    {
        return session.createQueue(String.format(
                "ADDR:%s; {create: always, node: {x-declare: {arguments:{'x-qpid-capacity': %d,"
                                                                     + " 'x-qpid-flow-resume-capacity': %d}}}}",
                queueName,
                capacity,
                resumeCapacity));
    }
}



