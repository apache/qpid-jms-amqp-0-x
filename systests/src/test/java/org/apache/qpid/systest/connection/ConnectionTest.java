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
package org.apache.qpid.systest.connection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.net.InetSocketAddress;
import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.QueueSession;
import javax.jms.TopicSession;

import org.junit.Test;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.jms.Session;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;

public class ConnectionTest extends JmsTestBase
{
    private static final String BROKER_URL_TEMPLATE = "tcp://%s:%d?failover='nofailover'";
    private static final String CONNECTION_URL_TEMPLATE = "amqp://%s:%s@%s/%s?brokerlist='%s'";
    private static final String USER1 = "guest";
    private static final String USER1_PASSWORD = "guest";
    private static final String USER2 = "admin";
    private static final String USER2_PASSWORD = "admin";

    @Test
    public void testDefaultExchanges() throws Exception
    {
        assumeThat("0-10 c++ broker doesn't implement wacky exchanges",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

        String connectionUrlTemplate = CONNECTION_URL_TEMPLATE
                                       + "&defaultQueueExchange='test.direct'"
                                       + "&defaultTopicExchange='test.topic'"
                                       + "&temporaryQueueExchange='tmp.direct'"
                                       + "&temporaryTopicExchange='tmp.topic'";
        BrokerAdmin admin = getBrokerAdmin();
        InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        String brokerUrl = String.format(BROKER_URL_TEMPLATE, brokerAddress.getHostName(), brokerAddress.getPort());
        String urlString = String.format(connectionUrlTemplate,
                                         admin.getValidUsername(),
                                         admin.getValidPassword(),
                                         getTestName(),
                                         admin.getVirtualHostName(),
                                         brokerUrl);
        AMQConnection conn = new AMQConnection(new AMQConnectionURL(urlString));
        try
        {

            AMQSession sess = (AMQSession) conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            sess.declareExchange("test.direct", ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

            sess.declareExchange("tmp.direct", ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

            sess.declareExchange("tmp.topic", ExchangeDefaults.TOPIC_EXCHANGE_CLASS, false);

            sess.declareExchange("test.topic", ExchangeDefaults.TOPIC_EXCHANGE_CLASS, false);

            QueueSession queueSession = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            AMQQueue queue = (AMQQueue) queueSession.createQueue("MyQueue");

            assertEquals(queue.getExchangeName(), "test.direct");

            AMQQueue tempQueue = (AMQQueue) queueSession.createTemporaryQueue();

            assertEquals(tempQueue.getExchangeName(), "tmp.direct");

            queueSession.close();

            TopicSession topicSession = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

            AMQTopic topic = (AMQTopic) topicSession.createTopic("silly.topic");

            assertEquals(topic.getExchangeName(), "test.topic");

            AMQTopic tempTopic = (AMQTopic) topicSession.createTemporaryTopic();

            assertEquals(tempTopic.getExchangeName(), "tmp.topic");

            topicSession.close();
        }
        finally
        {
            conn.close();
        }
    }

    @Test
    public void testClientIdIsPopulatedAutomatically() throws Exception
    {
        BrokerAdmin admin = getBrokerAdmin();
        InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        String brokerUrl = String.format(BROKER_URL_TEMPLATE, brokerAddress.getHostName(), brokerAddress.getPort());
        Connection connection = new AMQConnection(brokerUrl,
                                                  admin.getValidUsername(),
                                                  admin.getValidPassword(),
                                                  null,
                                                  admin.getVirtualHostName());
        try
        {
            assertNotNull(connection.getClientID());
        }
        finally
        {
            connection.close();
        }
        connection.close();
    }

    @Test
    public void testUnsupportedSASLMechanism() throws Exception
    {
        BrokerAdmin admin = getBrokerAdmin();
        InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        String brokerUrl = String.format(BROKER_URL_TEMPLATE + "&sasl_mechs='%s'", brokerAddress.getHostName(),
                                         brokerAddress.getPort(), "MY_MECH");

        try
        {
            new AMQConnection(brokerUrl,
                              admin.getValidUsername(),
                              admin.getValidPassword(),
                              null,
                              admin.getVirtualHostName());
            fail("The client should throw a ConnectionException stating the" +
                 " broker does not support the SASL mech specified by the client");
        }
        catch (QpidException e)
        {
            if (getProtocol().equals("0-10"))
            {
                assertTrue("Unexpected exception message : " + e.getMessage(),
                           e.getMessage().contains("Client and broker have no SASL mechanisms in common."));
                assertTrue("Unexpected exception message : " + e.getMessage(),
                           e.getMessage().contains("Client restricted itself to : MY_MECH"));
            }
            else
            {
                assertTrue("Unexpected exception message : " + e.getMessage(),
                           e.getMessage().contains("No supported security mechanism found"));
            }
        }
    }

    /**
     * Tests that when the same user connects twice with same clientid, the second connection
     * fails if the clientid verification feature is enabled (which uses a dummy 0-10 Session
     * with the clientid as its name to detect the previous usage of the clientid by the user)
     */
    @Test
    public void testClientIDVerificationForSameUser() throws Exception
    {
        assumeThat("QPID-3396: 0-10 client specific behaviour",
                   getProtocol(),
                   is(equalTo("0-10")));

        System.setProperty(ClientProperties.QPID_VERIFY_CLIENT_ID, "true");
        try
        {
            BrokerAdmin admin = getBrokerAdmin();
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            String brokerUrl = String.format(BROKER_URL_TEMPLATE, brokerAddress.getHostName(), brokerAddress.getPort());
            Connection con = new AMQConnection(brokerUrl,
                                               admin.getValidUsername(),
                                               admin.getValidPassword(),
                                               "client_id",
                                               admin.getVirtualHostName());
            try
            {

                new AMQConnection(brokerUrl,
                                  admin.getValidUsername(),
                                  admin.getValidPassword(),
                                  "client_id",
                                  admin.getVirtualHostName());

                fail("The client should throw a ConnectionException stating the" +
                     " client ID is not unique");
            }
            catch (QpidException e)
            {
                assertTrue("Incorrect exception thrown: " + e.getMessage(),
                           e.getMessage().contains("ClientID must be unique"));
            }
            finally
            {
                con.close();
            }
        }
        finally
        {
            System.clearProperty(ClientProperties.QPID_VERIFY_CLIENT_ID);
        }
    }

    /**
     * Tests that when different users connects with same clientid, the second connection
     * succeeds even though the clientid verification feature is enabled (which uses a dummy
     * 0-10 Session with the clientid as its name; these are only verified unique on a
     * per-principal basis)
     */
    @Test
    public void testClientIDVerificationForDifferentUsers() throws Exception
    {
        System.setProperty(ClientProperties.QPID_VERIFY_CLIENT_ID, "true");
        try
        {
            BrokerAdmin admin = getBrokerAdmin();
            InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            String brokerUrl = String.format(BROKER_URL_TEMPLATE, brokerAddress.getHostName(), brokerAddress.getPort());

            String clientId = "client_id";
            Connection con = new AMQConnection(brokerUrl, USER1, USER1_PASSWORD,
                                               clientId, admin.getVirtualHostName());
            try
            {
                Connection con2 = new AMQConnection(brokerUrl, USER2, USER2_PASSWORD,
                                                    clientId, admin.getVirtualHostName());
                try
                {
                    assertNotNull(con2.createSession(false, Session.AUTO_ACKNOWLEDGE));
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
        finally
        {
            System.clearProperty(ClientProperties.QPID_VERIFY_CLIENT_ID);
        }
    }

    @Test
    public void testExceptionWhenUserPassIsRequired() throws Exception
    {
        assumeThat("QPID-3396: NPE is thrown on 0-8..0-10.",
                   getProtocol(),
                   is(equalTo("0-10")));

        BrokerAdmin admin = getBrokerAdmin();
        InetSocketAddress brokerAddress = admin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        String brokerUrl = String.format(BROKER_URL_TEMPLATE, brokerAddress.getHostName(), brokerAddress.getPort())
                           + "&sasl_mechs='PLAIN%2520CRAM-MD5'";
        String urlString = String.format("amqp:///%s?brokerlist='%s'", admin.getVirtualHostName(), brokerUrl);
        AMQConnection conn = null;
        try
        {
            conn = new AMQConnection(urlString);
            fail("Exception should be thrown as user name and password is required");
        }
        catch (Exception e)
        {
            if (!e.getMessage().contains("Username and Password is required for the selected mechanism"))
            {
                if (conn != null && !conn.isClosed())
                {
                    conn.close();
                }
                fail("Incorrect Exception thrown! The exception thrown is : " + e.getMessage());
            }
        }
    }

    @Test
    public void testConnectionMetadata() throws Exception
    {
        Connection con = getConnection();
        ConnectionMetaData metaData = con.getMetaData();
        assertNotNull(metaData);

        assertNotNull("Provider version unexpectedly null", metaData.getProviderVersion());
        assertTrue("Provider version unexpectedly empty", metaData.getProviderVersion().length() > 0);

        assertTrue("Provider major version has unexpected value", metaData.getProviderMajorVersion() > -1);
        assertTrue("Provider minor version has unexpected value", metaData.getProviderMinorVersion() > -1);

        Enumeration names = metaData.getJMSXPropertyNames();
        assertNotNull("JMSXPropertyNames unexpectedly null", names);
        assertTrue("JMSXPropertyNames should have at least one name", names.hasMoreElements());
    }
}
