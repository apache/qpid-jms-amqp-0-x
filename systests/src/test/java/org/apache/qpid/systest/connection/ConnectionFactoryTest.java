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
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.ConnectAttemptListener;
import org.apache.qpid.client.ConnectionExtension;
import org.apache.qpid.jms.ConnectionListener;
import org.apache.qpid.protocol.ErrorCodes;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;

public class ConnectionFactoryTest extends JmsTestBase
{
    private static final String BROKER_URL = "tcp://%s:%d%s";
    private static final String CONNECTION_URL = "amqp://%s:%s@clientID/?brokerlist='" + BROKER_URL + "'";
    private String _urlWithCredentials;
    private String _urlWithoutCredentials;
    private String _userName;
    private String _password;

    @Before
    public void setUp()
    {
        final BrokerAdmin brokerAdmin = getBrokerAdmin();
        final InetSocketAddress brokerAddress = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        _userName = brokerAdmin.getValidUsername();
        _password = brokerAdmin.getValidPassword();
        final String host = brokerAddress.getHostString();
        final int port = brokerAddress.getPort();
        _urlWithCredentials = String.format(CONNECTION_URL, _userName, _password, host, port, "");
        _urlWithoutCredentials = String.format(CONNECTION_URL, "", "", host, port, "");
    }

    @Test
    public void testCreateConnectionWithUsernamePasswordSetOnConnectionURL() throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory(_urlWithCredentials);

        AMQConnection con = null;
        try
        {
            con = factory.createConnection();
            assertEquals("Usernames used is different from the one in URL", _userName, con.getUsername());
            assertEquals("Password used is different from the one in URL", _password, con.getPassword());
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }
    }

    @Test
    public void testCreateConnectionWithUsernamePasswordPassedIntoCreateConnection() throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory(_urlWithCredentials);
        AMQConnection con2 = null;
        try
        {
            con2 = factory.createConnection("admin", "admin");
            assertEquals("Usernames used is different from the one in URL", "admin", con2.getUsername());
            assertEquals("Password used is different from the one in URL", "admin", con2.getPassword());
        }
        finally
        {
            if (con2 != null)
            {
                con2.close();
            }
        }
    }


    @Test
    public void testCreateConnectionWithUsernamePasswordPassedIntoCreateConnectionWhenConnectionUrlWithoutCredentials()
            throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory(_urlWithoutCredentials);
        AMQConnection con3 = null;
        try
        {
            con3 = factory.createConnection(_userName, _password);
            assertEquals("Usernames used is different from the one in URL", _userName, con3.getUsername());
            assertEquals("Password used is different from the one in URL", _password, con3.getPassword());
        }
        finally
        {
            if (con3 != null)
            {
                con3.close();
            }
        }
    }

    @Test
    public void testCreatingConnectionWithInstanceMadeUsingDefaultConstructor() throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory();
        factory.setConnectionURLString(_urlWithCredentials);

        AMQConnection con = null;
        try
        {
            con = factory.createConnection();
            assertNotNull(con);
            assertEquals(_userName, con.getUsername());
            assertEquals(_password, con.getPassword());
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }
    }

    @Test
    public void testCreatingConnectionUsingUserNameAndPasswordExtensions() throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory();
        factory.setConnectionURLString(_urlWithoutCredentials);
        factory.setExtension(ConnectionExtension.PASSWORD_OVERRIDE.getExtensionName(), (connection, uri) -> _password);
        factory.setExtension(ConnectionExtension.USERNAME_OVERRIDE.getExtensionName(), (connection, uri) -> _userName);

        AMQConnection con = null;
        try
        {
            con = factory.createConnection();
            assertNotNull(con);
            assertEquals(_userName, con.getUsername());
            assertEquals(_password, con.getPassword());
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }
    }

    @Test
    public void testCreatingConnectionUsingUserNameAndPasswordExtensionsWhenRuntimeExceptionIsThrown() throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory();
        factory.setConnectionURLString(_urlWithoutCredentials);
        factory.setExtension(ConnectionExtension.PASSWORD_OVERRIDE.getExtensionName(), (connection, uri) -> {
            throw new RuntimeException("Test");
        });
        factory.setExtension(ConnectionExtension.USERNAME_OVERRIDE.getExtensionName(), (connection, uri) -> _userName);

        AMQConnection con = null;
        try
        {
            con = factory.createConnection();
            fail("Exception is expected");
        }
        catch (JMSException e)
        {
            // pass
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }
    }

    @Test
    public void testAccountRotationViaConnectAttemptListenerOnConnect() throws Exception
    {
        final BrokerAdmin brokerAdmin = getBrokerAdmin();
        assumeThat("Requires authentication failure for invalid credentials",
                   brokerAdmin.getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

        final AMQConnectionFactory factory = new AMQConnectionFactory(_urlWithoutCredentials);
        final AccountRotatingHelper accountRotatingHelper = new AccountRotatingHelper();
        factory.setConnectAttemptListener(accountRotatingHelper);
        factory.setExtension(ConnectionExtension.PASSWORD_OVERRIDE.getExtensionName(),
                             accountRotatingHelper.getPasswordExtension());
        factory.setExtension(ConnectionExtension.USERNAME_OVERRIDE.getExtensionName(),
                             accountRotatingHelper.getUsernameExtension());
        AMQConnection con = null;
        try
        {
            con = factory.createConnection();
            assertNotNull(con);
            assertEquals(_userName, con.getUsername());
            assertEquals(_password, con.getPassword());
            assertTrue(accountRotatingHelper.getUseValidCredentials());

            final InetSocketAddress brokerAddress = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
            final URI expectedBrokerURI =
                    URI.create(String.format(BROKER_URL, brokerAddress.getHostName(), brokerAddress.getPort(), ""));
            assertEquals(expectedBrokerURI, accountRotatingHelper.getLastUriForFailedlAttempt());
            assertEquals(expectedBrokerURI, accountRotatingHelper.getLastUriForSuccessfulAttempt());
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }
    }

    @Test
    public void testAccountRotationViaConnectAttemptListenerOnFailover() throws Exception
    {
        final BrokerAdmin brokerAdmin = getBrokerAdmin();
        assumeThat("Requires authentication failure for invalid credentials",
                   brokerAdmin.getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

        final InetSocketAddress brokerAddress = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        final String retriesOption = "?retries='1'";
        final String url = String.format(CONNECTION_URL,
                                         "",
                                         "",
                                         brokerAddress.getHostName(),
                                         brokerAddress.getPort(),
                                         retriesOption) + "&failover='singlebroker?cyclecount='1''";

        final AMQConnectionFactory factory = new AMQConnectionFactory(url);

        final AccountRotatingHelper accountRotatingHelper = new AccountRotatingHelper();
        accountRotatingHelper.setUseValidCredentials(true);

        factory.setConnectAttemptListener(accountRotatingHelper);
        factory.setExtension(ConnectionExtension.PASSWORD_OVERRIDE.getExtensionName(),
                             accountRotatingHelper.getPasswordExtension());
        factory.setExtension(ConnectionExtension.USERNAME_OVERRIDE.getExtensionName(),
                             accountRotatingHelper.getUsernameExtension());

        AMQConnection con = null;
        try
        {
            con = factory.createConnection();
            assertNotNull(con);
            assertEquals(_userName, con.getUsername());
            assertEquals(_password, con.getPassword());
            assertEquals(1, accountRotatingHelper.getSuccessfulConnectCount());

            accountRotatingHelper.setUseValidCredentials(false);

            restartBrokerAndWaitForFailover(brokerAdmin, con);
            assertEquals(_userName, con.getUsername());
            assertEquals(_password, con.getPassword());
            assertTrue(accountRotatingHelper.getUseValidCredentials());
            assertEquals(2, accountRotatingHelper.getSuccessfulConnectCount());

            final URI expectedBrokerURI =
                    URI.create(String.format(BROKER_URL,
                                             brokerAddress.getHostName(),
                                             brokerAddress.getPort(),
                                             retriesOption));
            assertEquals(expectedBrokerURI, accountRotatingHelper.getLastUriForFailedlAttempt());
            assertEquals(expectedBrokerURI, accountRotatingHelper.getLastUriForSuccessfulAttempt());
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }
    }

    private void restartBrokerAndWaitForFailover(final BrokerAdmin brokerAdmin, final AMQConnection con)
            throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        final ConnectionListener connectionListener = new ConnectionListener()
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
                return true;
            }

            @Override
            public boolean preResubscribe()
            {
                return false;
            }

            @Override
            public void failoverComplete()
            {
                latch.countDown();
            }
        };
        con.setConnectionListener(connectionListener);

        brokerAdmin.restart();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private class AccountRotatingHelper implements ConnectAttemptListener
    {

        private final AtomicBoolean _useValidCredentials = new AtomicBoolean(false);
        private final AtomicInteger _successCounter = new AtomicInteger();
        private volatile URI _lastUriForSuccessfulAttempt;
        private volatile URI _lastUriForFailedlAttempt;

        @Override
        public boolean connectAttemptFailed(final URI brokerURI, final JMSException e)
        {
            boolean reattempt = String.valueOf(ErrorCodes.CONNECTION_FORCED).equals(e.getErrorCode())
                                || String.valueOf(ErrorCodes.NOT_ALLOWED).equals(e.getErrorCode());
            if (reattempt)
            {
                _useValidCredentials.set(true);
            }
            _lastUriForFailedlAttempt = brokerURI;
            return reattempt;
        }

        @Override
        public void connectAttemptSucceeded(final URI brokerURI)
        {
            _successCounter.incrementAndGet();
            _lastUriForSuccessfulAttempt = brokerURI;
        }

        BiFunction<Connection, URI, Object> getPasswordExtension()
        {
            return (connection, uri) -> _useValidCredentials.get() ? _password : "invalid";
        }

        BiFunction<Connection, URI, Object> getUsernameExtension()
        {
            return (connection, uri) -> _useValidCredentials.get() ? _userName : "invalid";
        }

        void setUseValidCredentials(boolean value)
        {
            _useValidCredentials.set(value);
        }

        boolean getUseValidCredentials()
        {
            return _useValidCredentials.get();
        }

        int getSuccessfulConnectCount()
        {
            return _successCounter.get();
        }

        URI getLastUriForSuccessfulAttempt()
        {
            return _lastUriForSuccessfulAttempt;
        }

        URI getLastUriForFailedlAttempt()
        {
            return _lastUriForFailedlAttempt;
        }
    }

}
