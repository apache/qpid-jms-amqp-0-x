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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import javax.jms.JMSException;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.client.ConnectionExtension;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;

public class ConnectionFactoryTest extends JmsTestBase
{
    private static final String CONNECTION_URL = "amqp://%s:%s@clientID/?brokerlist='tcp://%s:%d'";
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
        _urlWithCredentials = String.format(CONNECTION_URL, _userName, _password, host, port);
        _urlWithoutCredentials = String.format(CONNECTION_URL, "", "", host, port);
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
}
