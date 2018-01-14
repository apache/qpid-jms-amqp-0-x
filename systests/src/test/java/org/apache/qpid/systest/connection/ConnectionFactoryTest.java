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

import java.net.InetSocketAddress;

import javax.jms.Connection;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;

public class ConnectionFactoryTest extends JmsTestBase
{
    private static final String CONNECTION_URL = "amqp://guest:guest@clientID/?brokerlist='tcp://%s:%d'";
    private String _url;

    @Before
    public void setUp()
    {
        final InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        _url = String.format(CONNECTION_URL,
                             brokerAddress.getHostString(),
                             brokerAddress.getPort());
    }
    /**
     * The username & password specified should not override the default
     * specified in the URL.
     */
    @Test
    public void testCreateConnectionWithUsernamePassword() throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory(_url);

        AMQConnection con = null;
        try
        {
            con = factory.createConnection();
            assertEquals("Usernames used is different from the one in URL","guest",con.getConnectionURL().getUsername());
            assertEquals("Password used is different from the one in URL","guest",con.getConnectionURL().getPassword());
        }
        finally
        {
            if (con != null)
            {
                con.close();
            }
        }

        AMQConnection con2 = null;
        try
        {
            con2 = factory.createConnection("user", "pass");
            assertEquals("Usernames used is different from the one in URL","user",con2.getConnectionURL().getUsername());
            assertEquals("Password used is different from the one in URL","pass",con2.getConnectionURL().getPassword());
        }
        catch(Exception e)
        {
            // ignore
        }
        finally
        {
            if (con2 != null)
            {
                con2.close();
            }
        }

        AMQConnection con3 = null;
        try
        {
            con3 = factory.createConnection();
            assertEquals("Usernames used is different from the one in URL","guest",con3.getConnectionURL().getUsername());
            assertEquals("Password used is different from the one in URL","guest",con3.getConnectionURL().getPassword());
        }
        finally
        {
            if (con3 != null)
            {
                con3.close();
            }
        }
    }

    /**
     * Verifies that a connection can be made using an instance of AMQConnectionFactory created with the
     * default constructor and provided with the connection url via setter.
     */
    @Test
    public void testCreatingConnectionWithInstanceMadeUsingDefaultConstructor() throws Exception
    {
        AMQConnectionFactory factory = new AMQConnectionFactory();
        factory.setConnectionURLString(_url);

        Connection con = null;
        try
        {
            con = factory.createConnection();
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
