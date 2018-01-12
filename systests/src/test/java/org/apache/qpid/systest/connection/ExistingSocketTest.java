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

import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.net.Socket;

import javax.jms.Connection;

import org.junit.Test;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.transport.network.io.IoNetworkTransport;

public class ExistingSocketTest extends JmsTestBase
{
    private static final String SOCKET_NAME = "mysock";
    private static final String CONNECTION_URL_TEMPLATE = "amqp://%s:%s@/%s?brokerlist='socket://%s'";

    @Test
    public void testExistingSocket_SuccessfulConnection() throws Exception
    {
        BrokerAdmin brokerAdmin = getBrokerAdmin();
        InetSocketAddress brokerAddress = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        try (Socket sock = new Socket("localhost", brokerAddress.getPort()))
        {

            IoNetworkTransport.registerOpenSocket(SOCKET_NAME, sock);

            String url = String.format(CONNECTION_URL_TEMPLATE,
                                       brokerAdmin.getValidUsername(),
                                       brokerAdmin.getValidUsername(),
                                       brokerAdmin.getVirtualHostName(),
                                       SOCKET_NAME);

            Connection conn = new AMQConnection(url);
            conn.createSession(true, javax.jms.Session.SESSION_TRANSACTED);
            conn.close();
        }
    }

    @Test
    public void testExistingSocket_UnknownSocket() throws Exception
    {
        final Object unknownSockName = "unknownSock";

        BrokerAdmin brokerAdmin = getBrokerAdmin();

        String url = String.format(CONNECTION_URL_TEMPLATE,
                                   brokerAdmin.getValidUsername(),
                                   brokerAdmin.getValidUsername(),
                                   brokerAdmin.getVirtualHostName(),
                                   unknownSockName);

        try
        {
            new AMQConnection(url);
        }
        catch (Exception e)
        {
            String expected = String.format("No socket registered with id '%s'",
                                            unknownSockName);
            assertTrue("Unexpected exception message : " + e.getMessage(), e.getMessage().contains(expected));
        }
    }
}
