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
package org.apache.qpid.client.handler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.security.sasl.SaslClient;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.ConnectionTuneParameters;
import org.apache.qpid.client.MockAMQConnection;
import org.apache.qpid.client.protocol.AMQProtocolSession;
import org.apache.qpid.client.state.AMQState;
import org.apache.qpid.client.state.AMQStateManager;
import org.apache.qpid.framing.ConnectionTuneBody;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.test.utils.QpidTestCase;

public class ConnectionTuneMethodHandlerTest extends QpidTestCase
{
    private static final int CHANNEL_ID = 0;
    private AMQProtocolSession _amqProtocolSession;
    private ConnectionTuneBody _body;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _amqProtocolSession = mock(AMQProtocolSession.class);
        _body = mock(ConnectionTuneBody.class);
    }

    public void testMethodReceived_SaslClientIsNotFound() throws Exception
    {
        ConnectionTuneMethodHandler handler = ConnectionTuneMethodHandler.getInstance();
        try
        {
            handler.methodReceived(_amqProtocolSession, _body, CHANNEL_ID);
            fail("Exception is not thrown");
        }
        catch (QpidException e)
        {
            // pass
        }
    }

    public void testMethodReceived_SaslNegotiationIsNotComplete() throws Exception
    {
        ConnectionTuneMethodHandler handler = ConnectionTuneMethodHandler.getInstance();
        try
        {
            final SaslClient saslClient = mock(SaslClient.class);
            when(saslClient.isComplete()).thenReturn(false);
            when(_amqProtocolSession.getSaslClient()).thenReturn(saslClient);
            handler.methodReceived(_amqProtocolSession, _body, CHANNEL_ID);
            fail("Exception is not thrown");
        }
        catch (QpidException e)
        {
            // pass
        }
    }

    public void testMethodReceived_SaslNegotiationIsComplete() throws Exception
    {
        ConnectionTuneMethodHandler handler = ConnectionTuneMethodHandler.getInstance();
        final SaslClient saslClient = mock(SaslClient.class);
        when(saslClient.isComplete()).thenReturn(true);
        final AMQStateManager stateManager = mock(AMQStateManager.class);
        ConnectionTuneParameters parameters = new ConnectionTuneParameters();
        final AMQConnection connection = new MockAMQConnection("loaclhost", "test", "test", "client", "virtualhost");
        final MethodRegistry methodRegistry = new MethodRegistry(ProtocolVersion.getLatestSupportedVersion());
        when(_amqProtocolSession.getSaslClient()).thenReturn(saslClient);
        when(_amqProtocolSession.getStateManager()).thenReturn(stateManager);
        when(_amqProtocolSession.getMethodRegistry()).thenReturn(methodRegistry);
        when(_amqProtocolSession.getConnectionTuneParameters()).thenReturn(parameters);
        when(_amqProtocolSession.getAMQConnection()).thenReturn(connection);

        handler.methodReceived(_amqProtocolSession, _body, CHANNEL_ID);
        verify(stateManager, times(1)).changeState(AMQState.CONNECTION_NOT_OPENED);
        verify(saslClient, times(1)).isComplete();
    }
}
