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

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.ConnectionTuneParameters;
import org.apache.qpid.client.protocol.AMQProtocolSession;
import org.apache.qpid.client.state.AMQState;
import org.apache.qpid.client.state.StateAwareMethodListener;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.ConnectionOpenBody;
import org.apache.qpid.framing.ConnectionTuneBody;
import org.apache.qpid.framing.ConnectionTuneOkBody;
import org.apache.qpid.framing.MethodRegistry;

public class ConnectionTuneMethodHandler implements StateAwareMethodListener<ConnectionTuneBody>
{
    private static final Logger _logger = LoggerFactory.getLogger(ConnectionTuneMethodHandler.class);

    private static final ConnectionTuneMethodHandler _instance = new ConnectionTuneMethodHandler();

    public static ConnectionTuneMethodHandler getInstance()
    {
        return _instance;
    }

    protected ConnectionTuneMethodHandler()
    { }

    public void methodReceived(AMQProtocolSession session, ConnectionTuneBody frame, int channelId) throws QpidException
    {
        _logger.debug("ConnectionTune frame received");

        verifySaslNegotiationComplete(session);

        final MethodRegistry methodRegistry = session.getMethodRegistry();

        ConnectionTuneParameters params = session.getConnectionTuneParameters();

        int maxChannelNumber = frame.getChannelMax();
        //0 implies no limit, except that forced by protocol limitations (0xFFFF)
        params.setChannelMax(maxChannelNumber == 0 ? AMQProtocolSession.MAX_CHANNEL_MAX : maxChannelNumber);
        params.setFrameMax(frame.getFrameMax());

        //if the heart beat delay hasn't been configured, we use the broker-supplied value
        if (params.getHeartbeat() == null)
        {
            params.setHeartbeat(frame.getHeartbeat());
        }

        session.tuneConnection(params);

        session.getStateManager().changeState(AMQState.CONNECTION_NOT_OPENED);

        ConnectionTuneOkBody tuneOkBody = methodRegistry.createConnectionTuneOkBody(params.getChannelMax(),
                                                                                    params.getFrameMax(),
                                                                                    params.getHeartbeat());

        session.setMaxFrameSize(params.getFrameMax());
        // Be aware of possible changes to parameter order as versions change.
        session.writeFrame(tuneOkBody.generateFrame(channelId));

        String host = session.getAMQConnection().getVirtualHost();
        AMQShortString virtualHost = new AMQShortString("/" + host);

        ConnectionOpenBody openBody = methodRegistry.createConnectionOpenBody(virtualHost,null,true);

        // Be aware of possible changes to parameter order as versions change.
        session.writeFrame(openBody.generateFrame(channelId));
    }

    private void verifySaslNegotiationComplete(final AMQProtocolSession session) throws QpidException
    {
        SaslClient client = session.getSaslClient();
        if (client == null)
        {
            throw new QpidException("No SASL client set up - cannot proceed with connection open");
        }

        if (!client.isComplete())
        {
            throw new QpidException("SASL negotiation has not been completed - cannot proceed with connection open");
        }

        String mechanismName = client.getMechanismName();
        try
        {
            client.dispose();
        }
        catch (SaslException e)
        {
            _logger.warn("Disposal of client sasl for mechanism '{}' has failed", mechanismName, e);
        }
        finally
        {
            session.setSaslClient(null);
        }
    }
}
