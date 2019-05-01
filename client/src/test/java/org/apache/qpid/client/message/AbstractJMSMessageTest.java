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
package org.apache.qpid.client.message;


import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;

import org.mockito.ArgumentMatcher;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentBody;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.test.utils.QpidTestCase;

public class AbstractJMSMessageTest extends QpidTestCase
{

    private final AMQSession<?,?> _session = mock(AMQSession.class);
    private final MessageFactoryRegistry _messageFactoryRegistry = MessageFactoryRegistry.newDefaultRegistry(_session);

    public void testIncoming08ReplyTo() throws Exception
    {
        when(_session.isQueueBound(isNull(String.class), eq("knownQueue"), isNull(String.class))).thenReturn(true);
        when(_session.isExchangeExist(isDestinationWithAddress("knownExchange"), anyBoolean())).thenReturn(true);

        doReplyToTest("direct://amq.direct/knownQueue?routingkey='knownQueue'", "direct://amq.direct/knownQueue/knownQueue?routingkey='knownQueue'");
        doReplyToTest("knownQueue", "direct:///knownQueue/knownQueue?routingkey='knownQueue'");
        doReplyToTest("knownExchange", "direct://knownExchange//?routingkey=''");
        doReplyToTest("news-service/sports", "direct://news-service/sports/sports?routingkey='sports'");
    }

    public void testSetNullJMSReplyTo08() throws JMSException
    {
        JMSTextMessage message = new JMSTextMessage(AMQMessageDelegateFactory.FACTORY_0_8);
        try
        {
            message.setJMSReplyTo(null);
        }
        catch (IllegalArgumentException e)
        {
            fail("Null destination should be allowed");
        }
    }

    public void testSetNullJMSReplyTo10() throws JMSException
    {
        JMSTextMessage message = new JMSTextMessage(AMQMessageDelegateFactory.FACTORY_0_10);
        try
        {
            message.setJMSReplyTo(null);
        }
        catch (IllegalArgumentException e)
        {
            fail("Null destination should be allowed");
        }
    }

    private void doReplyToTest(final String headerReplyTo, final String expectedReplyToAddress)
            throws QpidException, JMSException
    {
        final ContentHeaderBody contentHeader = new ContentHeaderBody(new BasicContentHeaderProperties());
        contentHeader.getProperties().setReplyTo(headerReplyTo);

        final List<ContentBody> contentBodies = new ArrayList<>();
        final AbstractJMSMessage message = _messageFactoryRegistry.createMessage(0, false,
                                                                                 "amq.direct",
                                                                                 "routingKey",
                                                                                 contentHeader,
                                                                                 contentBodies,
                                                                                 null,
                                                                                 null,
                                                                                 0);
        message.setAMQSession(_session);

        assertNotNull(message.getJMSReplyTo());
        assertEquals(expectedReplyToAddress, message.getJMSReplyTo().toString());
    }

    private AMQDestination isDestinationWithAddress(final String expectedAddress)
    {
        return argThat( new ArgumentMatcher<AMQDestination>()
        {
            @Override
            public boolean matches(AMQDestination argument)
            {
                return argument.getAddressName().equals(expectedAddress);
            }
        });
    }
}
