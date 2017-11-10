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
package org.apache.qpid.framing;

import java.nio.ByteBuffer;

import org.apache.qpid.QpidException;
import org.apache.qpid.protocol.AMQVersionAwareProtocolSession;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.util.ByteBufferUtils;

public class ContentBody implements AMQBody
{
    public static final byte TYPE = 3;

    private ByteBuffer _payload;


    public ContentBody(ByteBuffer payload)
    {
        _payload = payload.duplicate();
    }

    public byte getFrameType()
    {
        return TYPE;
    }

    public int getSize()
    {
        return _payload == null ? 0 : _payload.remaining();
    }

    public void handle(final int channelId, final AMQVersionAwareProtocolSession session)
            throws QpidException
    {
        session.contentBodyReceived(channelId, this);
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {
        if(_payload != null)
        {
            sender.send(_payload.duplicate());
            return _payload.remaining();
        }
        else
        {
            return 0l;
        }
    }

    public ByteBuffer getPayload()
    {
        return _payload;
    }

    public void dispose()
    {
        if (_payload != null)
        {
            _payload = null;
        }
    }

    public static void process(final ByteBuffer in,
                               final ChannelMethodProcessor methodProcessor, final long bodySize)
    {
        ByteBuffer payload = ByteBufferUtils.view(in, 0, (int) bodySize);

        if(!methodProcessor.ignoreAllButCloseOk())
        {
            methodProcessor.receiveMessageContent(payload);
        }

        in.position(in.position()+(int)bodySize);
    }

    public static AMQFrame createAMQFrame(int channelId, ContentBody body)
    {
        return new AMQFrame(channelId, body);
    }
}
