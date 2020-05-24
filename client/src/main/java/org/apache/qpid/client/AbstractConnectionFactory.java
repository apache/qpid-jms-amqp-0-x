/*
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

package org.apache.qpid.client;

import java.net.URI;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiFunction;

import javax.jms.Connection;

import org.apache.qpid.QpidException;
import org.apache.qpid.jms.ConnectionURL;

public abstract class AbstractConnectionFactory
{
    private final Map<ConnectionExtension, BiFunction<Connection, URI, Object>>
            _extensions = new EnumMap<>(ConnectionExtension.class);
    private volatile ConnectAttemptListener _connectAttemptListener;


    public void setExtension(String extensionName, BiFunction<Connection, URI, Object> extension)
    {
        final ConnectionExtension connectionExtension = ConnectionExtension.fromString(extensionName);
        if (extension == null)
        {
            _extensions.remove(connectionExtension);
        }
        else
        {
            _extensions.put(connectionExtension, extension);
        }
    }

    public void setConnectAttemptListener(final ConnectAttemptListener connectAttemptListener)
    {
        _connectAttemptListener = connectAttemptListener;
    }

    protected CommonConnection newConnectionInstance(final ConnectionURL connectionDetails) throws QpidException
    {
        return newAMQConnectionInstance(connectionDetails);
    }

    protected Map<ConnectionExtension, BiFunction<Connection, URI, Object>> getExtensions()
    {
        return new EnumMap<>(_extensions);
    }

    final AMQConnection newAMQConnectionInstance(final ConnectionURL connectionDetails) throws QpidException
    {
        final Map<ConnectionExtension, BiFunction<Connection, URI, Object>> extensions = getExtensions();
        final ConnectionURL connectionURL =
                extensions.isEmpty() ? connectionDetails : new ExtensibleConnectionURL(connectionDetails, extensions);
        final AMQConnection connection = new AMQConnection(connectionURL, _connectAttemptListener);
        if (connectionURL instanceof ExtensibleConnectionURL)
        {
            ((ExtensibleConnectionURL) connectionURL).setConnectionSupplier(() -> connection);
        }
        return connection;
    }
}
