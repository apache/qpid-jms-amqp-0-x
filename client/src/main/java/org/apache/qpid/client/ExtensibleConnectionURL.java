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
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import javax.jms.Connection;

import org.apache.qpid.QpidException;
import org.apache.qpid.jms.ConnectionURL;

public class ExtensibleConnectionURL implements ConnectionURL
{
    private final ConnectionURL _connectionURL;
    private final Map<ConnectionExtension, BiFunction<Connection, URI, Object>> _extensions;
    private final URI _connectionURI;
    private volatile Supplier<AMQConnection> _connectionSupplier;

    ExtensibleConnectionURL(final ConnectionURL connectionURL,
                            final Map<ConnectionExtension, BiFunction<Connection, URI, Object>> extensions)
            throws QpidException
    {
        this(connectionURL, extensions, () -> null);
    }

    ExtensibleConnectionURL(final ConnectionURL connectionURL,
                            final Map<ConnectionExtension, BiFunction<Connection, URI, Object>> extensions,
                            final Supplier<AMQConnection> connectionSupplier) throws QpidException
    {
        _connectionURL = connectionURL;
        _extensions = extensions;
        _connectionSupplier = connectionSupplier;

        try
        {
            _connectionURI = new URI(_connectionURL.getURL());
        }
        catch (URISyntaxException e)
        {
            throw new QpidException("Unexpected connection URL", e);
        }
    }

    @Override
    public String getURL()
    {
        return _connectionURL.getURL();
    }

    @Override
    public String getFailoverMethod()
    {
        return _connectionURL.getFailoverMethod();
    }

    @Override
    public String getFailoverOption(final String key)
    {
        return _connectionURL.getFailoverOption(key);
    }

    @Override
    public int getBrokerCount()
    {
        return _connectionURL.getBrokerCount();
    }

    @Override
    public BrokerDetails getBrokerDetails(final int index)
    {
        return _connectionURL.getBrokerDetails(index);
    }

    @Override
    public void addBrokerDetails(final BrokerDetails broker)
    {
        _connectionURL.addBrokerDetails(broker);
    }

    @Override
    public void setBrokerDetails(final List<BrokerDetails> brokers)
    {
        _connectionURL.setBrokerDetails(brokers);
    }

    @Override
    public List<BrokerDetails> getAllBrokerDetails()
    {
        return _connectionURL.getAllBrokerDetails();
    }

    @Override
    public String getClientName()
    {
        return _connectionURL.getClientName();
    }

    @Override
    public void setClientName(final String clientName)
    {
        _connectionURL.setClientName(clientName);
    }

    @Override
    public String getUsername()
    {
        if (_extensions.containsKey(ConnectionExtension.USERNAME_OVERRIDE))
        {
            final BiFunction<Connection, URI, Object> extension =
                    _extensions.get(ConnectionExtension.USERNAME_OVERRIDE);
            final Object userName = extension.apply(_connectionSupplier.get(), _connectionURI);
            return userName == null ? null : String.valueOf(userName);
        }
        else
        {
            return _connectionURL.getUsername();
        }
    }

    @Override
    public void setUsername(final String username)
    {
        _connectionURL.setUsername(username);
    }

    @Override
    public String getPassword()
    {
        if (_extensions.containsKey(ConnectionExtension.PASSWORD_OVERRIDE))
        {
            final BiFunction<Connection, URI, Object> extension =
                    _extensions.get(ConnectionExtension.PASSWORD_OVERRIDE);
            final Object password = extension.apply(_connectionSupplier.get(), _connectionURI);
            return password == null ? null : String.valueOf(password);
        }
        else
        {
            return _connectionURL.getPassword();
        }
    }

    @Override
    public void setPassword(final String password)
    {
        _connectionURL.setPassword(password);
    }

    @Override
    public String getVirtualHost()
    {
        return _connectionURL.getVirtualHost();
    }

    @Override
    public void setVirtualHost(final String virtualHost)
    {
        _connectionURL.setVirtualHost(virtualHost);
    }

    @Override
    public String getOption(final String key)
    {
        return _connectionURL.getOption(key);
    }

    @Override
    public void setOption(final String key, final String value)
    {
        _connectionURL.setOption(key, value);
    }

    @Override
    public String getDefaultQueueExchangeName()
    {
        return _connectionURL.getDefaultQueueExchangeName();
    }

    @Override
    public String getDefaultTopicExchangeName()
    {
        return _connectionURL.getDefaultTopicExchangeName();
    }

    @Override
    public String getTemporaryQueueExchangeName()
    {
        return _connectionURL.getTemporaryQueueExchangeName();
    }

    @Override
    public String getTemporaryTopicExchangeName()
    {
        return _connectionURL.getTemporaryTopicExchangeName();
    }

    @Override
    public String toString()
    {
        return _connectionURL.toString();
    }

    void setConnectionSupplier(final Supplier<AMQConnection> connectionSupplier)
    {
        _connectionSupplier = connectionSupplier;
    }
}
