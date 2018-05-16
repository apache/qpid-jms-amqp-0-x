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

package org.apache.qpid.systest.core.cpp;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.qpid.systest.core.AbstractSpawnQpidBrokerAdmin;
import org.apache.qpid.systest.core.BrokerAdminException;
import org.apache.qpid.systest.core.LogConsumer;

public class SpawnQpidBrokerAdmin extends AbstractSpawnQpidBrokerAdmin
{
    private static final String SYSTEST_PROPERTY_BROKER_EXECUTABLE = "qpid.systest.broker.executable";
    private static final String SYSTEST_PROPERTY_BROKER_MODULE_DIR = "qpid.systest.broker.moduleDir";
    private static final String SYSTEST_PROPERTY_BROKER_STORE_INITIALIZED = "qpid.systest.broker.storeInitialized";
    private static final String BROKER_OUTPUT_LOG_RUNNING = "Broker \\(pid=([0-9]+)\\) running";
    private static final String BROKER_OUTPUT_LOG_SHUT_DOWN = "Broker \\(pid=([0-9]+)\\) shut-down";
    private static final String BROKER_OUTPUT_STORE_INITIALIZED = "Store module initialized";
    private static final String BROKER_OUTPUT_LOG_LISTENING = "Listening on (TCP/TCP6) port ([0-9]+)";
    private final String _storeInitalised;
    private final String _moduleDir;
    private final String _ready;
    private final String _stopped;
    private final String _amqpListening;
    private final String _process;
    private volatile String _workingDirectory;
    private boolean _supportsPersistence = false;
    private int _previousPort = 0;

    public SpawnQpidBrokerAdmin()
    {
        _storeInitalised = System.getProperty(SYSTEST_PROPERTY_BROKER_STORE_INITIALIZED, BROKER_OUTPUT_STORE_INITIALIZED);
        _moduleDir = System.getProperty(SYSTEST_PROPERTY_BROKER_MODULE_DIR);
        _ready = System.getProperty(SYSTEST_PROPERTY_BROKER_READY_LOG, BROKER_OUTPUT_LOG_RUNNING);
        _stopped = System.getProperty(SYSTEST_PROPERTY_BROKER_STOPPED_LOG, BROKER_OUTPUT_LOG_SHUT_DOWN);
        _amqpListening = System.getProperty(SYSTEST_PROPERTY_BROKER_LISTENING_LOG,
                                                  BROKER_OUTPUT_LOG_LISTENING);
        _process = System.getProperty(SYSTEST_PROPERTY_BROKER_PROCESS_LOG, BROKER_OUTPUT_LOG_RUNNING);
    }


    @Override
    public boolean supportsPersistence()
    {
        return _supportsPersistence;
    }

    @Override
    public String getValidUsername()
    {
        return "";
    }

    @Override
    public String getValidPassword()
    {
        return "";
    }

    @Override
    public String getVirtualHostName()
    {
        return "";
    }

    @Override
    public BrokerType getBrokerType()
    {
        return BrokerType.CPP;
    }


    @Override
    protected void setUp(final Class testClass)
    {
    }

    @Override
    protected void cleanUp(final Class testClass)
    {
    }

    @Override
    public LogConsumer getLogConsumer()
    {
        final LogConsumer superConsumer = super.getLogConsumer();
        return new LogConsumer()
        {
            @Override
            public void accept(final String line)
            {
                superConsumer.accept(line);
                if (line != null && line.contains(_storeInitalised))
                {
                    _supportsPersistence = true;
                }
            }
        };
    }

    @Override
    protected void begin(final Class testClass, final Method method)
    {
        _workingDirectory = getWorkingDirectory(testClass, method);
        doRunBroker(testClass, method);
    }

    private void doRunBroker(final Class testClass, final Method method)
    {
        try
        {
            runBroker(testClass, method, _ready, _stopped, _amqpListening, _process, _workingDirectory);
        }
        catch (IOException e)
        {
            throw new BrokerAdminException("Unexpected exception on broker startup", e);
        }
    }

    @Override
    public void restart()
    {
        try
        {
            _previousPort = getBrokerAddress(PortType.AMQP).getPort();
        }
        catch (IllegalArgumentException e)
        {
            _previousPort = 0;
        }

        try
        {
            shutdownBroker();
            doRunBroker(_currentTestClass, _currentTestMethod);
        }
        finally
        {
            _previousPort = 0;
        }
    }

    @Override
    protected void end(final Class testClass, final Method method)
    {
        shutdownBroker();
        cleanWorkDirectory(_workingDirectory);
        _workingDirectory = null;
    }

    @Override
    protected ProcessBuilder createBrokerProcessBuilder(final String workDirectory,
                                                        final Class testClass)
    {
        List<String> cmd = new ArrayList<>(Arrays.asList(
                System.getProperty(SYSTEST_PROPERTY_BROKER_EXECUTABLE, "qpidd"),
                "-p",
                String.format("%d", _previousPort),
                "--data-dir",
                escapePath(workDirectory),
                "-t",
                "--auth",
                "no"));

        if (_moduleDir != null && _moduleDir.length() > 0  && new File(_moduleDir).isDirectory())
        {
            cmd.add("--module-dir");
            cmd.add(escapePath(_moduleDir));
        }
        else
        {
            cmd.add("--no-module-dir");
        }

        return new ProcessBuilder(cmd);
    }
}
