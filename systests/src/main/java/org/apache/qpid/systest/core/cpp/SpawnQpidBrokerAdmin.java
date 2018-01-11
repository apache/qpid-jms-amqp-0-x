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

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.qpid.systest.core.AbstractSpawnQpidBrokerAdmin;
import org.apache.qpid.systest.core.BrokerAdminException;

public class SpawnQpidBrokerAdmin extends AbstractSpawnQpidBrokerAdmin
{
    private static final String SYSTEST_PROPERTY_BROKER_EXECUTABLE = "qpid.systest.broker.executable";
    private static final String BROKER_OUTPUT_LOG_RUNNING = "Broker \\(pid=([0-9]+)\\) running";
    private static final String BROKER_OUTPUT_LOG_SHUT_DOWN = "Broker \\(pid=([0-9]+)\\) shut-down";
    private static final String BROKER_OUTPUT_LOG_LISTENING = "Listening on (TCP/TCP6) port ([0-9]+)";

    @Override
    public boolean supportsPersistence()
    {
        return false;
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
    protected void begin(final Class testClass, final Method method)
    {
        try
        {
            String ready = System.getProperty(SYSTEST_PROPERTY_BROKER_READY_LOG, BROKER_OUTPUT_LOG_RUNNING);
            String stopped = System.getProperty(SYSTEST_PROPERTY_BROKER_STOPPED_LOG, BROKER_OUTPUT_LOG_SHUT_DOWN);
            String amqpListening = System.getProperty(SYSTEST_PROPERTY_BROKER_LISTENING_LOG,
                                                      BROKER_OUTPUT_LOG_LISTENING);
            String process = System.getProperty(SYSTEST_PROPERTY_BROKER_PROCESS_LOG, BROKER_OUTPUT_LOG_RUNNING);
            runBroker(testClass, method, ready, stopped, amqpListening, process);
        }
        catch (IOException e)
        {
            throw new BrokerAdminException("Unexpected exception on broker startup", e);
        }
    }

    @Override
    protected void end(final Class testClass, final Method method)
    {
        shutdownBroker();
    }

    @Override
    protected ProcessBuilder createBrokerProcessBuilder(final String workDirectory, final Class testClass)
            throws IOException
    {
        String[] cmd = new String[]{
                System.getProperty(SYSTEST_PROPERTY_BROKER_EXECUTABLE, "qpidd"),
                "-p",
                "0",
                "--data-dir",
                escapePath(workDirectory),
                "-t",
                "--auth",
                "no",
                "--no-module-dir"
        };

        return new ProcessBuilder(cmd);
    }
}
