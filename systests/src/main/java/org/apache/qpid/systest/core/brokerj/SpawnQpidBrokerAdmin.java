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

package org.apache.qpid.systest.core.brokerj;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systest.core.AbstractSpawnQpidBrokerAdmin;
import org.apache.qpid.systest.core.BrokerAdminException;
import org.apache.qpid.systest.core.dependency.ClasspathQuery;
import org.apache.qpid.systest.core.logback.LogbackSocketPortNumberDefiner;

public class SpawnQpidBrokerAdmin extends AbstractSpawnQpidBrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpawnQpidBrokerAdmin.class);
    private static final String BROKER_LOG_PREFIX = "BRK";
    private static final String SYSTEST_PROPERTY_PREFIX = "qpid.systest.";
    private static final String SYSTEST_PROPERTY_JAVA_EXECUTABLE = "qpid.systest.java8.executable";
    private static final String SYSTEST_PROPERTY_LOGBACK_CONTEXT = "qpid.systest.logback.context";
    private static final String SYSTEST_PROPERTY_REMOTE_DEBUGGER = "qpid.systest.remote_debugger";
    private static final String SYSTEST_PROPERTY_LOGBACK_ORIGIN = "qpid.systest.logback.origin";
    private static final String SYSTEST_PROPERTY_LOGBACK_SOCKET_PORT = "qpid.systest.logback.socket.port";

    private static final String BROKER_TYPE_LOGBACK_SOCKET_LOGGER =
            "org.apache.qpid.server.logging.logback.BrokerLogbackSocketLogger";
    private static final String BROKER_TYPE_NAME_AND_LEVEL_LOG_INCLUSION_RULE =
            "org.apache.qpid.server.logging.logback.BrokerNameAndLevelLogInclusionRule";
    private static final String BROKER_TYPE_VIRTUAL_HOST_NODE = "org.apache.qpid.VirtualHostNode";

    static final String SYSTEST_PROPERTY_VIRTUALHOSTNODE_TYPE = "qpid.systest.virtualhostnode.type";
    static final String SYSTEST_PROPERTY_VIRTUALHOST_BLUEPRINT = "qpid.systest.virtualhost.blueprint";
    static final String SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION = "qpid.systest.initialConfigurationLocation";
    static final String SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE = "qpid.systest.build.classpath.file";
    static final String SYSTEST_PROPERTY_BROKERJ_DEPENDENCIES = "qpid.systest.brokerj.dependencies";

    private volatile boolean _isPersistentStore;
    private volatile String _virtualHostNodeName;
    private volatile String _workingDirectory;

    @Override
    public boolean supportsPersistence()
    {
        return _isPersistentStore;
    }

    @Override
    public String getValidUsername()
    {
        return "guest";
    }

    @Override
    public String getValidPassword()
    {
        return "guest";
    }

    @Override
    public String getVirtualHostName()
    {
        return _virtualHostNodeName;
    }

    @Override
    public BrokerType getBrokerType()
    {
        return BrokerType.BROKERJ;
    }

    @Override
    public void restart()
    {
        if (_virtualHostNodeName == null)
        {
            throw new BrokerAdminException("Virtual host is not started");
        }
        restartVirtualHost(_virtualHostNodeName);
    }

    @Override
    public void stop()
    {
        if (_virtualHostNodeName == null)
        {
            throw new BrokerAdminException("Virtual host is not started");
        }
        stopVirtualHost(_virtualHostNodeName);
    }

    @Override
    protected void setUp(final Class testClass)
    {
        try
        {
            String ready = System.getProperty(SYSTEST_PROPERTY_BROKER_READY_LOG, "BRK-1004 : Qpid Broker Ready");
            String stopped = System.getProperty(SYSTEST_PROPERTY_BROKER_STOPPED_LOG, "BRK-1005 : Stopped");
            String amqpListening = System.getProperty(SYSTEST_PROPERTY_BROKER_LISTENING_LOG,
                                                      "BRK-1002 : Starting : Listening on (\\w*) port ([0-9]+)");
            String process = System.getProperty(SYSTEST_PROPERTY_BROKER_PROCESS_LOG, "BRK-1017 : Process : PID : ([0-9]+)");
            _workingDirectory = getWorkingDirectory(testClass, null);
            runBroker(testClass, null, ready, stopped, amqpListening, process, _workingDirectory);
        }
        catch (IOException e)
        {
            throw new BrokerAdminException("Unexpected exception on broker startup", e);
        }
    }

    @Override
    protected void cleanUp(final Class testClass)
    {
        shutdownBroker();
        cleanWorkDirectory(_workingDirectory);
        _workingDirectory = null;
    }

    @Override
    protected void setClassQualifiedTestName(final Class testClass, final Method method)
    {
        if (testClass != null)
        {
            String qualifiedTestName;
            String loggerName;
            String logLevel;
            if (method == null)
            {
                qualifiedTestName = testClass.getName();
                loggerName = testClass.getSimpleName();
                logLevel = "INFO";
            }
            else
            {
                logLevel = "DEBUG";
                qualifiedTestName = String.format("%s.%s", testClass.getName(), method.getName());
                loggerName = method.getName();
            }
            createBrokerSocketLoggerAndRulesAndDeleteOldLogger(loggerName, qualifiedTestName, logLevel);
        }
        super.setClassQualifiedTestName(testClass, method);
    }

    @Override
    protected void begin(final Class testClass, final Method method)
    {
        LOGGER.info("========================= prepare test environment for test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
        String virtualHostNodeName = getVirtualHostNodeName(testClass, method);
        createVirtualHost(virtualHostNodeName);
        _virtualHostNodeName = virtualHostNodeName;
        _isPersistentStore = !"Memory".equals(getNodeType());
    }

    @Override
    protected void end(final Class testClass, final Method method)
    {
        deleteVirtualHost(getVirtualHostNodeName(testClass, method));
        _virtualHostNodeName = null;
        _isPersistentStore = false;
    }

    @Override
    protected ProcessBuilder createBrokerProcessBuilder(String _currentWorkDirectory, final Class testClass) throws IOException
    {
        String initialConfiguration = System.getProperty(SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION);
        if (initialConfiguration == null)
        {
            throw new BrokerAdminException(
                    String.format("No initial configuration is found: JVM property '%s' is not set.",
                                  SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION));
        }

        File testInitialConfiguration = new File(_currentWorkDirectory, "initial-configuration.json");
        if (!testInitialConfiguration.createNewFile())
        {
            throw new BrokerAdminException("Failed to create a file for a copy of initial configuration");
        }
        if (initialConfiguration.startsWith("classpath:"))
        {
            String config = initialConfiguration.substring("classpath:".length());
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(config);
                 OutputStream os = new FileOutputStream(testInitialConfiguration))
            {
                ByteStreams.copy(is, os);
            }
        }
        else
        {
            Files.copy(new File(initialConfiguration).toPath(), testInitialConfiguration.toPath());
        }

        String classpath;
        File file = new File(System.getProperty(SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE));
        if (!file.exists())
        {
            String dependencies = System.getProperty(SYSTEST_PROPERTY_BROKERJ_DEPENDENCIES);
            final ClasspathQuery classpathQuery = new ClasspathQuery(SpawnQpidBrokerAdmin.class,
                                                                     Arrays.asList(dependencies.split(",")));
            classpath = classpathQuery.getClasspath();
            Files.write(file.toPath(), Collections.singleton(classpath), UTF_8);
        }
        else
        {
            classpath = new String(Files.readAllBytes(file.toPath()), UTF_8);
        }

        // grab Qpid related JVM settings
        List<String> jvmArguments = new ArrayList<>();
        Properties jvmProperties = System.getProperties();
        for (String jvmProperty : jvmProperties.stringPropertyNames())
        {
            if (jvmProperty.startsWith(SYSTEST_PROPERTY_PREFIX)
                || jvmProperty.equalsIgnoreCase("java.io.tmpdir"))
            {
                jvmArguments.add(String.format("-D%s=%s", jvmProperty, jvmProperties.getProperty(jvmProperty)));
            }
        }

        jvmArguments.add(0, System.getProperty(SYSTEST_PROPERTY_JAVA_EXECUTABLE, "java"));
        jvmArguments.add(1, "-cp");
        jvmArguments.add(2, classpath);
        jvmArguments.add(String.format("-D%s=%d",
                                       SYSTEST_PROPERTY_LOGBACK_SOCKET_PORT,
                                       LogbackSocketPortNumberDefiner.getLogbackSocketPortNumber()));
        jvmArguments.add(String.format("-D%s=%s-%s",
                                       SYSTEST_PROPERTY_LOGBACK_ORIGIN,
                                       BROKER_LOG_PREFIX,
                                       testClass.getSimpleName()));
        jvmArguments.add(String.format("-D%s=%s", SYSTEST_PROPERTY_LOGBACK_CONTEXT, testClass.getName()));
        if (System.getProperty(SYSTEST_PROPERTY_REMOTE_DEBUGGER) != null)
        {
            jvmArguments.add(System.getProperty(SYSTEST_PROPERTY_REMOTE_DEBUGGER));
        }
        jvmArguments.add("org.apache.qpid.server.Main");
        jvmArguments.add("-prop");
        jvmArguments.add(String.format("qpid.work_dir=%s", escapePath(_currentWorkDirectory)));
        jvmArguments.add("--store-type");
        jvmArguments.add("JSON");
        jvmArguments.add("--initial-config-path");
        jvmArguments.add(escapePath(testInitialConfiguration.toString()));

        LOGGER.debug("Spawning broker JVM :", jvmArguments);
        String[] cmd = jvmArguments.toArray(new String[jvmArguments.size()]);

        return new ProcessBuilder(cmd);
    }

    void createVirtualHost(final String virtualHostNodeName)
    {
        final String nodeType = getNodeType();

        String storeDir = null;
        if (System.getProperty("profile", "").startsWith("java-dby-mem"))
        {
            storeDir = ":memory:";
        }
        else if (!"Memory".equals(nodeType))
        {
            storeDir = "${qpid.work_dir}" + File.separator + virtualHostNodeName;
        }

        String blueprint = System.getProperty(SYSTEST_PROPERTY_VIRTUALHOST_BLUEPRINT);
        LOGGER.debug("Creating Virtual host from blueprint: {}", blueprint);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", virtualHostNodeName);
        attributes.put("type", nodeType);
        attributes.put("qpid-type", nodeType);
        String contextAsString;
        try
        {
            contextAsString =
                    new ObjectMapper().writeValueAsString(Collections.singletonMap("virtualhostBlueprint", blueprint));
        }
        catch (JsonProcessingException e)
        {
            throw new BrokerAdminException("Cannot create virtual host as context serialization failed", e);
        }
        attributes.put("context", contextAsString);
        attributes.put("defaultVirtualHostNode", true);
        attributes.put("virtualHostInitialConfiguration", blueprint);
        if (storeDir != null)
        {
            attributes.put("storePath", storeDir);
        }

        try
        {
            Connection connection = createManagementConnection();
            try
            {
                connection.start();
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try
                {
                    new AmqpManagementFacade().createEntityUsingAmqpManagement(virtualHostNodeName,
                                                                               BROKER_TYPE_VIRTUAL_HOST_NODE,
                                                                               attributes,
                                                                               session);
                }
                catch (AmqpManagementFacade.OperationUnsuccessfulException e)
                {
                    throw new BrokerAdminException(String.format("Cannot create test virtual host '%s'",
                                                                 virtualHostNodeName), e);
                }
                finally
                {
                    session.close();
                }
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot create virtual host '%s'", virtualHostNodeName), e);
        }
    }

    void deleteVirtualHost(final String virtualHostNodeName)
    {
        try
        {
            Connection connection = createManagementConnection();
            try
            {
                connection.start();
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try
                {
                    new AmqpManagementFacade().deleteEntityUsingAmqpManagement(virtualHostNodeName,
                                                                               BROKER_TYPE_VIRTUAL_HOST_NODE,
                                                                               session);
                }
                catch (AmqpManagementFacade.OperationUnsuccessfulException e)
                {
                    throw new BrokerAdminException(String.format("Cannot delete test virtual host '%s'",
                                                                 virtualHostNodeName), e);
                }
                finally
                {
                    session.close();
                }
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot delete virtual host '%s'", virtualHostNodeName), e);
        }
    }

    void restartVirtualHost(final String virtualHostNodeName)
    {
        try
        {
            Connection connection = createManagementConnection();
            try
            {
                connection.start();
                updateVirtualHostNode(virtualHostNodeName,
                                      Collections.<String, Object>singletonMap("desiredState", "STOPPED"), connection);
                updateVirtualHostNode(virtualHostNodeName,
                                      Collections.<String, Object>singletonMap("desiredState", "ACTIVE"), connection);
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot restart virtual host '%s'", virtualHostNodeName), e);
        }
    }

    void stopVirtualHost(final String virtualHostNodeName)
    {
        try
        {
            Connection connection = createManagementConnection();
            try
            {
                connection.start();
                updateVirtualHostNode(virtualHostNodeName,
                                      Collections.<String, Object>singletonMap("desiredState", "STOPPED"), connection);
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot stop virtual host '%s'", virtualHostNodeName), e);
        }
    }

    private String getNodeType()
    {
        return System.getProperty(SYSTEST_PROPERTY_VIRTUALHOSTNODE_TYPE, "JSON");
    }

    private void updateVirtualHostNode(final String virtualHostNodeName,
                                       final Map<String, Object> attributes,
                                       final Connection connection) throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            new AmqpManagementFacade().updateEntityUsingAmqpManagement(virtualHostNodeName,
                                                                       BROKER_TYPE_VIRTUAL_HOST_NODE,
                                                                       attributes,
                                                                       session);
        }
        catch (AmqpManagementFacade.OperationUnsuccessfulException e)
        {
            throw new BrokerAdminException(String.format("Cannot update test virtual host '%s'", virtualHostNodeName),
                                           e);
        }
        finally
        {
            session.close();
        }
    }

    private void createBrokerSocketLoggerAndRulesAndDeleteOldLogger(String loggerName,
                                                                    String classQualifiedTestName,
                                                                    final String qpidLogLevel)
    {
        try
        {
            AmqpManagementFacade amqpManagementFacade = new AmqpManagementFacade();
            Connection connection = createManagementConnection();
            try
            {
                connection.start();

                String oldLogger = findOldLogger(amqpManagementFacade, connection);
                if (oldLogger != null)
                {
                    removeBrokerLogger(oldLogger, amqpManagementFacade, connection);
                }

                createBrokerSocketLogger(loggerName, classQualifiedTestName, amqpManagementFacade, connection);

                createBrokerLoggerRule(loggerName, "Root", "ROOT", "INFO", amqpManagementFacade, connection);
                createBrokerLoggerRule(loggerName,
                                       "Qpid",
                                       "org.apache.qpid.*",
                                       qpidLogLevel,
                                       amqpManagementFacade,
                                       connection);
                createBrokerLoggerRule(loggerName,
                                       "Operational",
                                       "qpid.message.*",
                                       "INFO",
                                       amqpManagementFacade,
                                       connection);
                createBrokerLoggerRule(loggerName,
                                       "Statistics",
                                       "qpid.statistics.*",
                                       "INFO",
                                       amqpManagementFacade,
                                       connection);
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot create broker socket logger and rules for '%s'",
                                                         classQualifiedTestName), e);
        }
    }

    Connection createManagementConnection() throws JMSException
    {
        return getConnection("$management", null);
    }

    private String findOldLogger(final AmqpManagementFacade amqpManagementFacade, final Connection connection)
            throws JMSException
    {
        String oldLoggerName = null;
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            List<Map<String, Object>> loggers =
                    amqpManagementFacade.managementQueryObjects(BROKER_TYPE_LOGBACK_SOCKET_LOGGER,
                                                                session);
            for (Map<String, Object> logger : loggers)
            {
                if ("BrokerLogbackSocket".equals(logger.get("qpid-type")))
                {
                    if (oldLoggerName == null)
                    {
                        oldLoggerName = (String) logger.get("name");
                    }
                    else
                    {
                        throw new BrokerAdminException("More than one BrokerLogbackSocket is configured on Broker");
                    }
                }
            }
        }
        finally
        {
            session.close();
        }
        return oldLoggerName;
    }

    private void removeBrokerLogger(final String loggerName,
                                    final AmqpManagementFacade amqpManagementFacade,
                                    final Connection connection) throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            amqpManagementFacade.deleteEntityUsingAmqpManagement(loggerName,
                                                                 BROKER_TYPE_LOGBACK_SOCKET_LOGGER,
                                                                 session);
        }
        finally
        {
            session.close();
        }
    }

    private void createBrokerSocketLogger(final String loggerName,
                                          final String classQualifiedTestName,
                                          final AmqpManagementFacade amqpManagementFacade, final Connection connection)
            throws JMSException
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", loggerName);
        attributes.put("port", "${" + SYSTEST_PROPERTY_LOGBACK_SOCKET_PORT + "}");
        attributes.put("type", "BrokerLogbackSocket");
        attributes.put("qpid-type", "BrokerLogbackSocket");
        attributes.put("contextProperties", "{\"classQualifiedTestName\" : \"" + classQualifiedTestName + "\"}");
        attributes.put("mappedDiagnosticContext", "{\"origin\" : \"" + BROKER_LOG_PREFIX + "-" + loggerName + "\"}");

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            amqpManagementFacade.createEntityUsingAmqpManagement(loggerName,
                                                                 BROKER_TYPE_LOGBACK_SOCKET_LOGGER,
                                                                 attributes,
                                                                 session);
        }
        finally
        {
            session.close();
        }
    }

    private void createBrokerLoggerRule(final String brokerLoggerName,
                                        final String ruleName,
                                        final String loggerName,
                                        final String loggerLevel,
                                        final AmqpManagementFacade amqpManagementFacade, final Connection connection)
            throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            final Map<String, Object> attributes = new HashMap<>();
            attributes.put("name", ruleName);
            attributes.put("type", "NameAndLevel");
            attributes.put("qpid-type", "NameAndLevel");
            attributes.put("object-path", brokerLoggerName);
            attributes.put("loggerName", loggerName);
            attributes.put("level", loggerLevel);
            amqpManagementFacade.createEntityUsingAmqpManagement(ruleName,
                                                                 BROKER_TYPE_NAME_AND_LEVEL_LOG_INCLUSION_RULE,
                                                                 attributes,
                                                                 session);
        }
        finally
        {
            session.close();
        }
    }

    private String getVirtualHostNodeName(final Class testClass, final Method method)
    {
        return testClass.getSimpleName() + "_" + method.getName();
    }

}
