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

package org.apache.qpid.systest.connection;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.core.brokerj.AmqpManagementFacade;

public class TlsTest extends JmsTestBase
{
    private static final String KEY_STORE = "/tls/client_keystore.jks";
    private static final String BROKER_KEY_STORE = "/tls/broker_keystore.jks";
    private static final String STORE_PASSWORD = "password";

    private List<File> _files;
    private File _brokerKeyStore;
    private File _clientCertificatePath;
    private File _clientPrivateKeyPath;
    private File _brokerCertificatePath;
    private String _portName;
    private String _keyStoreName;
    private String _trustStoreName;

    @Before
    public void setUp() throws Exception
    {
        assumeThat("Broker-j specific functionality is used by the test suite",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

        _files = new ArrayList<>();

        _brokerKeyStore = copyResource(BROKER_KEY_STORE);
        _files.add(_brokerKeyStore);
        final File[] clientResources = extractResourcesFromTestKeyStore(KEY_STORE, "app1");
        _files.addAll(Arrays.asList(clientResources));
        _clientCertificatePath = clientResources[1];
        _clientPrivateKeyPath = clientResources[0];
        final File[] brokerResources = extractResourcesFromTestKeyStore(BROKER_KEY_STORE, "java-broker");
        _files.addAll(Arrays.asList(brokerResources));
        _brokerCertificatePath = brokerResources[1];
        _portName = getTestName() + "Port";
        _keyStoreName = _portName + "KeyStore";
        _trustStoreName = _portName + "TrustStore";
    }


    @After
    public void tearDown()
    {
        if (_files != null)
        {
            _files.forEach(f -> f.delete());
        }
    }

    @Test
    public void testCreateSSLWithCertFileAndPrivateKey() throws Exception
    {
        final int port = createTlsPort(true, false);

        final Map<String, String> options = new HashMap<>();
        options.put("client_cert_path", encodePathOption(_clientCertificatePath.getCanonicalPath()));
        options.put("client_cert_priv_key_path", encodePathOption(_clientPrivateKeyPath.getCanonicalPath()));
        options.put("trusted_certs_path", encodePathOption(_brokerCertificatePath.getCanonicalPath()));
        options.put("ssl", "true");
        final Connection connection =
                getBrokerAdmin().getConnection(getBrokerAdmin().getVirtualHostName(), options, port);
        try
        {
            assertConnection(connection);
        }
        finally
        {
            connection.close();
        }
    }

    private File copyResource(final String resource) throws IOException
    {
        Path brokerKeyStore = Files.createTempFile("key_store", ".jks");
        try (final InputStream in = getClass().getResourceAsStream(resource))
        {
            Files.copy(in, brokerKeyStore, REPLACE_EXISTING);
        }
        return brokerKeyStore.toFile();
    }


    public int createTlsPort(final boolean needClientAuth,
                             final boolean wantClientAuth) throws Exception
    {
        final AmqpManagementFacade managementFacade = new AmqpManagementFacade();
        Connection connection = getBrokerManagementConnection();
        try
        {
            connection.start();

            final String authenticationProviderName = getAuthenticationProviderName(connection, managementFacade);
            createKeyStore(connection, managementFacade);
            createTrustStore(connection, managementFacade);
            createPort(connection,
                       managementFacade,
                       needClientAuth,
                       wantClientAuth,
                       authenticationProviderName);
            return getBoundPort(connection, managementFacade);
        }
        finally
        {
            connection.close();
        }
    }

    private void createKeyStore(final Connection connection,
                                final AmqpManagementFacade managementFacade) throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            final Map<String, Object> keyStoreAttributes = new HashMap<>();
            keyStoreAttributes.put("storeUrl", _brokerKeyStore.getAbsolutePath());
            keyStoreAttributes.put("password", STORE_PASSWORD);
            keyStoreAttributes.put("keyStoreType", "pkcs12");
            managementFacade.createEntityUsingAmqpManagement(_keyStoreName,
                                                             "org.apache.qpid.server.security.FileKeyStore",
                                                             keyStoreAttributes,
                                                             session);
        }
        finally
        {
            session.close();
        }
    }

    private void createTrustStore(final Connection connection,
                                  final AmqpManagementFacade managementFacade) throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            final Map<String, Object> trustStoreAttributes = new HashMap<>();
            trustStoreAttributes.put("certificatesUrl", _clientCertificatePath.getAbsolutePath());
            managementFacade.createEntityUsingAmqpManagement(_trustStoreName,
                                                             "org.apache.qpid.server.security.NonJavaTrustStore",
                                                             trustStoreAttributes,
                                                             session);
        }
        finally
        {
            session.close();
        }
    }

    private void createPort(final Connection connection,
                            final AmqpManagementFacade managementFacade,
                            final boolean needClientAuth,
                            final boolean wantClientAuth,
                            final String authenticationProvider)
            throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            final Map<String, Object> sslPortAttributes = new HashMap<>();
            sslPortAttributes.put("transports", "[\"SSL\"]");
            sslPortAttributes.put("port", 0);
            sslPortAttributes.put("authenticationProvider", authenticationProvider);
            sslPortAttributes.put("needClientAuth", needClientAuth);
            sslPortAttributes.put("wantClientAuth", wantClientAuth);
            sslPortAttributes.put("name", _portName);
            sslPortAttributes.put("keyStore", _keyStoreName);
            sslPortAttributes.put("trustStores", "[\"" + _trustStoreName + "\"]");

            managementFacade.createEntityUsingAmqpManagement(_portName,
                                                             "org.apache.qpid.AmqpPort",
                                                             sslPortAttributes,
                                                             session);
        }
        finally
        {
            session.close();
        }
    }

    private int getBoundPort(final Connection connection,
                             final AmqpManagementFacade managementFacade) throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            final Map<String, Object> portEffectiveAttributes =
                    managementFacade.readEntityUsingAmqpManagement(_portName,
                                                                   "org.apache.qpid.AmqpPort",
                                                                   false,
                                                                   session);
            if (portEffectiveAttributes.containsKey("boundPort"))
            {
                return (int) portEffectiveAttributes.get("boundPort");
            }
            throw new RuntimeException("Bound port is not found");
        }
        finally
        {
            session.close();
        }
    }

    private String getAuthenticationProviderName(final Connection connection,
                                                 final AmqpManagementFacade managementFacade) throws JMSException
    {
        String authenticationProvider = null;
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            final List<Map<String, Object>> ports =
                    managementFacade.managementQueryObjects("org.apache.qpid.AmqpPort", session);
            for (Map<String, Object> port : ports)
            {
                final String name = String.valueOf(port.get("name"));

                final Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try
                {
                    Map<String, Object> attributes = managementFacade.readEntityUsingAmqpManagement(name,
                                                                                                    "org.apache.qpid.AmqpPort",
                                                                                                    false,
                                                                                                    s);
                    if (attributes.get("boundPort")
                                  .equals(getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getPort()))
                    {
                        authenticationProvider = String.valueOf(attributes.get("authenticationProvider"));
                        break;
                    }
                }
                finally
                {
                    s.close();
                }
            }
        }
        finally
        {
            session.close();
        }
        return authenticationProvider;
    }

    private File[] extractResourcesFromTestKeyStore(final String keyStore, final String alias) throws Exception
    {
        final java.security.KeyStore ks = java.security.KeyStore.getInstance("pkcs12");
        try (final InputStream in = getClass().getResourceAsStream(keyStore))
        {
            ks.load(in, STORE_PASSWORD.toCharArray());
        }

        final File privateKeyFile = Files.createTempFile(getTestName(), ".private-key.der").toFile();
        try (FileOutputStream kos = new FileOutputStream(privateKeyFile))
        {
            Key pvt = ks.getKey(alias, STORE_PASSWORD.toCharArray());
            kos.write(pvt.getEncoded());
        }

        final File certificateFile = Files.createTempFile(getTestName(), ".certificate.der").toFile();
        try (FileOutputStream cos = new FileOutputStream(certificateFile))
        {
            Certificate[] chain = ks.getCertificateChain(alias);
            for (Certificate pub : chain)
            {
                cos.write(pub.getEncoded());
            }
            cos.flush();
        }

        return new File[]{privateKeyFile, certificateFile};
    }

    private String encodePathOption(final String canonicalPath)
    {
        try
        {
            return URLEncoder.encode(canonicalPath, StandardCharsets.UTF_8.name()).replace("+", "%20");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void assertConnection(final Connection connection) throws JMSException
    {
        assertNotNull("connection should be successful", connection);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull("create session should be successful", session);
    }
}
