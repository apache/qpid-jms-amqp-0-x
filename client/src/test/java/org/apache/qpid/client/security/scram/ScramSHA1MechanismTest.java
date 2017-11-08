/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.client.security.scram;

import static org.junit.Assert.assertArrayEquals;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.security.UsernamePasswordCallbackHandler;
import org.apache.qpid.jms.ConnectionURL;

/**
 * The known good used by these tests is taken from the example in RFC 5802 section 5.
 */
public class ScramSHA1MechanismTest extends AbstractScramSHAMechanismTestBase
{

    private static final String USERNAME = "user";
    private static final String PASSWORD = "pencil";

    private static final String CLIENT_NONCE = "fyko+d2lbbFgONRv9qkxdawL";

    private static final byte[] EXPECTED_CLIENT_INITIAL_RESPONSE =
            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVER_FIRST_MESSAGE =
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EXPECTED_CLIENT_FINAL_MESSAGE =
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=".getBytes(
                    StandardCharsets.UTF_8);
    private static final byte[] SERVER_FINAL_MESSAGE =
            "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=".getBytes(StandardCharsets.UTF_8);

    public ScramSHA1MechanismTest()
    {
        super(EXPECTED_CLIENT_INITIAL_RESPONSE,
              SERVER_FIRST_MESSAGE,
              EXPECTED_CLIENT_FINAL_MESSAGE,
              SERVER_FINAL_MESSAGE);
    }

    @Override
    protected AbstractScramSaslClient getScramSaslClient() throws Exception
    {
        return getScramSaslClient(USERNAME, PASSWORD);
    }

    @Override
    protected AbstractScramSaslClient getScramSaslClient(final String username, final String password) throws Exception
    {

        UsernamePasswordCallbackHandler callbackHandler = new UsernamePasswordCallbackHandler();
        ConnectionURL connectionURL = new AMQConnectionURL(String.format("amqp://%s:%s@////", username, password));
        callbackHandler.initialise(connectionURL);

        return new ScramSHA1SaslClient(callbackHandler, CLIENT_NONCE);
    }

    @Override
    protected String getExpectedInitialResponseString(final String escapedUsername)
    {
        return "n,,n=" + escapedUsername + ",r=" + CLIENT_NONCE;
    }

    public void testPasswordCommaEqualsCharactersNotEscaped() throws Exception
    {
        AbstractScramSaslClient mechanism = getScramSaslClient(USERNAME, PASSWORD + ",=");

        byte[] clientInitialResponse = mechanism.evaluateChallenge(null);
        assertArrayEquals(EXPECTED_CLIENT_INITIAL_RESPONSE, clientInitialResponse);

        byte[] serverFirstMessage =
                "r=fyko+d2lbbFgONRv9qkxdawLdcbfa301-1618-46ee-96c1-2bf60139dc7f,s=Q0zM1qzKMOmI0sAzE7dXt6ru4ZIXhAzn40g4mQXKQdw=,i=4096"
                        .getBytes(StandardCharsets.UTF_8);
        byte[] expectedClientFinalMessage =
                "c=biws,r=fyko+d2lbbFgONRv9qkxdawLdcbfa301-1618-46ee-96c1-2bf60139dc7f,p=quRNWvZqGUvPXoazebZe0ZYsjQI=".getBytes(
                        StandardCharsets.UTF_8);

        byte[] clientFinalMessage = mechanism.evaluateChallenge(serverFirstMessage);

        assertArrayEquals(expectedClientFinalMessage, clientFinalMessage);

        byte[] serverFinalMessage = "v=dnJDHm3fp6WwVrl5yjZuqKp03lQ=".getBytes(StandardCharsets.UTF_8);
        byte[] expectedFinalChallengeResponse = "".getBytes();

        assertArrayEquals(expectedFinalChallengeResponse, mechanism.evaluateChallenge(serverFinalMessage));

        assertTrue(mechanism.isComplete());
    }
}