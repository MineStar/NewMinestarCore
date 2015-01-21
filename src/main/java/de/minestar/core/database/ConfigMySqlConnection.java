/*
 * This file is licensed under the MIT License (MIT).
 *
 * Copyright (c) Minestar.de <http://www.minestar.de/>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package de.minestar.core.database;

import com.j256.ormlite.support.ConnectionSource;
import jodd.json.JsonParser;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class for file based connection configuration of a {@link MySqlConnection}. The file must be a JSON file with the following content:
 * <code>
 * <pre>
 * {
 *     "host": "localhost",
 *     "port": 3306,
 *     "database": "minecraft",
 *     "username": "root",
 *     "password": "test"
 * }
 * </pre>
 * </code>
 */
public class ConfigMySqlConnection implements ConnectionSupplier {

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String DATABASE = "database";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private final MySqlConnection mySqlConnection;

    public ConfigMySqlConnection(File configJsonFile) throws Exception {
        this.mySqlConnection = createConnection(configJsonFile);
    }

    private MySqlConnection createConnection(File configJsonFile) throws Exception {

        // Read file content as one string
        String jsonString = new String(Files.readAllBytes(configJsonFile.toPath()), Charset.forName("UTF8"));
        Map<String, Object> values = new JsonParser().parse(jsonString);
        validateValues(configJsonFile, values);

        String host = (String) values.get(HOST);
        int port = (int) values.get(PORT);
        String database = (String) values.get(DATABASE);
        String user = (String) values.get(USERNAME);
        String pass = (String) values.get(PASSWORD);

        return new MySqlConnection(host, port, database, user, pass);
    }

    private void validateValues(File configJsonFile, Map<String, Object> values) throws Exception {
        List<String> keysToCheck = Arrays.asList(HOST, PORT, DATABASE, USERNAME, PASSWORD);
        // Find missing keys
        List<String> missingKeys = keysToCheck.stream().filter(key -> !values.containsKey(key)).collect(Collectors.toList());
        if (!missingKeys.isEmpty()) {
            throw new IllegalArgumentException("File '" + configJsonFile.getAbsolutePath() + "' does not contains the keys '" + missingKeys + "'!");
        }
    }

    @Override
    public ConnectionSource createConnection() throws Exception {
        return mySqlConnection.createConnection();
    }
}
