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

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;
import com.j256.ormlite.table.TableUtils;
import jodd.json.JsonSerializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseAccessTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testManualSqliteAccess() throws Exception {
        // Create database connection supplier - this defines what database and how to connect to it
        ConnectionSupplier connectionSupplier = new SqliteConnection(temporaryFolder.newFile());
        testDatabase(connectionSupplier);
    }

    @Test
    public void testFileBasedSqliteAcces() throws Exception {
        // Test only:  Create a config file for SQLite connection.
        // For real usage: Use a file configured by end user
        File sqliteConfigFile = prepareSqliteConfigFile();

        // Create database connection supplier - this defines what database and how to connect to it
        ConnectionSupplier connectionSupplier = new ConfigSqliteConnection(sqliteConfigFile);
        testDatabase(connectionSupplier);
    }

    private void testDatabase(ConnectionSupplier connectionSupplier) throws Exception {
        // Create access to the database - now the connection will be opened
        DatabaseAccess databaseAccess = new DatabaseAccess(connectionSupplier);

        // Create the table for the entity class 'TestModelClass'(class definition at the bottom)
        TableUtils.createTableIfNotExists(databaseAccess.getConnectionSource(), TestModelClass.class);
        // Create the Dao for the class. This will maps the object to the table entities and is used for query and
        // create objects

        // The first type parameter is the mapped class
        // The second type parameter is the type of key
        Dao<TestModelClass, Integer> modelDao = databaseAccess.getDao(TestModelClass.class);

        List<TestModelClass> testData = generateTestData();
        // Store data in table
        for (TestModelClass data : testData) {
            modelDao.create(data);
        }
        // Check if they are successfully stored
        Assert.assertEquals(testData.size(), modelDao.queryForAll().size());
    }

    private File prepareSqliteConfigFile() throws Exception {
        // Create json string containing path to the database file
        File databaseFile = temporaryFolder.newFile();
        Map<String, String> values = new HashMap<>();
        values.put("file", databaseFile.getAbsolutePath());
        String jsonString = new JsonSerializer().serialize(values);

        // Write json string to config file (separate file)
        File configFile = temporaryFolder.newFile();
        PrintWriter writer = new PrintWriter(configFile);
        writer.write(jsonString);
        writer.close();

        return configFile;
    }

    private List<TestModelClass> generateTestData() {
        TestModelClass first = new TestModelClass("Meldanor", true);
        TestModelClass second = new TestModelClass("GeMoschen", true);
        TestModelClass third = new TestModelClass("west_", false);

        return Arrays.asList(first, second, third);
    }

    // This class is an example for the necessary annotations and the empty constructor
    @DatabaseTable
    static class TestModelClass {
        @DatabaseField(generatedId = true)
        private int id;

        @DatabaseField(index = true)
        private String userName;

        @DatabaseField
        private boolean isAdmin;

        // Empty constructor for ORMLite
        public TestModelClass() {

        }

        public TestModelClass(String userName, boolean isAdmin) {
            this.userName = userName;
            this.isAdmin = isAdmin;
        }

        public String getUserName() {
            return userName;
        }

        public int getId() {
            return id;
        }

        public boolean isAdmin() {
            return isAdmin;
        }
    }

}