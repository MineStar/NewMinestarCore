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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Random;

public class DatabaseConsumerTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void simpleConsumerTest() throws Exception {
        // Create database
        DatabaseAccess access = createDatabase();

        TableUtils.createTableIfNotExists(access.getConnectionSource(), SimpleEntity.class);

        // Create a consumer with default size and default sleep time
        DatabaseConsumer<SimpleEntity> consumer = new DatabaseConsumer<>(access, SimpleEntity.class);
        // Start the consumer
        DatabaseConsumer.kickOf(consumer);

        final int sampleSize = 100;
        Random random = new Random();
        int[] randomChars = random.ints(sampleSize, 'a', 'z' + 1).toArray();

        for (int i = 0; i < sampleSize; ++i) {
            SimpleEntity entity = new SimpleEntity(System.nanoTime(), (char) randomChars[i]);
            consumer.consume(entity);
        }
        // Sleep long enough the consumer can do stuff
        Thread.sleep(250L);
        // Stop the consumer - it should flush its content
        consumer.stop();
        // Sleep long enough to finish the flush
        Thread.sleep(250L);
        // Check if all entities are persisted
        Dao<SimpleEntity, Integer> dao = access.getDao(SimpleEntity.class);
        Assert.assertEquals(sampleSize, dao.queryForAll().size());

        access.close();
    }

    private DatabaseAccess createDatabase() throws Exception {
        return new DatabaseAccess(new SqliteConnection(temporaryFolder.newFile()));
    }

    @DatabaseTable
    static class SimpleEntity {
        @DatabaseField(generatedId = true)
        private int id;

        @DatabaseField
        private long timeStampNanos;

        @DatabaseField
        private char randomChar;

        public SimpleEntity() {
            // Empty constructor for ORMLite
        }

        public SimpleEntity(long timeStampNanos, char randomChar) {
            this.timeStampNanos = timeStampNanos;
            this.randomChar = randomChar;
        }
    }

}