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
import com.j256.ormlite.stmt.PreparedQuery;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.SelectArg;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Kilian on 23.01.2015.
 */
public class HeavySelectTest {

    private static final int SAMPLE_SIZE = (int) Math.pow(2, 10);
    private static final int SAMPLE_ROUNDS = 5;

    public static void main(String[] args) throws Exception {
        List<Selection> sampleSelections = generateSampleLocations();

        ConnectionSupplier mysqlDbConnection = new MySqlConnection("192.168.1.29", 3306, "minestar_therock", "consumertest", "test");
        System.out.println("Rounds:\t\t" + SAMPLE_ROUNDS);
        System.out.println("Sample Size:\t" + SAMPLE_SIZE);
        System.out.println("----------------------------");
        // Warm up
        System.out.println("Run MySQL warm up");
        test(mysqlDbConnection, sampleSelections);
        test(mysqlDbConnection, sampleSelections);

        System.out.println("Run MySQL Tests");
        Map<Integer, List<Long>> mySQLTimes = new HashMap<>();
        for (int i = 0; i < SAMPLE_ROUNDS; ++i) {
            mySQLTimes.put(i, test(mysqlDbConnection, sampleSelections));
        }

        ConnectionSupplier mariaDbConnection = new MariaDbConnection("192.168.1.29", 3307, "minestar_therock", "consumertest", "test");

        // Warm up
        System.out.println("Run MariaDB warm up");
        test(mariaDbConnection, sampleSelections);
        test(mariaDbConnection, sampleSelections);

        System.out.println("Run MariaDB Tests");
        Map<Integer, List<Long>> mariaDBTimes = new HashMap<>();
        for (int i = 0; i < SAMPLE_ROUNDS; ++i) {
            mariaDBTimes.put(i, test(mysqlDbConnection, sampleSelections));
        }

        System.out.println("MySQL Results:");
        System.out.println("Mean: " + calculateMeanTimes(mySQLTimes));
        System.out.println("Median: " + calculateMedianTimes(mySQLTimes));

        System.out.println("MariaDB Results:");
        System.out.println("Mean: " + calculateMeanTimes(mariaDBTimes));
        System.out.println("Median: " + calculateMedianTimes(mariaDBTimes));
    }

    private static List<Long> calculateMeanTimes(Map<Integer, List<Long>> times) {
        List<Long> result = new ArrayList<>();

        for (int i = 0; i < SAMPLE_SIZE; i++) {
            BigInteger sum = BigInteger.ZERO;
            int j = 0;
            for (List<Long> roundTimes : times.values()) {
                sum = sum.add(BigInteger.valueOf(roundTimes.get(i)));
                ++j;
            }
            result.add(sum.divide(BigInteger.valueOf(j)).longValue());
        }

        return result;
    }

    private static List<Long> calculateMedianTimes(Map<Integer, List<Long>> times) {
        List<Long> result = new ArrayList<>();

        for (int i = 0; i < SAMPLE_SIZE; i++) {
            List<Long> medianList = new ArrayList<>();
            int j = 0;
            for (List<Long> roundTimes : times.values()) {
                medianList.add(roundTimes.get(i));
                ++j;
            }
            medianList.sort(Long::compareTo);

            result.add(medianList.get(j / 2));
        }

        return result;
    }

    private static List<Long> test(ConnectionSupplier supplier, List<Selection> sampleSelections) throws Exception {
        DatabaseAccess access = new DatabaseAccess(supplier);
        Dao<HeavyConsumerTest.Block, ?> dao = access.getDao(HeavyConsumerTest.Block.class);

        QueryBuilder<HeavyConsumerTest.Block, ?> queryBuilder = dao.queryBuilder();
        queryBuilder.where()
                .between("blockX", new SelectArg(), new SelectArg())
                .and()
                .between("blockY", new SelectArg(), new SelectArg())
                .and()
                .between("blockZ", new SelectArg(), new SelectArg());
        PreparedQuery<HeavyConsumerTest.Block> blockPreparedQuery = queryBuilder.prepare();

        List<Long> times = new ArrayList<>(SAMPLE_SIZE);
        int i = 0;
        for (Selection selection : sampleSelections) {
            Location first = selection.first;
            Location secoN = selection.second;

            blockPreparedQuery.setArgumentHolderValue(0, Math.min(first.x, secoN.x));
            blockPreparedQuery.setArgumentHolderValue(1, Math.max(first.x, secoN.x));

            blockPreparedQuery.setArgumentHolderValue(2, Math.min(first.y, secoN.y));
            blockPreparedQuery.setArgumentHolderValue(3, Math.max(first.y, secoN.y));

            blockPreparedQuery.setArgumentHolderValue(4, Math.min(first.z, secoN.z));
            blockPreparedQuery.setArgumentHolderValue(5, Math.max(first.z, secoN.z));
            long time = System.nanoTime();
            List<HeavyConsumerTest.Block> result = dao.query(blockPreparedQuery);
            time = System.nanoTime() - time;
            times.add(TimeUnit.NANOSECONDS.toMicros(time));
            if (!result.isEmpty())
                System.out.println(i++ + " -> " + result.size() + " found!");
        }


        access.close();
        return times;

    }

    private static List<Selection> generateSampleLocations() {
        List<Selection> result = new ArrayList<>(SAMPLE_SIZE);

        Random random = new Random();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            // [10-100] x [10-100] rectangle
            int width = random.nextInt(90) + 10;
            int height = random.nextInt(90) + 10;
            // [10-30] deep cuboid
            int depth = random.nextInt(20) + 10;

            int blockX = random.nextInt(Integer.MAX_VALUE) * (random.nextBoolean() ? 1 : -1);
            int blockZ = random.nextInt(Integer.MAX_VALUE) * (random.nextBoolean() ? 1 : -1);
            int blockY = random.nextInt(256);

            Location first = new Location(blockX, blockY, blockZ);
            blockX = blockX + (width * (random.nextBoolean() ? 1 : -1));
            blockZ = blockZ + (height * (random.nextBoolean() ? 1 : -1));
            blockY = blockY + (depth * (random.nextBoolean() ? 1 : -1));

            Location second = new Location(blockX, blockY, blockZ);
            result.add(new Selection(first, second));
        }

        return result;
    }

    private static class Selection {
        Location first;
        Location second;

        public Selection(Location first, Location second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return "Selection{" +
                    "first=" + first +
                    ", second=" + second +
                    '}';
        }
    }

    private static class Location {
        int x;
        int y;
        int z;

        public Location(int x, int y, int z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        @Override
        public String toString() {
            return "Location{" +
                    "x=" + x +
                    ", y=" + y +
                    ", z=" + z +
                    '}';
        }
    }


}
