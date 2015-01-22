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

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;
import com.j256.ormlite.table.TableUtils;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Kilian on 22.01.2015.
 */
public class HeavyConsumerTest {

    // Current amount of data in database
    private static final int SAMPLE_SIZE = (int) Math.pow(2, 15);

    public static void main(String[] args) throws Exception {

        Map<Integer, Integer> testValues = new HashMap<>();
        testValues.put(256, 50);
        testValues.put(128, 50);
        testValues.put(64, 50);
        testValues.put(32, 50);
        ConnectionSupplier mariaDbConnection = new MariaDbConnection("192.168.1.29", 3307, "minestar_therock", "consumertest", "test");
        ConnectionSupplier mysqlDbConnection = new MySqlConnection("192.168.1.29", 3306, "minestar_therock", "consumertest", "test");
        test(testValues, mysqlDbConnection, true);
        testValues = new HashMap<>();
        testValues.put(256, 25);
        testValues.put(128, 25);
        testValues.put(64, 25);
        testValues.put(32, 25);
        test(testValues, mysqlDbConnection, true);
    }


    public static void test(Map<Integer, Integer> testValues, ConnectionSupplier supplier, boolean onlyResult) throws Exception {
        for (Map.Entry<Integer, Integer> testValue : testValues.entrySet()) {
            int flushSize = testValue.getKey();
            int sleepTime = testValue.getValue();

            List<Integer> values = new LinkedList<>();
            DatabaseAccess access = new DatabaseAccess(supplier);
            TableUtils.dropTable(access.getConnectionSource(), Block.class, true);
            TableUtils.createTableIfNotExists(access.getConnectionSource(), Block.class);
            DatabaseConsumer<Block> consumer = new DatabaseConsumer<>(access, Block.class, flushSize, sleepTime);
            DatabaseConsumer.kickOf(consumer);
            long time = System.nanoTime();
            for (int i = 0, j = 0; i < SAMPLE_SIZE; ++i, ++j) {
                consumer.consume(generateBlock());
                if (j == 1000) {
                    int size = queueSize(consumer);
                    values.add(size);
                    if (!onlyResult)
                        System.out.println("Sleep (i = " + i + ", Consumer Queue: " + size + ")");
                    Thread.sleep(900L + random.nextInt(100));
                    j = 0;
                }
            }
            int size;
            while ((size = queueSize(consumer)) >= flushSize) {
                values.add(size);
                if (!onlyResult)
                    System.out.println("Sleep (Consumer Queue: " + size + ")");
                Thread.sleep(1000);
            }
            consumer.flush();
            consumer.stop();
            time = System.nanoTime() - (time);
            time = time - TimeUnit.MILLISECONDS.toNanos(1000);
            System.out.println("Results for Flush Size " + flushSize + " and Sleep Time " + sleepTime);
            System.out.println(values + ",");
            System.out.println(Duration.ofNanos(time).toMillis() + "ms");
            access.close();
        }
    }


    static Field field;

    static {
        try {
            field = DatabaseConsumer.class.getDeclaredField("queue");
            field.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    private static <T> int queueSize(DatabaseConsumer<T> consumer) throws Exception {
        return ((BlockingQueue<T>) field.get(consumer)).size();
    }

    private static final String[] extraData = {
            "wegen der`Spawns aus-`leuchten`möchtest.",
            "Offen gelassen,`falls du die`Höhle er-`forschen oder",
            "Offen gelassen,`falls du die`Höhle wegen der`Spawns",
            "ausleuchten`möchtest.``",
            "Sry, aber iwie`ist dieses Ding`verschwunden...`(Überm Kessel)",
            "U-Bahn`Farm`Uglytown`------>",
            "===============`GESPERRT`LANE CLOSED`===============",
            "3      MBB rj53``UGLYhbf`",
            "3      MBB rj53``UGLYhbf`",
            "MBB rj53      3``UGLYhbf`",
            "MBB rj53      3``UGLYhbf`",
            "```",
            "```",
            "MBB rj 48     2`Rom; ILL; BBT;`Stark Tower;`Minehattan HBF",
            "MBB rj 48     2`Rom; ILL; BBT;`Stark Tower;`Minehattan HBF",
            "2      MBB rj48`Rom; ILL; BBT;`Stark Tower;`Minehattan HBF",
            "2     MBB rj 48`Rom; ILL; BBT;`Stark Tower;`Minehattan HBF",
            "`BAUSTELLE``",
            "`Farm OST`2             3`",
            "MBB-Bahnhof`Straßenbahn`<-----`^^Zentrum^^",
            "MBB-Bahnhof`S1, Linie 8`<--------`",
            "Bahnsteige`[Lift Down]`Ausgang`Linie 8",
            "1`nach:`Lieonsberg`",
            "Bahnsteige`[Lift Down]`Ausgang`Linie 8",
            "```",
            "`im Bau``",
            "in`Bau``",
            "===============`Kaptiolsspitze`119m ü. NN`===============",
            "`nope.``",
            "`OUT``",
            "Erbauer`MarX`&`Minercraftchef",
            "Turbinenhalle 2```",
            "Feuerstein`<--``",
            "Obsidian`<--`-->`Feuerstein",
            "Brick Slap`<--`-->`Obsidian",
            "Redstone`<--`-->`Brick Slap",
            "Stock`<--`-->`Zaun",
            "Nether`<--`-->`Stock",
            "Holz`<--`-->`Nether",
            "Fackeln`<--`-->`Grasblock",
            "Eis`<--`-->`Fackeln",
            "Leiter`<--`-->`Eis",
            "Schienen`<--`-->`Leiter",
            "Laub`<--`-->`Schienen",
            "Lehm`<--`-->`Laub",
            "Holz`<--``",
            "Wolle`<--`-->`Holz",
            "Kies`<--`-->`Wolle",
            "Erde`<--`-->`Kies",
            "Lapis Lazuli`<--`-->`Erde",
    };

    private final static String[] REASONS = {
            "Sheep",
            "STICKY_PISTON",
            "immobilienkaj",
            "Crafter5520",
            "s1rm4x",
            "FallingSand",
            "Skeleton",
            "Chillhase1",
            "Chicken",
            "PISTON",
            "SnowMan",
            "Zombie",
            "sebihapunkt",
            "hanno1903",
            "General_Mayrling",
            "Waterflow",
            "Soulikotze",
            "Krishnack",
            "jan00020",
            "MarX610",
            "srt_Tatwaffe",
            "Creeper",
            "bruellwitz",
            "fuzzy128",
            "Schoelle",
            "Render_SM",
            "mighty_prince",
            "GameChickenLP",
            "Fubaki",
            "MC_Splash247",
            "Witch",
            "PrimedTnt",
            "Marci_dietl",
            "Spider",
            "Rude_Awakening",
            "Slime",
            "Lavaflow",
            "ichotolot",
            "xXCrAzYgUy",
            "lunaticXprime",
            "Bjoern_22",
            "danielmue20",
            "Flasche_Pommes",
            "Axer1990",
            "Villager",
            "Renovat",
            "JanSeneberg",
            "MR_BLACK97",
            "GeMoschen",
            "Silverfish",
            "Wyzzlex",
            "lReloade",
            "Fleckzeck",
            "130i",
            "west_",
            "goldschaf",
            "GenoTop",
            "Cow",
            "LeopardN02",
            "Smodd",
            "Max50090",
            "Pig",
            "CaveSpider",
            "Zassi12",
            "BMW168",
            "Rheumi",
            "Sebastian56244",
            "Sarrim",
            "Loomes_",
    };

    private static Random random = new Random();

    private static Block generateBlock() {
        Block block = new Block();
        block.blockX = random.nextInt(Integer.MAX_VALUE) * (random.nextBoolean() ? 1 : -1);
        block.blockY = random.nextInt(256);
        block.blockZ = random.nextInt(Integer.MAX_VALUE) * (random.nextBoolean() ? 1 : -1);
        block.timestamp = System.currentTimeMillis();
        block.fromId = random.nextInt(256);
        block.toId = random.nextInt(256);
        block.fromData = random.nextInt(16);
        block.toData = random.nextInt(16);
        block.reason = REASONS[random.nextInt(REASONS.length)];
        block.extraData = (random.nextInt(2400) == 0) ? extraData[random.nextInt(extraData.length)] : "";
        return block;
    }

    @DatabaseTable
    static class Block {
        @DatabaseField(generatedId = true)
        private int id;
        @DatabaseField(index = true)
        private long timestamp;
        @DatabaseField
        private String reason;
        @DatabaseField
        private int eventType;
        @DatabaseField(index = true)
        private int blockX;
        @DatabaseField(index = true)
        private int blockY;
        @DatabaseField(index = true)
        private int blockZ;
        @DatabaseField
        private int fromId;
        @DatabaseField
        private int toId;
        @DatabaseField
        private int fromData;
        @DatabaseField
        private int toData;
        @DatabaseField
        private String extraData;

        public Block() {
            // Empty constructor for empty shit
        }

        public Block(long timestamp, String reason, int eventType, int blockX, int blockY, int blockZ, int fromId, int toId, int fromData, int toData, String extraData) {
            this.timestamp = timestamp;
            this.reason = reason;
            this.eventType = eventType;
            this.blockX = blockX;
            this.blockY = blockY;
            this.blockZ = blockZ;
            this.fromId = fromId;
            this.toId = toId;
            this.fromData = fromData;
            this.toData = toData;
            this.extraData = extraData;
        }
    }
}
