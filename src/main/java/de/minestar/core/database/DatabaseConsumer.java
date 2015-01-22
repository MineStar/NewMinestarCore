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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes objects and store them into to the database. <br>
 * <p>
 * The objects are buffered into a queue and only persisted, when the flush size is reached (default
 * {@value DatabaseConsumer#DEFAULT_FLUSH_SIZE}) or the methods {@link DatabaseConsumer#flush()} or
 * {@link DatabaseConsumer#stop()} are invoked. The objects are stored in a {@link java.util.concurrent.BlockingQueue}
 * and drained into a temporary buffer. This reduces the blocking time while flushing.
 * <p>
 * The consumer will check the size of its queue in a interval(default {@value DatabaseConsumer#DEFAULT_SLEEP_TIME_MILLIS} ms).
 * If the queue contains much more than the flush size is, the time interval will be halved and will be reset when the
 * consumer had enough idle cycles without a flush. This mechanism will prevent heavy load spikes.
 * <p>
 * The method {@link DatabaseConsumer#kickOf(DatabaseConsumer)} provide a standard method to start a consumer. The consumer
 * are handled by a Thread Pool. Using an own thread for the consumer needs to invoke {@link DatabaseConsumer#start()}
 * before starting the thread.
 *
 * @param <T> The type of objects to consume
 */
public class DatabaseConsumer<T> implements Runnable {

    private static final double TOO_MANY_ELEMENTS = 1.1;
    private static final long DEFAULT_SLEEP_TIME_MILLIS = 50L;
    private static final int DEFAULT_FLUSH_SIZE = 32;
    private static final int IDLE_CYCLES_BEFORE_RESET = 10;

    private final DatabaseAccess access;
    private final int flushSize;
    private final Class<T> entityClass;
    private final List<T> flushBuffer;

    private final long initialSleepTime;
    private long sleepTime;
    private BlockingQueue<T> queue;

    private boolean isRunning;
    private AtomicInteger idleCycles;

    /**
     * Creates an default database consumer with default sleep time of {@value DatabaseConsumer#DEFAULT_SLEEP_TIME_MILLIS} ms
     * and a default flush size of {@value DatabaseConsumer#DEFAULT_FLUSH_SIZE}.
     *
     * @param access      The access to the database. Cannot be null
     * @param entityClass The class of the entity to consume. Cannot be null
     */
    public DatabaseConsumer(final DatabaseAccess access, final Class<T> entityClass) {
        this(access, entityClass, DEFAULT_FLUSH_SIZE, DEFAULT_SLEEP_TIME_MILLIS);
    }

    /**
     * Creates a database consumer with fine adjustment of running parameter.
     *
     * @param access          The access to the database. Cannot be null
     * @param entityClass     The class of the entity to consume. Cannot be null
     * @param flushSize       If the added object count is equals or higher than this parameter, the queue will be flushed.
     * @param sleepTimeMillis The interval the consumer will check queues size
     */
    public DatabaseConsumer(final DatabaseAccess access, final Class<T> entityClass, final int flushSize, final long sleepTimeMillis) {
        this.access = access;
        this.flushSize = flushSize;
        this.entityClass = entityClass;

        this.initialSleepTime = sleepTimeMillis;
        this.sleepTime = sleepTimeMillis;
        this.idleCycles = new AtomicInteger();
        this.queue = new LinkedBlockingQueue<>();
        this.flushBuffer = new ArrayList<>(flushSize);
    }

    /**
     * Add an object to the consumer. The consumer will persist it later.
     *
     * @param ele The object to add
     */
    public void consume(T ele) {
        this.queue.add(ele);
    }

    /**
     * Starts the consumer. This does not mean, that the consumer will persist the objects immediately! This method should
     * be invoked by a Thread before the Thread is started!
     */
    public void start() {
        this.isRunning = true;
    }

    /**
     * Stops the consumer. If the consumer is running by a Thread, it will flush the queue. Otherwise, a manual flush
     * is necessary!
     */
    public void stop() {
        this.isRunning = false;
    }

    /**
     * Persists all objects in the queue ignoring the flush size.
     */
    public void flush() {
        this.flush(queue.size());
    }

    @Override
    public void run() {

        while (isRunning) {
            int queueSize = queue.size();
            // Flush the queue
            if (queueSize >= flushSize) {
                flush(queueSize);

                // Decrease the sleep time if the queue was overloaded
                this.sleepTime = wasQueueOverloaded(queueSize) ? sleepTime / 2L : sleepTime;
            }
            // Idle cycle
            // If they are high enough reset sleep time to initial sleep time to prevent idle cycles due sleep time
            // reduction
            else if (idleCycles.incrementAndGet() > IDLE_CYCLES_BEFORE_RESET) {
                idleCycles.set(0);
                this.sleepTime = initialSleepTime;
            }

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.out.println("Thread " + Thread.currentThread().getName() + " was interrupted. Flush queue!");
                stop();
            }
        }
        if (!queue.isEmpty())
            flush();
    }

    private boolean wasQueueOverloaded(int queueSize) {
        return ((double) queueSize / (double) flushSize) > TOO_MANY_ELEMENTS;
    }

    private void flush(int queueSize) {

        queue.drainTo(flushBuffer);
        try {
            Dao<T, ?> dao = access.getDao(entityClass);
            dao.callBatchTasks(() -> {
                for (int i = 0; i < queueSize; ++i) {
                    dao.create(flushBuffer.get(i));
                }
                return null;
            });
            flushBuffer.clear();
        } catch (Exception ignore) {
        }
    }

    private static ExecutorService threadPool = Executors.newCachedThreadPool();

    /**
     * Starts the consumer using a Thread provided by Thread Pool. Using this method is suggested, but not necessary.
     *
     * @param consumer The consumer to start. The consumer will be handled by a Thread.
     * @param <T>      The type of objects to consume
     */
    public static <T> void kickOf(DatabaseConsumer<T> consumer) {
        consumer.start();
        threadPool.submit(consumer);
    }
}
