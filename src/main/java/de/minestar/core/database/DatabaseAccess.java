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
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.support.ConnectionSource;

import java.sql.SQLException;

/**
 * Provides an API to access a database connection to via a {@link ConnectionSupplier}.
 */
public class DatabaseAccess {

    private ConnectionSupplier connectionSupplier;
    private ConnectionSource connectionSource;

    /**
     * Opens the connection from the connectionSupplier to connect to the database.
     *
     * @param connectionSupplier Used to open the connection to the database and reconnect to it.
     * @throws Exception Something went wrong while opening the connection.
     * @see ConnectionSupplier#createConnection()
     */
    public DatabaseAccess(ConnectionSupplier connectionSupplier) throws Exception {
        this.connectionSupplier = connectionSupplier;
        this.connectionSource = connectionSupplier.createConnection();
    }

    /**
     * @return The underlying connection source from ORMLite.
     */
    public ConnectionSource getConnectionSource() {
        return connectionSource;
    }

    /**
     * Create(or get if created before) the {@link com.j256.ormlite.dao.Dao} for the clazz to manipulate and query objects in the database. <br>
     * Do ALWAYS use this method to get a Dao and do NOT reuse them as attributes or long living objects. The reason for this is
     * the method {@link DatabaseAccess#reconnect()}. If you store a Dao with an old connection and change the connection and reuse the Dao,
     * the Dao will use the old, invalid connection. To prevent this, ALWAYS use this method for Dao creation.
     *
     * @param clazz The class the Dao will be responsible for. Table for the clazz must exists otherwise an error is thrown!
     * @param <T>   The class type
     * @return Dao responsible for the clazz
     * @throws SQLException
     */
    public <D extends Dao<T, ?>, T> D getDao(Class<T> clazz) throws SQLException {

        return DaoManager.createDao(connectionSource, clazz);
    }

    /**
     * Close the current connection without throwing an exception.
     */
    public void close() {
        this.connectionSource.closeQuietly();
    }

    /**
     * Close the current connection and opens a new one using the initial provided connectionSupplier.
     *
     * @throws Exception Something went wrong while connecting.
     * @see ConnectionSupplier#createConnection()
     */
    public void reconnect() throws Exception {
        this.reconnect(connectionSupplier);
    }

    /**
     * Close the current connection and opens a new one using the new used connectionSupplier. The new connectionSupplier will replace the old one.
     *
     * @param newConnectionSupplier The new connectionSupplier used for {@link de.minestar.core.database.DatabaseAccess#reconnect()}
     * @throws Exception Something went wrong while connecting.
     * @see ConnectionSupplier#createConnection()
     */
    public void reconnect(ConnectionSupplier newConnectionSupplier) throws Exception {
        this.close();
        this.connectionSupplier = newConnectionSupplier;
        this.connectionSource = newConnectionSupplier.createConnection();
    }
}
