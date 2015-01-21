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

import com.j256.ormlite.db.MysqlDatabaseType;
import com.j256.ormlite.jdbc.JdbcPooledConnectionSource;
import com.j256.ormlite.support.ConnectionSource;

/**
 * Provide information to open a connection to a MySQL database. Use a pooled connection to keep the connection alive.
 */
public class MySqlConnection implements ConnectionSupplier {

    private static final int DEFAULT_PORT = 3306;

    private final String user;
    private final String password;

    private final String jdbcUrl;

    /**
     * Connects to a MySQL Database using the host and the database name using the default port
     * {@value MySqlConnection#DEFAULT_PORT} without credentials.
     *
     * @param host     The host of the database server
     * @param database The name of the database
     */
    public MySqlConnection(String host, String database) {
        this(host, DEFAULT_PORT, database, null, null);
    }

    /**
     * Connects to a MySQL Database using the host, the port and the database name without credentials.
     *
     * @param host     The host of the database server
     * @param port     The port of the database server
     * @param database The name of the database
     */
    public MySqlConnection(String host, int port, String database) {
        this(host, port, database, null, null);
    }

    /**
     * Connects to a MySQL Database using the host, the port and the database name with the given credentials.
     *
     * @param host     The host of the database server
     * @param port     The port of the database server
     * @param database The name of the database
     * @param user     The user name for authorization
     * @param password The password for authorization
     */
    public MySqlConnection(String host, int port, String database, String user, String password) {
        this.jdbcUrl = "jdbc::mysql://" + host + ":" + port + "/" + database;

        this.user = user;
        this.password = password;
    }

    @Override
    public ConnectionSource createConnection() throws Exception {
        return new JdbcPooledConnectionSource(jdbcUrl, user, password, new MysqlDatabaseType());
    }
}
