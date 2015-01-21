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

import com.j256.ormlite.db.SqliteDatabaseType;
import com.j256.ormlite.jdbc.JdbcPooledConnectionSource;
import com.j256.ormlite.support.ConnectionSource;

import java.io.File;

/**
 * Provide information to open a connection to a SQLite filebased database.
 */
public class SqliteConnection implements ConnectionSupplier {

    private final String jdbcUrl;

    /**
     * Creates if not existing the file and connecting to the database.
     *
     * @param file The file of the SQLite database
     */
    public SqliteConnection(File file) {
        this(file.getAbsolutePath());
    }

    /**
     * Creates if not existing the file and connecting to the database.
     *
     * @param filePath The path to the SQLite database file
     */
    public SqliteConnection(String filePath) {
        this.jdbcUrl = "jdbc:sqlite:" + filePath;
    }

    @Override
    public ConnectionSource createConnection() throws Exception {
        return new JdbcPooledConnectionSource(jdbcUrl, new SqliteDatabaseType());
    }
}
