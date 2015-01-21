package de.minestar.core.database;

import com.j256.ormlite.support.ConnectionSource;

/**
 * Opens a connection to a database.
 */
public interface ConnectionSupplier {

    /**
     * Create a connection to a database. This method must always return a newly created connection source.
     *
     * @return A connection to a database.
     * @throws Exception The connection failed because of an offline database, wrong user/password information,
     *                   wrong connection information or failed file access.
     */
    ConnectionSource createConnection() throws Exception;
}
