package com.github.basking2.jiraffet.db;

import java.io.File;
import java.sql.SQLException;

import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.session.SqlSessionManager;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.flywaydb.core.Flyway;

/**
 * Manage opening, migrating and building database related resources.
 */
public class LogDaoDbManager implements AutoCloseable {
    private static String DBADMIN_USER = "jiraffet_admin";
    private static String DBADMIN_PASS = "";

    private File where;
    private SqlSessionManager sqlSessionManager;

    public LogDaoDbManager(final String where) throws ClassNotFoundException, SQLException
    {
        this(new File(where));
    }

    /**
     * Create a DiskDbDocumentStore.
     *
     * @param where Where is the disk database loaded.
     *
     * @throws ClassNotFoundException when the database driver cannot be loaded.
     * @throws SQLException when the database cannot be initialized.
     */
    public LogDaoDbManager(final File where) throws ClassNotFoundException, SQLException
    {
        this.where = new File(where, "db");

        migrate();

        PooledDataSource dataSource = new PooledDataSource(
                "org.h2.Driver",
                "jdbc:h2:"+this.where.getAbsolutePath()+";IFEXISTS=TRUE;ACCESS_MODE_DATA=rwd",
                DBADMIN_USER,
                DBADMIN_PASS
        );

        final TransactionFactory transactionFactory = new JdbcTransactionFactory();
        final Environment        environment        = new Environment("jiraffet", transactionFactory, dataSource);
        final Configuration      configuration      = new Configuration(environment);

        // Add our custom type handlers.
        configuration.getTypeHandlerRegistry().register(
                java.io.InputStream.class,
                BlobTypeHandler.class
        );
        configuration.getTypeHandlerRegistry().register(
                byte[].class,
                ByteArrayBlobTypeHandler.class
        );

        configuration.addMapper(LogMapper.class);

        sqlSessionManager =
                SqlSessionManager.newInstance(
                        new SqlSessionFactoryBuilder().build(configuration));
    }

    private void migrate() throws ClassNotFoundException, SQLException
    {
        Class.forName("org.h2.Driver");

        final Flyway flyway = new Flyway();
        flyway.setDataSource(
                "jdbc:h2:"+where.getAbsolutePath()+";IFEXISTS=FALSE;ACCESS_MODE_DATA=rwd",
                DBADMIN_USER,
                DBADMIN_PASS
        );
        // Migrations for our configuration database.
        flyway.setLocations("/db/store/migrations");
        flyway.migrate();
    }

    public LogDaoMyBatis getLogDao() {
        return new LogDaoMyBatis(sqlSessionManager);
    }

    @Override
    public void close() throws Exception {
        if (sqlSessionManager.isManagedSessionStarted()) {
            sqlSessionManager.close();
        }
    }
}
