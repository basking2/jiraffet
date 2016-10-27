package com.github.basking2.jiraffet.db;

import org.apache.ibatis.type.TypeHandler;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Blob;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedJdbcTypes;

public class BlobTypeHandler implements TypeHandler<InputStream>
{

    /**
     * {@inheritDoc}
     */
    @Override
    public void setParameter(PreparedStatement ps, int i, InputStream parameter, JdbcType jdbcType)
            throws SQLException
    {
        ps.setBlob(i, parameter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getResult(ResultSet rs, String columnName)
            throws SQLException
    {
        final Blob blob = rs.getBlob(columnName);

        if (blob == null)
        {
            return null;
        }

        return blob.getBinaryStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getResult(ResultSet rs, int columnIndex)
            throws SQLException
    {
        final Blob blob = rs.getBlob(columnIndex);

        if (blob == null)
        {
            return null;
        }

        return blob.getBinaryStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getResult(CallableStatement cs, int columnIndex)
            throws SQLException
    {
        final Blob blob = cs.getBlob(columnIndex);

        if (blob == null)
        {
            return null;
        }

        return blob.getBinaryStream();
    }
}
