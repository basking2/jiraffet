package com.github.basking2.jiraffet.db;

import java.io.IOException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

public class ByteArrayBlobTypeHandler implements TypeHandler<byte[]> {

    @Override
    public void setParameter(PreparedStatement ps, int i, byte[] parameter, JdbcType jdbcType) throws SQLException {
        ps.setBytes(i, parameter);
    }

    @Override
    public byte[] getResult(ResultSet rs, String columnName) throws SQLException {
        final Blob blob = rs.getBlob(columnName);

        if (blob == null)
        {
            return null;
        }

        try {
            return IOUtils.toByteArray(blob.getBinaryStream());
        }
        catch (final IOException e) {
            throw new SQLException("Reading blob.", e);
        }
    }

    @Override
    public byte[] getResult(ResultSet rs, int columnIndex) throws SQLException {
        final Blob blob = rs.getBlob(columnIndex);

        if (blob == null)
        {
            return null;
        }

        try {
            return IOUtils.toByteArray(blob.getBinaryStream());
        }
        catch (final IOException e) {
            throw new SQLException("Reading blob.", e);
        }
    }

    @Override
    public byte[] getResult(CallableStatement cs, int columnIndex) throws SQLException {
        final Blob blob = cs.getBlob(columnIndex);

        if (blob == null)
        {
            return null;
        }

        try {
            return IOUtils.toByteArray(blob.getBinaryStream());
        }
        catch (final IOException e) {
            throw new SQLException("Reading blob.", e);
        }
    }
}
