package com.github.basking2.jiraffet.db;

import org.apache.ibatis.annotations.*;
import org.apache.ibatis.type.JdbcType;

import java.sql.Blob;
import java.util.List;

/**
 * Store and load key values.
 */
public interface KeyValueMapper {


    @Select("SELECT data FROM keyvalue WHERE id = #{value} LIMIT 1")
    @Result(
            column="data",
            jdbcType= JdbcType.BLOB,
            javaType=byte[].class,
            typeHandler=ByteArrayBlobTypeHandler.class
    )
    List<byte[]> get(String key);

    @Insert("DELETE FROM keyvalue WHERE id = #{value}")
    void delete(String key);

    @Insert("INSERT INTO keyvalue (id, type, data) VALUES (#{id}, #{type}, #{data})")
    void put(@Param("id") String key, @Param("type") String type, @Param("data") byte[] data);

    @Delete("DELETE FROM keyvalue")
    void clear();

    @Select("SELECT id, type, data FROM keyvalue WHERE id = #{value}")
    @ConstructorArgs({
            @Arg(column = "id", javaType=String.class),
            @Arg(column = "type", javaType=String.class),
            @Arg(column = "data", javaType=byte[].class)
    })
    List<KeyValueEntry> getEntry(final String id);

    @Insert("INSERT INTO keyvalue (id, type, data) VALUES (#{id}, #{type}, #{data})")
    void putEntry(KeyValueEntry keyValueEntry);
}
