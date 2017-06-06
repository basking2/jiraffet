package com.github.basking2.jiraffet.db;

import org.apache.ibatis.annotations.*;
import org.apache.ibatis.type.JdbcType;

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

    @Insert("INSERT INTO keyvalue (id, data) VALUES (#{id}, #{data})")
    void put(@Param("id") String key, @Param("data") byte[] data);

    @Delete("DELETE FROM keyvalue")
    void clear();
}
