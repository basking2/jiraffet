package com.github.basking2.jiraffetdb.dao;

import org.apache.ibatis.annotations.*;

/**
 */
public interface KeyValueMapper {

    @Select("SELECT value FROM key_values WHERE key = #{value}")
    byte[] get(String name);

    @Insert( "INSERT INTO key_values (key, index, value) VALUES (#{key}, #{index}, #{value})")
    void insert(@Param("key") String key, @Param("index") Integer index, @Param("value") byte[] value);

    @Delete("DELETE FROM key_values WHERE name = #{value}")
    void remove(String key);

    @Update("UPDATE key_values SET value = #{value} WHERE key = #{key}")
    Integer update(@Param("key") String key, @Param("value") byte[] value);
}
