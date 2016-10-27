package com.github.basking2.jiraffet.db;

import com.github.basking2.jiraffet.LogDao;
import org.apache.ibatis.annotations.*;

public interface LogMapper {

    @Update("UPDATE CURRENT_TERM SET term = #{value}")
    void setCurrentTerm(int currentTerm);

    @Select("SELECT term FROM CURRENT_TERM LIMIT 1")
    Integer getCurrentTerm();

    @Update("UPDATE VOTED_FOR SET id = #{value}")
    void setVotedFor(String id);

    @Select("SELECT id FROM VOTED_FOR LIMIT 1")
    String getVotedFor();

    @Select("SELECT index, term FROM ENTRIES WHERE index = #{value}")
    @ConstructorArgs({
            @Arg(column = "term", javaType=Integer.class),
            @Arg(column = "index", javaType=Integer.class)
    })
    LogDao.EntryMeta getMeta(int index);

    @Select("SELECT data FROM ENTRIES WHERE index = #{value}")
    byte[] read(int index);

    @Select("SELECT COUNT(1) FROM ENTRIES WHERE index = #{index} AND term = #{term}")
    Integer hasEntry(@Param("index") int index, @Param("term") int term);

    @Select("INSERT INTO ENTRIES (index, term, data) VALUES (#{index}, #{term}, #{data})")
    void write(@Param("term") int term, @Param("index") int index, @Param("data") byte[] data);

    @Delete("DELETE FROM ENTRIES WHERE index >= #{value}")
    void remove(int i);

    @Update("UPDATE ENTRIES SET applied = true WHERE index <= #{value}")
    void apply(int index);

    @Select("SELECT index, term FROM ENTRIES ORDER BY index ASC LIMIT 1")
    @ConstructorArgs({
            @Arg(column = "term", javaType=Integer.class),
            @Arg(column = "index", javaType=Integer.class)
    })
    LogDao.EntryMeta last();

}
