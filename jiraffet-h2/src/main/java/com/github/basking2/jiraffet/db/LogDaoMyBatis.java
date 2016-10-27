package com.github.basking2.jiraffet.db;

import java.io.IOException;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionManager;

import com.github.basking2.jiraffet.LogDao;

/**
 */
public class LogDaoMyBatis implements LogDao {

    private SqlSessionManager sqlSessionManager;

    public LogDaoMyBatis(final SqlSessionManager sqlSessionManager) {
        this.sqlSessionManager = sqlSessionManager;
    }

    @Override
    public void setCurrentTerm(int currentTerm) throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            mapper.setCurrentTerm(currentTerm);
            session.commit();
        }
    }

    @Override
    public int getCurrentTerm() throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            return mapper.getCurrentTerm();
        }
    }

    @Override
    public void setVotedFor(String id) throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            mapper.setVotedFor(id);
            session.commit();
        }
    }

    @Override
    public String getVotedFor() throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            return mapper.getVotedFor();
        }
    }

    @Override
    public EntryMeta getMeta(int index) throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            return mapper.getMeta(index);
        }
    }

    @Override
    public byte[] read(int index) throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            return mapper.read(index);
        }
    }

    @Override
    public boolean hasEntry(int index, int term) throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            return mapper.hasEntry(index, term) != 0;
        }
    }

    @Override
    public void write(int term, int index, byte[] data) throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            mapper.write(term, index, data);
            session.commit();
        }
    }

    @Override
    public void remove(int i) throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            mapper.remove(i);
            session.commit();
        }
    }

    @Override
    public void apply(int index) throws IllegalStateException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            mapper.apply(index);
            session.commit();
        }
    }

    @Override
    public EntryMeta last() throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            return mapper.last();
        }
    }
}
