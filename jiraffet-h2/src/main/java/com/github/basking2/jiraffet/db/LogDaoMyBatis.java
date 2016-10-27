package com.github.basking2.jiraffet.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.LogDao;

/**
 */
public class LogDaoMyBatis implements LogDao {
    
    private static final Logger LOG = LoggerFactory.getLogger(LogDaoMyBatis.class);

    private SqlSessionManager sqlSessionManager;
    
    private List<Applier> appliers;

    public LogDaoMyBatis(final SqlSessionManager sqlSessionManager) {
        this.sqlSessionManager = sqlSessionManager;
        this.appliers = new ArrayList<Applier>();
    }

    @Override
    public void setCurrentTerm(int currentTerm) throws IOException {
        LOG.info("Setting current term to {}", currentTerm);
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
    public void setVotedFor(final String id) throws IOException {
        LOG.info("Setting voted for to {}", id);
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
            final List<byte[]> result = mapper.read(index);
            
            if (result.size() == 0) {
                return null;
            }
            else {
                return result.get(0);
            }
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
        LOG.info("Wrinting entry term/index {}/{}", term, index);
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            mapper.write(term, index, data);
            session.commit();
        }
    }

    @Override
    public void remove(int i) throws IOException {
        LOG.info("Removing index and all following {}.", i);
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            mapper.remove(i);
            session.commit();
        }
    }

    @Override
    public void apply(int index) throws IllegalStateException {
        LOG.info("Applying up to index {}", index);
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            Integer lastApplied = mapper.getLastApplied();
            
            if (lastApplied == null) {
                lastApplied = 0;
            }

            for (int i = lastApplied; i < index; ++i) {

                try {
                    for (final Applier applier : appliers) {
                        applier.apply(i);
                    }
                    mapper.apply(i);
                }
                catch (final Exception e) {
                    LOG.error("Failed to apply log.", e);
                }
            }

            session.commit();
        }
    }

    @Override
    public EntryMeta last() throws IOException {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final LogMapper mapper = session.getMapper(LogMapper.class);
            final EntryMeta meta = mapper.last();
            if (meta == null) {
                return new EntryMeta(0,0);
            }
            else {
                return meta;
            }
        }
    }
    
    @FunctionalInterface
    public interface Applier {
        void apply(int index) throws Exception;
    }
}
