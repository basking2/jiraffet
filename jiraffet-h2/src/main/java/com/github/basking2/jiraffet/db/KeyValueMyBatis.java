package com.github.basking2.jiraffet.db;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 */
public class KeyValueMyBatis {
    private static final Logger LOG = LoggerFactory.getLogger(LogMyBatis.class);

    private SqlSessionManager sqlSessionManager;

    public KeyValueMyBatis(final SqlSessionManager sqlSessionManager) {
        this.sqlSessionManager = sqlSessionManager;
    }

    public byte[] get(final String key) {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final KeyValueMapper mapper = session.getMapper(KeyValueMapper.class);
            final List<byte[]> data = mapper.get(key);
            if (data.isEmpty()) {
                return null;
            }
            else {
                return data.get(0);
            }
        }
    }

    public void put(final String key, final byte[] data) {
        put(key, "application/octet-stream", data);
    }

    public void put(final String key, final String type, final byte[] data) {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final KeyValueMapper mapper = session.getMapper(KeyValueMapper.class);
            mapper.put(key, type, data);
            session.commit();
        }
    }

    public void delete(final String key) {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            final KeyValueMapper mapper = session.getMapper(KeyValueMapper.class);
            mapper.delete(key);
            session.commit();
        }
    }

    public void clear() {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            session.getMapper(KeyValueMapper.class).clear();
            session.commit();
        }
    }

    public KeyValueEntry getEntry(final String key) {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            List<KeyValueEntry> entries = session.getMapper(KeyValueMapper.class).getEntry(key);
            if (entries.isEmpty()) {
                return null;
            }
            else {
                return entries.get(0);
            }
        }
    }

    public void putEntry(final KeyValueEntry entry) {
        try(final SqlSession session = sqlSessionManager.openSession()) {
            session.getMapper(KeyValueMapper.class).putEntry(entry);
            session.commit();
        }
    }
}
