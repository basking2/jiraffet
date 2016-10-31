package com.github.basking2.jiraffetdb.dao;

import com.github.basking2.jiraffetdb.util.KeyValue;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionException;
import org.apache.ibatis.session.SqlSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class KeyValueDao {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueDao.class);

    private SqlSessionManager sqlSessionManager;

    public KeyValueDao(final SqlSessionManager sqlSessionManager) {
        this.sqlSessionManager = sqlSessionManager;
    }

    public byte[] get(final String key) {
        try (final SqlSession session = sqlSessionManager.openSession()) {
            final KeyValueMapper mapper = session.getMapper(KeyValueMapper.class);
            return mapper.get(key);
        }
    }

    public void set(final KeyValue kv, int index) {
        set(kv.getKey(), index, kv.getValue());
    }

    public void set(final String key, final Integer index, final byte[] value) {
        try (final SqlSession session = sqlSessionManager.openSession()) {
            final KeyValueMapper mapper = session.getMapper(KeyValueMapper.class);
            try {
                mapper.insert(key, index, value);
            }
            catch (final SqlSessionException e) {
                LOG.error("Inserting key {}. Retrying.", key, e);
                mapper.remove(key);
                mapper.insert(key, index, value);
            }
        }
    }

}
