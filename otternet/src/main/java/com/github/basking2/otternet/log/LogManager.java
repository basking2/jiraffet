package com.github.basking2.otternet.log;

import com.github.basking2.jiraffet.LogDao;
import com.github.basking2.jiraffet.db.LogDaoMyBatis;

/**
 */
public class LogManager implements LogDaoMyBatis.Applier {
    final private LogDao log;

    public LogManager(final LogDao log) {
        this.log = log;
    }

    @Override
    public void apply(int index) throws Exception {
        final byte[] msg = log.read(index);

        // FIXME - parse message and take action.
    }
}
