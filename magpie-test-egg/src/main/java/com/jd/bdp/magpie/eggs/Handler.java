package com.jd.bdp.magpie.eggs;

import com.jd.bdp.magpie.MagpieExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by phoenix on 2015-04-14.
 */
public class Handler implements MagpieExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(Handler.class);
    private String id = null;

	@Override
	public void prepare(String id) throws Exception {
        LOG.info("start to prepare " + id);
        this.id = id;
        Thread.sleep(3000);
        LOG.info("end to prepare " + id);
	}

	@Override
	public void reload(String id) throws Exception {
        LOG.info("start to reload " + id);
        Thread.sleep(3000);
        LOG.info("end to reload " + id);
	}

	@Override
	public void pause(String id) throws Exception {
        LOG.info("start to pause " + id);
        Thread.sleep(3000);
        LOG.info("end to pause " + id);
	}

	@Override
	public void close(String id) throws Exception {
        LOG.info("start to close " + id);
        Thread.sleep(3000);
        LOG.info("end to close " + id);
	}

	@Override
	public void run() throws Exception {
        LOG.info("start to run " + this.id);
        Thread.sleep(3000);
        LOG.info("end to run " + this.id);
	}

}