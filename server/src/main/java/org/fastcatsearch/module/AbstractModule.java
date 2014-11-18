package org.fastcatsearch.module;

import org.fastcatsearch.env.Environment;
import org.fastcatsearch.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractModule {

	protected static final Logger logger = LoggerFactory.getLogger(AbstractModule.class);

	protected Environment environment;
	protected Settings settings;
	protected boolean isLoaded;

	public AbstractModule(Environment environment, Settings settings) {
		this.environment = environment;
		this.settings = settings;
	}

	public synchronized boolean load() throws ModuleException {
		if(isLoaded) {
			logger.info("Module is already loaded. {}", toString());
			return false;
		}
		if (doLoad()) {
			logger.info("Load module {}", toString());
			isLoaded = true;
			return true;

		} else {
			return false;
		}

	}

	public synchronized boolean unload() throws ModuleException {
		if (!isLoaded) {
			logger.info("Module is not loaded. {}", toString());
			return false;
		}
		if (doUnload()) {
			logger.info("Unload module {}", toString());
			isLoaded = false;
			return true;

		} else {
			return false;
		}
	}

	protected abstract boolean doLoad() throws ModuleException;

	protected abstract boolean doUnload() throws ModuleException;

	public Settings settings() {
		return settings;
	}
}
