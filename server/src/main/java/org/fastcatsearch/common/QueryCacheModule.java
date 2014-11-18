/*
 * Copyright (c) 2013 Websquared, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 * 
 * Contributors:
 *     swsong - initial API and implementation
 */

package org.fastcatsearch.common;

import org.fastcatsearch.env.Environment;
import org.fastcatsearch.module.AbstractModule;
import org.fastcatsearch.module.ModuleException;
import org.fastcatsearch.settings.Settings;
import org.fastcatsearch.util.LRUCache;

/**
 * */
public class QueryCacheModule<K, V> extends AbstractModule {

	private LRUCache<K, V> lruCache;
	private String name;
	
	public QueryCacheModule(String name, Environment environment, Settings settings) {
		super(environment, settings);
		this.name = name;
	}

	@Override
	protected boolean doLoad() throws ModuleException {
		int maxCacheSize = settings.getInt("search-cache-size", 1000);
		lruCache = new LRUCache<K, V>(maxCacheSize);
		return true;
	}

	@Override
	protected boolean doUnload() {
		lruCache.close();
		return true;
	}

	public void put(K key, V value) {
		lruCache.put(key, value);
	}

	public V get(K key) {
		return lruCache.get(key);
	}

	public int size() {
		return lruCache.size();
	}
	
	@Override
	public String toString() {
		return name;
	}
}
