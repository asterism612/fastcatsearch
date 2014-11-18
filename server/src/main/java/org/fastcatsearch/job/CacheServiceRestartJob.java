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

package org.fastcatsearch.job;


import org.fastcatsearch.ir.IRService;
import org.fastcatsearch.service.ServiceManager;

public class CacheServiceRestartJob extends Job{
	private int delay;
	
	public CacheServiceRestartJob(){ 
		delay = 1000; //1초.
	}
	
	public CacheServiceRestartJob(int delay){ 
		this.delay = delay;
	}
	
	@Override
	public JobResult doRun() {
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) { }
		
		IRService irService = ServiceManager.getInstance().getService(IRService.class);
		
		boolean result = irService.searchCache().unload();
		result = irService.groupingCache().unload() && result;
		result = irService.documentCache().unload() && result;
		result = irService.searchCache().load() && result;
		result = irService.groupingCache().load() && result;
		result = irService.documentCache().load() && result;
		
		return new JobResult(result);
	}

}
