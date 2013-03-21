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

package org.fastcatsearch.service;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.fastcatsearch.FastcatSearchEnv;
import org.fastcatsearch.cli.ConsoleActionServlet;
import org.fastcatsearch.ir.config.IRConfig;
import org.fastcatsearch.ir.config.IRSettings;
import org.fastcatsearch.servlet.DocumentListServlet;
import org.fastcatsearch.servlet.DocumentSearchServlet;
import org.fastcatsearch.servlet.PopularKeywordServlet;
import org.fastcatsearch.servlet.SearchServlet;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ServiceHandler extends CatServiceComponent{
	private static Logger logger = LoggerFactory.getLogger(ServiceHandler.class);
	
	//always use Root context
	private String SERVICE_CONTEXT = "/search";
	private String KEYWORD_CONTEXT = "/keyword";
	private String DOCUMENT_LIST_CONTEXT = "/doclist";
	private String DOCUMENT_SEARCH_CONTEXT = "/docsearch";
	private String CONSOLE_CONTEXT = "/console";
	
	private Server server;
	private int SERVER_PORT;
	private static ServiceHandler instance;
	
	public static ServiceHandler getInstance(){
		
		if(instance == null) {
			instance = new ServiceHandler();
		}
		return instance;
	}
	//WAS내장시에는 서블릿을 web.xml에 설정하지 않고 코드내에 설정한다.  
	private ServiceHandler() {
		
	}
	
	public static void main(String[] args) throws Exception {
		ServiceHandler s = new ServiceHandler();
		s.start();
		logger.info("ServiceHandler started!");
	}
	
	protected boolean start0() throws ServiceException{
		IRConfig config = IRSettings.getConfig();
		if(System.getProperty("server.port")!=null) {
			SERVER_PORT = Integer.parseInt(System.getProperty("server.port"));
		} else {
			SERVER_PORT = config.getInt("server.port");
		}
		
		server = new Server(SERVER_PORT);
		HandlerList handlerList = new HandlerList();
		
		// Search ServletContextHandler
		final Context context = new Context(server, SERVICE_CONTEXT, Context.SESSIONS);
		context.setMaxFormContentSize(10 * 1024 * 1024); //파라미터전송 10MB까지 가능.
		context.addServlet(new ServletHolder(new SearchServlet(SearchServlet.JSON_TYPE)),"/json");
		context.addServlet(new ServletHolder(new SearchServlet(SearchServlet.JSONP_TYPE)),"/jsonp");
		context.addServlet(new ServletHolder(new SearchServlet(SearchServlet.XML_TYPE)),"/xml");
		context.addServlet(new ServletHolder(new SearchServlet(SearchServlet.IS_ALIVE)),"/isAlive");
		handlerList.addHandler(context);
		
        // ServletContextHandler
		final Context context3 = new Context(server, KEYWORD_CONTEXT, Context.SESSIONS);
		context3.addServlet(new ServletHolder(new PopularKeywordServlet()),"/popular");
		context3.addServlet(new ServletHolder(new PopularKeywordServlet(PopularKeywordServlet.JSON_TYPE)),"/popular/json");
		context3.addServlet(new ServletHolder(new PopularKeywordServlet(PopularKeywordServlet.JSONP_TYPE)),"/popular/jsonp");
		context3.addServlet(new ServletHolder(new PopularKeywordServlet(PopularKeywordServlet.XML_TYPE)),"/popular/xml");
		handlerList.addHandler(context3);
		
        // DOCUMENT_LIST_CONTEXT
		final Context context4 = new Context(server, DOCUMENT_LIST_CONTEXT, Context.SESSIONS);
		context4.addServlet(new ServletHolder(new DocumentListServlet(DocumentListServlet.JSON_TYPE)),"/json");
		context4.addServlet(new ServletHolder(new DocumentListServlet(DocumentListServlet.XML_TYPE)),"/xml");
		handlerList.addHandler(context4);
		
		// DOCUMENT_SEARCH_CONTEXT
		final Context context5 = new Context(server, DOCUMENT_SEARCH_CONTEXT, Context.SESSIONS);
		context5.addServlet(new ServletHolder(new DocumentSearchServlet(DocumentSearchServlet.JSON_TYPE)),"/json");
		context5.addServlet(new ServletHolder(new DocumentSearchServlet(DocumentSearchServlet.XML_TYPE)),"/xml");
		handlerList.addHandler(context5);
		
		final Context context6 = new Context(server, CONSOLE_CONTEXT, Context.SESSIONS);
		context6.addServlet(new ServletHolder(new ConsoleActionServlet()), "/command");
		handlerList.addHandler(context6);
		
        server.setHandler(handlerList);
        
		try {
			//서버는 예외가 발생해도 시작처리되므로 미리 running 표시필요.
			isRunning = true;
			//stop을 명령하면 즉시 중지되도록.
			server.setStopAtShutdown(true);
			server.start();
			//Jetty는 2초후에 정지된다.
			if( server.getThreadPool() instanceof QueuedThreadPool ){
			   ((QueuedThreadPool) server.getThreadPool()).setMaxIdleTimeMs( 2000 );
			}
			logger.info("ServiceHandler Started! port = "+SERVER_PORT);
		} catch (Exception e) {
			throw new ServiceException(SERVER_PORT+" PORT로 웹서버 시작중 에러발생. ", e);
			
		}
		return true;
	}
	
	protected boolean shutdown0() throws ServiceException{
		try {
			logger.info("ServiceHandler stop requested...");
			server.stop();
			logger.info("Server Stop Ok!");
		} catch (Exception e) {
			throw new ServiceException(e);
		}
		return true;
	}

	public int getClientCount() {
		return server.getConnectors().length;
	}
}