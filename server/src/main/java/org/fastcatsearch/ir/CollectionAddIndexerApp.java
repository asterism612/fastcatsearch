package org.fastcatsearch.ir;

import org.fastcatsearch.env.Environment;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.plugin.PluginService;
import org.fastcatsearch.service.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionAddIndexerApp {
	
	protected static final Logger logger = LoggerFactory.getLogger(CollectionAddIndexerApp.class);
			
	private Environment environment;
	

	// "/Users/swsong/TEST_HOME/fastcatsearch2_shard/node1/collections/"
	public static void main(String[] args) throws FastcatSearchException {
		if(args.length < 2){
			printUsage();
			System.exit(1);
		}
		String home = args[0];
		String collectionId = args[1];
		CollectionAddIndexerApp app = new CollectionAddIndexerApp(home);
		app.doIndexing(home, collectionId);
	}

	private static void printUsage(){
		System.out.println("Usage: CollectionFullIndexerApp [HOME_PATH] [COLLECTION_ID]");
	}
	
	ServiceManager serviceManager;
	
	public CollectionAddIndexerApp(String homeDirPath) throws FastcatSearchException {
		environment = new Environment(homeDirPath).init();
		serviceManager = new ServiceManager(environment);
		serviceManager.asSingleton();

		PluginService pluginService = serviceManager.createService("plugin", PluginService.class);
		pluginService.start();
		
		IRService irService = serviceManager.createService("ir", IRService.class);
		irService.start();
	}
	
	private void doIndexing(String home, String collectionId) {
		try {
			
			IRService irService = serviceManager.getService(IRService.class);
			CollectionHandler collectionHandler = irService.collectionHandler(collectionId);
			
//			IndexFilePaths collectionFilePaths = environment.filePaths().collectionFilePaths(collectionId);
//			Collection collection = new Collection("sample", true);
//			CollectionContext collectionContext = CollectionContextUtil.load(collection, collectionFilePaths);
//			System.out.println(collectionContext.schema().getFieldSetting("id"));
//			for (ShardContext shardContext : collectionContext.getShardContextList()) {
//				System.out.println(shardContext.shardConfig().getFilter());
//			}
			
			CollectionAddIndexer indexer = null;//new CollectionAddIndexer(collectionHandler);

			try {
				indexer.doIndexing();
			} finally {
				if (indexer != null) {
					indexer.close();
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
