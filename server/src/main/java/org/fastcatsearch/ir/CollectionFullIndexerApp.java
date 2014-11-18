package org.fastcatsearch.ir;

import org.fastcatsearch.env.Environment;
import org.fastcatsearch.exception.FastcatSearchException;
import org.fastcatsearch.ir.analysis.AnalyzerPoolManager;
import org.fastcatsearch.ir.config.CollectionContext;
import org.fastcatsearch.ir.config.CollectionsConfig.Collection;
import org.fastcatsearch.plugin.PluginService;
import org.fastcatsearch.service.ServiceManager;
import org.fastcatsearch.util.CollectionContextUtil;
import org.fastcatsearch.util.FilePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionFullIndexerApp {

	protected static final Logger logger = LoggerFactory.getLogger(CollectionFullIndexerApp.class);

	private Environment environment;

	// "/Users/swsong/TEST_HOME/fastcatsearch2_shard/node1/collections/"
	public static void main(String[] args) throws FastcatSearchException {
		if (args.length < 2) {
			printUsage();
			System.exit(1);
		}
		String home = args[0];
		String collectionId = args[1];
		CollectionFullIndexerApp app = new CollectionFullIndexerApp(home);
		app.doIndexing(home, collectionId);
	}

	private static void printUsage() {
		System.out.println("Usage: CollectionFullIndexerApp [HOME_PATH] [COLLECTION_ID]");
	}

	public CollectionFullIndexerApp(String homeDirPath) throws FastcatSearchException {
		environment = new Environment(homeDirPath).init();
		ServiceManager serviceManager = new ServiceManager(environment);
		serviceManager.asSingleton();

		PluginService pluginService = serviceManager.createService("plugin", PluginService.class);
		pluginService.start();
	}

	private void doIndexing(String home, String collectionId) {
		try {
			FilePaths collectionFilePaths = environment.filePaths().collectionFilePaths(collectionId);
			Collection collection = new Collection("sample");
			CollectionContext collectionContext = CollectionContextUtil.load(collection, collectionFilePaths);
			System.out.println(collectionContext.schema().getFieldSetting("id"));
			AnalyzerPoolManager analyzerPoolManager = new AnalyzerPoolManager();

			CollectionFullIndexer indexer = new CollectionFullIndexer(null, collectionContext, analyzerPoolManager);

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
