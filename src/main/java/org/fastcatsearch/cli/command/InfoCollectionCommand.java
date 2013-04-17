package org.fastcatsearch.cli.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

import org.fastcatsearch.cli.Command;
import org.fastcatsearch.cli.CommandException;
import org.fastcatsearch.cli.CommandResult;
import org.fastcatsearch.cli.ConsoleSessionContext;
import org.fastcatsearch.ir.config.IRSettings;
import org.fastcatsearch.ir.config.Schema;
import org.fastcatsearch.ir.search.CollectionHandler;
import org.fastcatsearch.ir.util.Formatter;
import org.fastcatsearch.service.IRService;

public class InfoCollectionCommand extends CollectionExtractCommand {

	private String[] header = new String[] { "collection name", "total field count", "indexing field count",
			"fileter field count", "group field count", "sort field count", "data source Type", "status", "start date",
			"run time" };

	private ArrayList<Object[]> data = new ArrayList<Object[]>();

	@Override
	public boolean isCommand(String[] cmd) {
		return isCommand(CMD_INFO_COLLECTION, cmd);
	}

	@Override
	public CommandResult doCommand(String[] cmd, ConsoleSessionContext context) throws IOException, CommandException {
		if ((cmd.length == 0) || (cmd.length > 2)) {
			return new CommandResult("invalid Command", CommandResult.Status.SUCCESS);
		} else {
			String inputCollection = "";

			if (cmd.length == 2)
				inputCollection = cmd[1].trim();
			else if (cmd.length == 1)
				inputCollection = ((String) context.getAttribute(SESSION_KEY_USING_COLLECTION)).trim();

			TreeSet ts = getCollectionList();

			if (ts.size() == 0) {
				return new CommandResult("there is no Collection", CommandResult.Status.SUCCESS);
			}

			// 전체 조회가 없다.
			// if (inputCollection.trim().equalsIgnoreCase("_ALL_")) {// 전체 조회
			// for (String collection : ts) {
			// if (getCollectionData(collection) == false) {
			// return new CommandResult("error happen while loading collection["
			// + collection + "]",
			// CommandResult.Status.SUCCESS);
			// }
			// }
			// return new CommandResult(printData(data, header),
			// CommandResult.Status.SUCCESS);
			//
			// } else
			boolean bExists = isCollectionExists(inputCollection);
			if (bExists == false) {
				// 컬렉션은 입력 됐는데 현재 컬렉션 리스트에 없을 때.
				return new CommandResult("collection " + inputCollection + " is not exists",
						CommandResult.Status.SUCCESS);
			} else {
				// 입력된 컬렉션이 현재 컬렉션 리스트에 있을 때
				if (getCollectionData(inputCollection))
					return new CommandResult(printData(data, header), CommandResult.Status.SUCCESS);
				else
					return new CommandResult("invalid Command", CommandResult.Status.SUCCESS);
			}
		}
	}

	private boolean getCollectionData(String collection) {

		Schema schema = null;
		try {
			schema = IRSettings.getSchema(collection, true);
		} catch (Exception e) {
			return false;
		}
		String dataSourceType = IRSettings.getDatasource(collection, true).sourceType;

		CollectionHandler ch = IRService.getInstance().getCollectionHandler(collection);
		boolean isRunning = (ch == null ? false : true);
		String durationStr = "";
		String strStartTime = "";
		if (ch != null) {
			long startTime = ch.getStartedTime();
			long duration = System.currentTimeMillis() - startTime;
			strStartTime = new Date(startTime).toString();
			durationStr = Formatter.getFormatTime(duration);
		}

		addRecord(data, collection, schema.getFieldSettingList().size() + "", schema.getIndexSettingList().size() + "",
				schema.getFilterSettingList().size() + "", schema.getGroupSettingList().size() + "", schema
						.getSortSettingList().size() + "", dataSourceType, (isRunning ? "Running" : "stop"),
				strStartTime, durationStr);

		return true;
	}

	private void addRecord(List<Object[]> data, String cn, String ftc, String ifc, String ffc, String gfc, String sfc,
			String dst, String status, String sdate, String runtime) {
		data.add(new Object[] { cn, ftc, ifc, ffc, gfc, sfc, dst, status, sdate, runtime });
	}

	// collection name", "total field count", "indexing field count",	"fileter
	// field count", "group field count", "sort field count", "data source
	// Type", "status", "start date",	"run time"
}