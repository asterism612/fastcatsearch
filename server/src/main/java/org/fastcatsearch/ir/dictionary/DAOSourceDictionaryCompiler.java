package org.fastcatsearch.ir.dictionary;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.fastcatsearch.db.dao.DictionaryDAO;
import org.fastcatsearch.plugin.analysis.AnalysisPluginSetting.ColumnSetting;

public class DAOSourceDictionaryCompiler {

	private static final int BULK_SIZE = 500;

	public static void compile(File targetFile, DictionaryDAO dictionaryDAO, SourceDictionary dictionaryType, List<ColumnSetting> columnList)
			throws Exception {

		int count = dictionaryDAO.getCount(null);

		int start = 1;

		String keyColumnName = null;
		boolean keyIgnoreCase = false;
		List<String> valueColumnNames = new ArrayList<String>();
		List<Boolean> ignoreCaseList = new ArrayList<Boolean>();
		
		// key는 설정안해도 무조건 isCompilable이다.
		for (int i = 0; i < columnList.size(); i++) {
			ColumnSetting columnSetting = columnList.get(i);
			if (columnSetting.isCompilable() && !columnSetting.isKey()) {
				valueColumnNames.add(columnSetting.getName());
				ignoreCaseList.add(columnSetting.isIgnoreCase());
			}
			if (columnSetting.isKey()) {
				keyColumnName = columnSetting.getName();
				keyIgnoreCase =columnSetting.isIgnoreCase(); 
			}
		}
		boolean[] valuesIgnoreCase = new boolean[ignoreCaseList.size()];
		for(int i=0;i<ignoreCaseList.size(); i++){
			valuesIgnoreCase[i] = ignoreCaseList.get(i);
		}
		
		while (start <= count) {
			int end = start + BULK_SIZE;

			List<Map<String, Object>> result = dictionaryDAO.getEntryList(start, end, null, null);
			for (int i = 0; i < result.size(); i++) {
				Map<String, Object> vo = result.get(i);

				Object[] values = new Object[valueColumnNames.size()];
				String key = vo.get(keyColumnName).toString();
				for (int j = 0; j < valueColumnNames.size(); j++) {
					String columnName = valueColumnNames.get(j);
					
					values[j] = vo.get(columnName).toString();
				}
				dictionaryType.addEntry(key, values, keyIgnoreCase, valuesIgnoreCase);
			}

			if (result.size() < BULK_SIZE) {
				// 다 읽어온 것임.
				break;
			}
			start += BULK_SIZE;
		}
		OutputStream out = null;
		try {
			out = new FileOutputStream(targetFile);
			dictionaryType.writeTo(out);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException ignore) {
				}
			}
		}

	}
}
