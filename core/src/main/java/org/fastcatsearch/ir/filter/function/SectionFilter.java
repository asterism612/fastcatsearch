/*
 * Copyright 2013 Websquared, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fastcatsearch.ir.filter.function;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.fastcatsearch.ir.filter.FilterException;
import org.fastcatsearch.ir.filter.FilterFunction;
import org.fastcatsearch.ir.io.DataRef;
import org.fastcatsearch.ir.query.Filter;
import org.fastcatsearch.ir.query.RankInfo;
import org.fastcatsearch.ir.settings.FieldIndexSetting;
import org.fastcatsearch.ir.settings.FieldSetting;

/**
 * 범위조건을 지정할수 있다. ~를 구분자로 시작과 끝범위를 지정하며, ;를 사용하여 여러범위를 포함할수있다. 시작패턴과 끝패턴이 함께
 * 입력되어야 하며, 1000~ 와 같은 표현을 지원하지 않는다.
 * 
 * @author swsong
 * 
 */
public class SectionFilter extends FilterFunction {

	public SectionFilter(Filter filter, FieldIndexSetting fieldIndexSetting, FieldSetting fieldSetting) throws FilterException {
		super(filter, fieldIndexSetting, fieldSetting, false);
	}

	public SectionFilter(Filter filter, FieldIndexSetting fieldIndexSetting, FieldSetting fieldSetting, boolean isBoostFunction) throws FilterException {
		super(filter, fieldIndexSetting, fieldSetting, isBoostFunction);
	}

	@Override
	public boolean filtering(RankInfo rankInfo, DataRef dataRef) throws IOException {
		
		while (dataRef.next()) {
			
			BytesRef bytesRef = dataRef.bytesRef();
			for (int j = 0; j < patternCount; j++) {
				BytesRef patternBuf1 = patternList[j];
				BytesRef patternBuf2 = endPatternList[j];

				//크기비교에서 문자열과 숫자형은 비교방식이 다르므로 다른 루틴을 사용하도록 함.
				//숫자 : 
				// 숫자는 MSB 비교가 우선이 되어야 함. 패턴크기는 항상 같음 (캐스팅되어 사용하기 때문에 항상 같은 바이트 수가 나옴)
				// 1. 패턴크기가 같은경우 : 앞에서부터 순차비교
				//문자 :
				// 1. 패턴크기가 같은경우 : 앞에서부터 순차비교
				// 2. 패턴크기가 다른경우 : 앞에서부터 순차비교, 짧은 패턴에 맞춤. 남는패턴이 있는쪽이 큼
				if (fieldSetting.isNumericField()) {

					if ((patternBuf1 == null || compareNumeric(bytesRef, bytesRef.length(), patternBuf1, patternBuf1.length()) >= 0) &&
						(patternBuf2 == null || compareNumeric(bytesRef, bytesRef.length(), patternBuf2, patternBuf2.length()) <= 0)) {
						if(isBoostFunction){
							//boost옵션이 있다면 점수를 올려주고 리턴한다.
							rankInfo.addScore(boostScore);
							if(rankInfo.isExplain()) {
								rankInfo.explain(fieldIndexId, boostScore, "SECTION_BOOST_FILTER");
							}
						}
						return true;
					}
				} else {
					if (compareString(bytesRef, bytesRef.length(), patternBuf1, patternBuf1.length()) >= 0 &&

					compareString(bytesRef, bytesRef.length(), patternBuf2, patternBuf2.length()) <= 0

					) {
						if(isBoostFunction){
							//boost옵션이 있다면 점수를 올려주고 리턴한다.
							rankInfo.addScore(boostScore);
							if(rankInfo.isExplain()) {
								rankInfo.explain(fieldIndexId, boostScore, "SECTION_BOOST_FILTER");
							}
						}
						return true;
					}
				}
				
			}//for
		}
		return isBoostFunction;
	}

	private static int compareString(BytesRef lval, int lsize, BytesRef rval, int rsize) {
		// 무조건 앞에서부터 비교.. 두 값 중 짧은 길이로 선택
		// 앞부분이 모두 같다면 긴쪽이 더 큰 값

		int size = lsize;

		if (size > rsize) {
			size = rsize;
		}

		for (int inx = 0; inx < size; inx++) {
			if ((lval.get(inx) & 0xff) > (rval.get(inx) & 0xff)) {
				return 1;
			} else if ((lval.get(inx) & 0xff) < (rval.get(inx) & 0xff)) {
				return -1;
			} else {
				continue;
			}

		}// 여기까지 왔다면 나머지는 길이비교

		if (lsize > rsize) {
			return 1;
		} else if (lsize < rsize) {
			return -1;
		}

		return 0;
	}

	/**
	 * 
	 * @param lval
	 * @param lsize
	 * @param rval
	 * @param rsize
	 * @return 0:lval and rval is same / 1:lval is bigger / -1:rval is bigger
	 */
	private static int compareNumeric(BytesRef lval, int lsize, BytesRef rval, int rsize) {
		// check msb;
		int direction = 1;

		int lbyte = lval.get(0) & 0xff;
		int rbyte = rval.get(0) & 0xff;

		if ((lbyte & 0x80) == 0) {
			if ((rbyte & 0x80) == 0) {
				// lbyte positive / rbyte positive
				// do nothing
			} else {
				// lbyte positive / rbyte negative
				return 1;
			}
		} else {
			if ((rbyte & 0x80) == 0) {
				// lbyte negative / rbyte positive
				return -1;
			} else {
				// lbyte negative / rbyte negative
				direction = -1;
			}
		}

		for (int inx = 1; inx < lsize; inx++) {
			if ((lval.get(inx) & 0xff) > (rval.get(inx) & 0xff)) {
				return 1 * direction;
			} else if ((lval.get(inx) & 0xff) < (rval.get(inx) & 0xff)) {
				return -1 * direction;
			} else {
				continue;
			}
		}
		return 0;
	}
}