package com.xly.gmall.realtime.app.func;

import com.xly.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
//表值函数，将一个集合中的一行数据转换成多行数据，相当于hive中的炸裂函数
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String str) {
        for (String keyword : KeywordUtil.analyze(str)) {
            collect(Row.of(keyword));
        }
    }
}
