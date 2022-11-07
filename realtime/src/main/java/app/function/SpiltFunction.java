package app.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import utils.KeywordUtil;

import java.io.IOException;
import java.util.List;

// 自定义SQL函数 输出类型 一对多 转换成多行
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SpiltFunction extends TableFunction<Row> {

    public void eval(String str){
        try {
            List<String> keywords = KeywordUtil.spiltKeyword(str);
            for (String word : keywords) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));  // 失败返回原词
        }
    }
}
