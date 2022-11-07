package utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> spiltKeyword(String keyword) throws IOException {
        ArrayList<String> res = new ArrayList<>();

        StringReader reader = new StringReader(keyword);
        // 需要一个Reader 分词器类型是smart
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        while(true){
            Lexeme next = ikSegmenter.next();
            if(next != null){
                // 取出单词
                String word = next.getLexemeText();
                res.add(word);
            }else {
                break;
            }
        }
        return res;
    }
}
