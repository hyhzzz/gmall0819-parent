package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-29 0:09
 * IK分词器进行分词的工具类
 */
public class KeywordUtil {
   public static List<String> analyze(String text){
      List<String> resList = new ArrayList<>();
      StringReader reader = new StringReader(text);
      IKSegmenter ikSegmenter = new IKSegmenter(reader,true);
      try {
         Lexeme lexeme = null;
         while ((lexeme = ikSegmenter.next())!=null){
            String keyword = lexeme.getLexemeText();
            resList.add(keyword);
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
      return resList;
   }

   public static void main(String[] args) {
      List<String> kwList = analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待");
      System.out.println(kwList);
   }
}

