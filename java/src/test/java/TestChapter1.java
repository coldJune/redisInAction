import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

public class TestChapter1  {

    @Test
    public void test(){
        Jedis conn = new Jedis();
        Chapter1 chapter1 = new Chapter1();
        System.out.println("--------测试postArticle-------");
        String articleId = chapter1.postArticle(conn,"user1","title1","link1");
        assert !conn.hgetAll("article:"+articleId).isEmpty();
        chapter1.postArticle(conn,"user2","title2","link2");
        printArticles(chapter1.getArticles(conn, 1));
        System.out.println(conn.zrangeWithScores("score:",0,-1));
        System.out.println(conn.zrangeWithScores("time:",0,-1));
        System.out.println("--------测试articleVote------");
        chapter1.articleVote(conn,"user2","article:1");
        printArticles(chapter1.getArticles(conn, 1));
        System.out.println("--------测试addGroup-------");
        chapter1.addGroups(conn,"1",new String[]{"1","2"});
        System.out.println("group1:"+conn.smembers("group:1"));
        System.out.println("group2:"+conn.smembers("group:2"));
        printArticles(chapter1.getGroupArticles(conn, "1", 1));
        conn.flushAll();
    }

    private void printArticles(List<Map<String,String>> articles){
        for(Map<String, String> article: articles){
            System.out.println(" id:"+article.get("id"));
            for(Map.Entry<String, String> entry: article.entrySet()){
                if(entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    "+entry.getKey()+":"+entry.getValue());
            }
        }
    }
}
