package redisInAction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter1 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    /**
     *
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(Jedis conn,String user, String title, String link){
        String articleId = String.valueOf(conn.incr("article:"));

        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        long now = System.currentTimeMillis()/1000;
        String article = "article:"+articleId;
        HashMap<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);
        conn.zadd("score:", now + VOTE_SCORE, article);
        conn.zadd("time:", now, article);
        return articleId;
    }

    /**
     *
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article){
        long cutoff = (System.currentTimeMillis()/1000)-ONE_WEEK_IN_SECONDS;
        if(conn.zscore("time:", article) < cutoff){
            return;
        }

        String articleId = article.substring(article.indexOf(":")+1);
        if(conn.sadd("voted:"+articleId, user)==1){
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes",1);
        }
    }

    /**
     *
     * @param conn
     * @param page
     * @param order
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page, String order){
        int start = (page-1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE-1;

        Set<String> ids = conn.zrevrange(order,start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id: ids){
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }
        return articles;
    }
    /**
     *
     * @param conn
     * @param page
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page){
        return getArticles(conn, page, "score:");
    }

    /**
     *
     * @param conn
     * @param articleId
     * @param toAdd
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd){
        String article = "article:"+ articleId;
        for(String group: toAdd){
            conn.sadd("group:"+group, article);
        }
    }

    /**
     *
     * @param conn
     * @param group
     * @param page
     * @param order
     * @return
     */
    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order){
        String key = order+group;
        if(!conn.exists(key)){
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:"+group, order);
            conn.expire(key, 60);
        }
        return  getArticles(conn, page, key);
    }

    /**
     *
     * @param conn
     * @param group
     * @param page
     * @return
     */
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page){
        return getGroupArticles(conn, group, page, "score:");
    }


}
