package redisInAction;

import redis.clients.jedis.Jedis;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class Chapter2 {
    /**
     *
     * @param conn
     * @param token
     * @return
     */
    public String checkToken(Jedis conn, String token){
        return conn.hget("login:",token);
    }

    /**
     *
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(Jedis conn, String token, String user, String item){
        long timestamp = System.currentTimeMillis()/1000;
        conn.hset("login:",token, user);
        conn.zadd("recent:", timestamp, token);
        if(item != null){
            conn.zadd("viewed:"+token, timestamp, item);
            conn.zremrangeByRank("viewed:"+token, 0, -26);
            conn.zincrby("viewed:",-1, item);
        }
    }

    /**
     *
     * @param conn
     * @param session
     * @param item
     * @param count
     */
    public void addToCart(Jedis conn, String session,String item, int count){
        if(count <=0){
            conn.hdel("cart:"+session, item);
        }else{
            conn.hset("cart:"+session, item, String.valueOf(count));
        }
    }

    /**
     *
     * @param conn
     * @param rowId
     * @param delay
     */
    public void scheduleRowCache(Jedis conn, String rowId, int delay){
        conn.zadd("delay:", delay, rowId);
        conn.zadd("schedule:", System.currentTimeMillis()/1000, rowId);
    }

    /**
     *
     * @param conn
     * @param request
     * @return
     */
    public boolean canCache(Jedis conn, String request){
        try{
            URL url = new URL(request);
            HashMap<String, String> params = new HashMap<String, String>();
            if(url.getQuery() != null){
                for (String param: url.getQuery().split("&")){
                    String[] pair = param.split("=",2);
                    params.put(pair[0], pair.length==2?pair[1]:null);
                }
            }
            String itemId = extractItemId(params);
            if(itemId == null || isDynamic(params)){
                return false;
            }
            Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank<1000;
        }catch (MalformedURLException e){
            return false;
        }
    }

    /**
     *
     * @param conn
     * @param request
     * @param callback
     * @return
     */
    public String cacheRequest(Jedis conn, String request, Callback callback){
        if(!canCache(conn, request)){
            return callback != null? callback.call(request):null;
        }

        String pageKey = "cache:"+hashRequest(request);
        String content = conn.get(pageKey);
        if(content == null && callback != null){
            content = callback.call(request);
            conn.setex(pageKey, 300, content);
        }
        return content;

    }

    public boolean isDynamic(Map<String,String> params){
        return params.containsKey("_");
    }

    public String extractItemId(Map<String, String> params){
        return params.get("item");
    }

    public String hashRequest(String request){
        return String.valueOf(request.hashCode());
    }
}
interface Callback{
    public String call(String request);
}

