package redisInAction;

import com.google.gson.Gson;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TestChapter2 {
    private Chapter2 chapter2= new Chapter2();
    private Jedis conn = new Jedis();
    @Test
    public void testLoginCookies() throws InterruptedException{
        System.out.println("-------testLoginCookies--------");
        String token = UUID.randomUUID().toString();
        chapter2.updateToken(conn,token,"username", "itemA");
        System.out.println("We logged-in/updated token:"+token);
        System.out.println("for user:'username'");
        System.out.println();
        String user = chapter2.checkToken(conn,token);
        System.out.println("look-up the token's user:"+user);
        Set<Tuple> viewedToken = conn.zrangeWithScores("viewed:"+token,0, -1);
        System.out.println("viewed:"+token+":"+viewedToken);
        Set<Tuple> viewed = conn.zrangeWithScores("viewed:",0, -1);
        System.out.println("viewed:"+viewed);


        CleanSessionsThread thread = new CleanSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if(thread.isAlive()){
            throw  new RuntimeException("the session clean thread is still alive");
        }
        long s = conn.hlen("login:");
        System.out.println("the current number of sessions:"+s);
    }

    @Test
    public void testShoppingCartCookies() throws  InterruptedException{
        System.out.println("-----------testShoppingCartCookies---------");
        String token = UUID.randomUUID().toString();
        chapter2.updateToken(conn, token, "username", "itemA");
        chapter2.addToCart(conn, token, "itemB", 3);
        Map<String, String> cartMap = conn.hgetAll("cart:"+token);
        for(Map.Entry<String,String> entry: cartMap.entrySet()){
            System.out.println("    "+entry.getKey()+":"+entry.getValue());
        }
        CleanFullSessionThread thread = new CleanFullSessionThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if(thread.isAlive()){
            throw  new RuntimeException("the session clean thread is still alive");
        }
        cartMap = conn.hgetAll("cart:"+token);
        for(Map.Entry<String,String> entry: cartMap.entrySet()){
            System.out.println("    "+entry.getKey()+":"+entry.getValue());
        }
    }

    @Test
    public void testCacheRow() throws InterruptedException{
        System.out.println("-----------testCacheRow---------");
        chapter2.scheduleRowCache(conn, "itemA",5);
        System.out.println("schedule like:");
        Set<Tuple> s = conn.zrangeWithScores("schedule:", 0, -1);
        for(Tuple tuple: s){
            System.out.println("    "+tuple.getElement()+","+tuple.getScore());
        }

        System.out.println("delay like:");
        Set<Tuple> delay = conn.zrangeWithScores("delay:", 0, -1);
        for(Tuple tuple: delay){
            System.out.println("    "+tuple.getElement()+","+tuple.getScore());
        }
        System.out.println("start a caching thread to cache data");
        CacheRowsThread cacheRowsThread = new CacheRowsThread();
        cacheRowsThread.start();

        Thread.sleep(1000);
        System.out.println("cached data likes:");
        String data = conn.get("inv:itemA");
        System.out.println(data);

        System.out.println("check data in 5 seconds");
        Thread.sleep(5000);
        System.out.println("cached data like:");
        data = conn.get("inv:itemA");
        System.out.println(data);

        System.out.println("force un-caching");
        chapter2.scheduleRowCache(conn,"itemA",-1);
        Thread.sleep(1000);
        data = conn.get("inv:itemA");
        System.out.println("cache was cleared?\n"+(data==null));

        cacheRowsThread.quit();
        Thread.sleep(2000);
        if(cacheRowsThread.isAlive()){
            throw  new RuntimeException("the session clean thread is still alive");
        }
    }

    @Test
    public void testCacheRequest(){
        System.out.println("--------testCacheRequest-------");
        String token = UUID.randomUUID().toString();
        Callback callback = new Callback() {
            public String call(String request) {
                return "content for "+ request;
            }
        };
        chapter2.updateToken(conn, token, "username", "itemA");
        String url = "http://test.io/?item=itemA";
        String result = chapter2.cacheRequest(conn, url, callback);
        System.out.println("initial content:"+result);

        String result2 = chapter2.cacheRequest(conn, url, null);
        System.out.println("we ended get the same response!!\n"+result2);
        assert result.equals(result2);
        assert !chapter2.canCache(conn, "http://test.io/?item=itemX&_=123456");
        assert !chapter2.canCache(conn, "http://test.io/");
    }
}


class CleanSessionsThread extends Thread{
    private Jedis conn;
    private int limit;
    private boolean quit;

    public CleanSessionsThread(int limit){
        this.conn = new Jedis();
        this.limit = limit;
    }

    public void quit(){
        quit = true;
    }

    public void run(){
        while(!quit){
            long size = conn.zcard("recent:");
            if(size <= limit){
                try{
                    sleep(1000);
                }catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            long endIndex = Math.min(size-limit, 100);
            Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
            String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

            ArrayList<String> sessionKeys = new ArrayList<String>();
            for (String token: tokens){
                sessionKeys.add("viewed:"+token);
            }

            conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
            conn.hdel("login:", tokens);
            conn.zrem("recent:", tokens);
        }
    }
}

class CleanFullSessionThread extends Thread{
    private Jedis conn;
    private int limit;
    private boolean quit;

    public CleanFullSessionThread(int limit){
        this.conn = new Jedis();
        this.limit = limit;
    }

    public void quit(){
        quit = true;
    }

    public void run(){
        while (!quit){
            long size = conn.zcard("recent:");
            if (size < limit){
                try{
                    sleep(1000);
                }catch (InterruptedException ie){
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            long endIndex = Math.min(size-  limit, 100);
            Set<String> sessionSet = conn.zrange("recent:",0, endIndex-1);
            String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

            ArrayList<String> sessionKeys = new ArrayList<String>();
            for (String sess: sessions){
                sessionKeys.add("viewed:"+sess);
                sessionKeys.add("cart:"+sess);
            }
            if(sessionKeys.size()!=0){
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", sessions);
                conn.zrem("recent:", sessions);
            }

        }
    }
}

class CacheRowsThread extends Thread{
    private Jedis conn;
    private boolean quit;

    public  CacheRowsThread(){
        this.conn = new Jedis();
    }

    public void quit(){
        quit = true;
    }

    public void run(){
        Gson gson = new Gson();
        while(!quit){
            Set<Tuple> range = conn.zrangeWithScores("schedule:",0,0);
            Tuple next = range.size() > 0 ?range.iterator().next():null;
            long now  = System.currentTimeMillis()/1000;
            if(next == null || next.getScore() > now){
                try{
                    sleep(50);
                }catch (InterruptedException ie){
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            String rowId = next.getElement();
            double delay = conn.zscore("delay:", rowId);
            if(delay <=0){
                conn.zrem("delay:", rowId);
                conn.zrem("schedule:", rowId);
                conn.del("inv:"+rowId);
                continue;
            }
            Inventory row = Inventory.get(rowId);
            conn.zadd("schedule:", now +delay, rowId);
            conn.set("inv:"+rowId, gson.toJson(row));
        }
    }
}
class Inventory{
    private String id;
    private String data;
    private long time;

    private Inventory(String id){
        this.id = id;
        this.data = "data to cache";
        this.time = System.currentTimeMillis()/1000;
    }

    public static Inventory get(String id){
        return new Inventory(id);
    }

}
