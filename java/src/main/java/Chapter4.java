import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import sun.nio.cs.ext.IBM037;

import java.lang.reflect.Method;
import java.util.List;

public class Chapter4 {

    /**
     *
     * @param conn
     * @param itemId
     * @param sellerId
     * @param price
     * @return
     */
    public boolean listItem(Jedis conn, String itemId, String sellerId, double price){
        String inventory = "inventory:"+sellerId;
        String item = itemId + "."+ sellerId;
        long end =System.currentTimeMillis() + 5000;

        while(System.currentTimeMillis() < end){
            conn.watch(inventory);
            if(!conn.sismember(inventory, itemId)){
                conn.unwatch();
                return false;
            }

            Transaction transaction = conn.multi();
            transaction.zadd("market:", price, item);
            transaction.srem(inventory, itemId);
            List<Object> results = transaction.exec();
            if(results == null){
                continue;
            }
            return true;
        }
        return false;
    }

    /**
     *
     * @param conn
     * @param buyerId
     * @param itemId
     * @param sellerId
     * @param lprice
     * @return
     */
    public boolean purchaseItem(Jedis conn, String buyerId, String itemId, String sellerId, double lprice){
        String buyer = "users:"+buyerId;
        String seller = "users:"+sellerId;
        String item = itemId+'.'+sellerId;
        String inventory = "inventory:"+buyerId;
        long end = System.currentTimeMillis() + 1000;
        while(System.currentTimeMillis() < end){
            conn.watch("market:", buyer);
            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));
            if(price != lprice || price > funds){
                conn.unwatch();
                return false;
            }

            Transaction transaction = conn.multi();
            transaction.hincrBy(seller,"funds", (int)price);
            transaction.hincrBy(buyer,"funds", (int)-price);
            transaction.sadd(inventory, itemId);
            transaction.zrem("market:",item);
            List<Object> results = transaction.exec();
            if (results == null){
                continue;
            }
            return true;
        }
        return false;
    }

    /**
     *
     * @param conn
     * @param duration
     */
    public void benchmarkUpdateToken(Jedis conn, int duration){
        try {
            Class[] args = new Class[]{Jedis.class, String.class,String.class,String.class};
            Method[] methods = new Method[]{
                    this.getClass().getDeclaredMethod("updateToken", args),
                    this.getClass().getDeclaredMethod("updateTokenPipeline", args)
            };
            for(Method method: methods){
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + (duration *1000);
                while(System.currentTimeMillis() < end){
                    count++;
                    method.invoke(this, conn, "token","user","item");
                }
                long delta = System.currentTimeMillis() - start;
                System.out.println(
                        method.getName() + ' '+count + ' '+ (delta/1000) + ' '+ (count/(delta/1000))
                );
            }
        }catch (Exception e){
            e.printStackTrace();
            throw  new RuntimeException();
        }
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
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);
        if(item != null){
            conn.zadd("viewed:"+token, timestamp, item);
            conn.zremrangeByRank("viewed:"+token,0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     *
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateTokenPipeline(Jedis conn, String token, String user, String item){
        long timestamp = System.currentTimeMillis()/1000;
        Pipeline pipeline = conn.pipelined();
        pipeline.multi();
        pipeline.hset("login:", token, user);
        pipeline.zadd("recent:", timestamp, token);
        if(item !=null){
            pipeline.zadd("viewed:"+token, timestamp, item);
            pipeline.zremrangeByRank("viewed:"+token, 0, -26);
            pipeline.zincrby("viewed:", -1, item);
        }
        pipeline.exec();
    }


}
