package redisInAction;

import com.sun.xml.internal.ws.api.pipe.Tube;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class Chapter5 {

    private static  final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    public static final Collator COLLATOR = Collator.getInstance();
    /**
     *
     * @param conn
     * @param name
     * @param message
     */
    public void logRecent(RedisConnection conn, String name,String message){
        logRecent(conn, name, message, "info");
    }
    /**
     *
     * @param conn
     * @param name
     * @param message
     * @param severity
     */
    public void logRecent(RedisConnection conn, String name, String message, String severity){
        String destination = "recent:"+name+":"+severity;
        conn.openPipeline();
        conn.lPush(destination.getBytes(), (TIMESTAMP.format(new Date()) +' ' +message).getBytes());
        conn.lTrim(destination.getBytes(),0,99);
        conn.closePipeline();
    }

    /**
     *
     * @param conn
     * @param name
     * @param message
     */
    public void logCommon(RedisConnection conn, String name, String message){
        logCommon(conn, name, message, "info", 5000);
    }
    /**
     *
     * @param conn
     * @param name
     * @param message
     * @param severity
     * @param timeout
     */
    public void logCommon(RedisConnection conn, String name, String message, String severity, int timeout){
        String commonDest = "common:" + name +':'+ severity;
        String startKey = commonDest + ":start";
        long end = System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < end){
            conn.watch(startKey.getBytes());
            String hourStart = TIMESTAMP.format(new Date());
            byte[] existing = conn.get(startKey.getBytes());
            conn.multi();
            if(existing != null && COLLATOR.compare(new String(existing), hourStart) < 0){
                conn.rename(commonDest.getBytes(), (commonDest+":last").getBytes());
                conn.rename(startKey.getBytes(), (commonDest+":pstart").getBytes());
                conn.set(startKey.getBytes(),hourStart.getBytes());
            }

            conn.zIncrBy(commonDest.getBytes(), 1, message.getBytes());

            String recentDest = "recent:" + name + ":" + severity;
            conn.lPush(recentDest.getBytes(), (TIMESTAMP.format(new Date())+' '+ message).getBytes());
            conn.lTrim(recentDest.getBytes(), 0, 99);
            List<Object> results = conn.exec();
            if(results == null){
                continue;
            }
            return;
        }
        conn.multi();
    }

    /**
     *
     * @param conn
     * @param name
     * @param count
     */
    public void updateCounter(RedisConnection conn, String name, int count){
        updateCounter(conn, name, count, System.currentTimeMillis()/1000);
    }
    public static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    /**
     *
     * @param conn
     * @param name
     * @param count
     * @param now
     */
    public void updateCounter(RedisConnection conn, String name, int count, long now){
        conn.multi();
        for(int prec: PRECISION){
            long pnow = (now/prec)*prec;
            String hash = String.valueOf(prec)+':'+name;
            conn.zAdd("known:".getBytes(), 0, hash.getBytes());
            conn.hIncrBy(("count:"+hash).getBytes(), String.valueOf(now).getBytes(), count);
        }
        conn.exec();
    }

    /**
     *
     * @param redisTemplate
     * @param name
     * @param precision
     * @return
     */
    public List<Pair<Integer, Integer>> getCounter(RedisTemplate redisTemplate, String name, int precision){
        String hash = String.valueOf(precision)+':'+name;
         Map<String, String> data = redisTemplate.opsForHash().entries("count:"+hash);
        ArrayList<Pair<Integer, Integer>> results = new ArrayList<Pair<Integer, Integer>>();
        for(Map.Entry<String,String> entry: data.entrySet()){
            results.add(new Pair<Integer, Integer>(
                    Integer.parseInt(entry.getKey()),
                    Integer.parseInt(entry.getValue())
            ));
        }
        Collections.sort(results);
        return results;
    }

    /**
     *
     * @param conn
     * @param context
     * @param type
     * @param value
     * @return
     */
    public List<Object> updateStats(RedisConnection conn, String context, String type,double value){
        int timeout = 5000;
        String destination = "stats:"+context+':'+type;
        String startKey = destination + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end){
            conn.watch(startKey.getBytes());
            String hourStart = TIMESTAMP.format(new Date());
            byte[] existing = conn.get(startKey.getBytes());
            conn.multi();
            if(existing != null && COLLATOR.compare(new String(existing), hourStart)< 0){
                conn.rename(destination.getBytes(), (destination+":last").getBytes());
                conn.rename(startKey.getBytes(), (destination+":pstart").getBytes());
                conn.set(startKey.getBytes(), hourStart.getBytes());
            }

            String tkey1 = UUID.randomUUID().toString();
            String tkey2 = UUID.randomUUID().toString();
            conn.zAdd(tkey1.getBytes(), value, "min".getBytes());
            conn.zAdd(tkey2.getBytes(), value, "max".getBytes());

            conn.zUnionStore(destination.getBytes(),  RedisZSetCommands.Aggregate.MIN,RedisZSetCommands.Weights.of(1,1), destination.getBytes(),tkey1.getBytes());
            conn.zUnionStore(destination.getBytes(),  RedisZSetCommands.Aggregate.MAX,RedisZSetCommands.Weights.of(1,1), destination.getBytes(),tkey2.getBytes());

            conn.del(tkey1.getBytes(), tkey2.getBytes());
            conn.zIncrBy(destination.getBytes(),1, "count".getBytes());
            conn.zIncrBy(destination.getBytes(), value, "sum".getBytes());
            conn.zIncrBy(destination.getBytes(), value*value, "sumsq".getBytes());

            List<Object> results = conn.exec();
            if(results ==null){
                continue;
            }
            return results.subList(results.size() -3, results.size());
        }
        return null;
    }

    /**
     *
     * @param conn
     * @param context
     * @param type
     * @return
     */
    public Map<String, Double> getStats(RedisConnection conn, String context, String type){
        String key = "stats:"+context+':'+type;
        Map<String, Double> stats = new HashMap<String, Double>();
        Set<RedisZSetCommands.Tuple>data = conn.zRangeWithScores(key.getBytes(), 0, -1);
        for(RedisZSetCommands.Tuple tuple: data){
            stats.put(new String(tuple.getValue()), tuple.getScore());
        }
        stats.put("average", stats.get("sum")/stats.get("count"));
        double numerator = stats.get("sumsq") - Math.pow(stats.get("sum"),2)/stats.get("count");
        double count = stats.get("count");
        stats.put("stddev", Math.pow(numerator/(count>1?count-1:1),.5));
        return stats;
    }
    public static void main(String[] args){
        System.out.println(TIMESTAMP.format(new Date()));
        Jedis conn = new Jedis();

    }

}
