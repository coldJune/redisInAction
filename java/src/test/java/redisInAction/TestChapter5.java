package redisInAction;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestChapter5 {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private Chapter5 chapter5;
    @Test
    public void testZunionStore(){
        redisTemplate.executePipelined(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();
                connection.zAdd("key1".getBytes(), 1, "min".getBytes());
                connection.zAdd("key2".getBytes(), 2, "min".getBytes());
                connection.zUnionStore("key3".getBytes(), RedisZSetCommands.Aggregate.MIN, new int[]{1,1},"key1".getBytes(),"key2".getBytes());
                connection.closePipeline();
                Set<byte[]> key3= connection.zRange("key3".getBytes(),0,-1);
                for(byte[] key: key3){
                    System.out.println(new String(key));
                }
                System.out.println();
                return null;
            }
        },new StringRedisSerializer());
        //可能由于序列化问题导致客户端无法取值
        System.out.println(redisTemplate.opsForZSet().range("key3",0,-1));
    }

    @Test
    public void testLogRecent(){
        System.out.println("--------testLogRecent-------");
        System.out.println("write a few logs to the recent log");
        redisTemplate.executePipelined(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                for (int i=0; i< 10; i++){
                    chapter5.logRecent(connection, "test", "this is log " + i);
                }
                return null;
            }
        }, new StringRedisSerializer());
        List<String> recent = redisTemplate.opsForList().range("recent:test:info", 0, -1);
        System.out.println("message's number:"+recent.size());
        for(String log: recent){
            System.out.println(log);
        }
    }

    @Test
    public void testLogCommon(){
        System.out.println("----------testLogCommon----------");
        System.out.println("write a few common log");
        redisTemplate.executePipelined(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
//                for(int count=1; count<6; count++){
//                    for(int i = 0; i<count; i++){
                        chapter5.logCommon(connection, "test","log-");
//                    }
//                }
                return null;
            }
        }, new StringRedisSerializer());
        Set<ZSetOperations.TypedTuple<String>> common = redisTemplate.opsForZSet().rangeWithScores("common:test:info", 0, -1);
        System.out.println("log's num:"+common.size());
        for(ZSetOperations.TypedTuple<String> log:common){
            System.out.println("    "+ log.getValue()+":"+log.getScore());
        }
    }

}
