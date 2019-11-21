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
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestChapter5 {

    @Autowired
    private RedisTemplate redisTemplate;
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
        System.out.println(redisTemplate.boundSetOps("key2").size());
    }
}
