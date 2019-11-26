package redisInAction;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestChapter7 {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private Chapter7 chapter7;
    private static String CONTENT =
            "this is some random content, look at how it is indexed.";
    @Test
    public void testIndexDocument(){
        System.out.println("---------testIndexDocument-------------");
        Set<String> tokens = chapter7.tokenize(CONTENT);
        System.out.println("tokens are:\n"+ Arrays.toString(tokens.toArray()));
        assert tokens.size()>0;
        System.out.println("index document");
        int count = chapter7.indexDocument("test", CONTENT);
        assert count == tokens.size();
        Set<String> test = new HashSet<String>();
        test.add("test");
        for(String t: tokens){
            Set<String> members = redisTemplate.opsForSet().members("idx:"+t);
            assert test.equals(members);
        }
    }

    @Test
    public void testSetOperations(){
        System.out.println("----testOperations------");
        chapter7.indexDocument("test",CONTENT);
        final Set<String> test = new HashSet<String>();
        test.add("test");
        redisTemplate.execute(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                String id = chapter7.interset(connection,10, "content", "indexed");
                assert test.equals(redisTemplate.opsForSet().members("idx:"+id));

                id = chapter7.interset(connection, 10, "content", "ignored");
                assert redisTemplate.opsForSet().members("idx:"+id).isEmpty();

                id = chapter7.union(connection, 10, "content","ignored");
                assert test.equals(redisTemplate.opsForSet().members("idx:"+id));

                id = chapter7.difference(connection, 10, "content","ignored");
                assert test.equals(redisTemplate.opsForSet().members("idx:"+id));

                id = chapter7.difference(connection, 10, "content","indexed");
                assert redisTemplate.opsForSet().members("idx:"+id).isEmpty();
                return null;
            }
        });

    }

    @Test
    public void testParseQuery(){
        System.out.println("--------testParseQuery-------------");
        String queryString = "test query without stopwords";
        Query query = chapter7.parse(queryString);
        String[] words = queryString.split(" ");
        for(int i =0; i<words.length; i++){
            List<String> word = new ArrayList<String>();
            word.add(words[i]);
            System.out.println(word.toString());
            assert word.equals(query.all.get(i));
        }

        assert query.unwanted.isEmpty();

        queryString = "test +query without -stopwords";
        query = chapter7.parse(queryString);
        System.out.println(query.all.toString());
        System.out.println(query.unwanted.toString());
        assert "test".equals(query.all.get(0).get(0));
        assert "query".equals(query.all.get(0).get(1));
        assert "without".equals(query.all.get(1).get(0));
        assert "stopwords".equals(query.unwanted.toArray()[0]);
    }

    @Test
    public void clean(){

        redisTemplate.execute(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                connection.flushAll();
                return null;
            }
        });
    }
}
