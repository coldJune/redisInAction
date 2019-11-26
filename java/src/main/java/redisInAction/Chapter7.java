package redisInAction;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class Chapter7 {
    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}");
    private static final Pattern WORDS_RE = Pattern.compile("[a-z']{2,}");
    private static final Set<String> STOP_WORDS = new HashSet<String>();
    ;

    static {
        for (String word :
                ("able about across after all almost also am among " +
                        "an and any are as at be because been but by can " +
                        "cannot could dear did do does either else ever " +
                        "every for from get got had has have he her hers " +
                        "him his how however if in into is it its just " +
                        "least let like likely may me might most must my " +
                        "neither no nor not of off often on only or other " +
                        "our own rather said say says she should since so " +
                        "some than that the their them then there these " +
                        "they this tis to too twas us wants was we were " +
                        "what when where which while who whom why will " +
                        "with would yet you your").split(" ")) {
            STOP_WORDS.add(word);
        }
    }

    /**
     * @param content
     * @return
     */
    public Set<String> tokenize(String content) {
        Set<String> words = new HashSet<String>();
        Matcher matcher = WORDS_RE.matcher(content);
        while (matcher.find()) {
            String word = matcher.group().trim();
            if (word.length() > 2 && !STOP_WORDS.contains(word)) {
                words.add(word);
            }
        }
        return words;
    }

    /**
     * @param docid
     * @param content
     * @return
     */
    public int indexDocument(String docid, String content) {
        Set<String> words = tokenize(content);
        redisTemplate.multi();
        for (String word : words) {
            redisTemplate.opsForSet().add("idx:" + word, docid);
        }
        return redisTemplate.exec().size();
    }

    /**
     * @param method
     * @param ttl
     * @param items
     * @return
     */
    public String setCommon(RedisSetCommands redisConnection, String method, int ttl, String... items) {

        byte[][] keys = new byte[items.length][];
        for (int i = 0; i < items.length; i++) {
            keys[i] = ("idx:" + items[i]).getBytes();
        }
        String id = UUID.randomUUID().toString();
        try {
            //redisTemplate和RedisConnection均无法使用反射获取到具体方法
            redisConnection.getClass().getDeclaredMethod(method, byte[].class, byte[][].class)
                    .invoke(redisConnection, ("idx:" + id).getBytes(), keys);
        } catch (Exception e) {
            e.printStackTrace();
        }
        redisTemplate.expire("idx:" + id, ttl, TimeUnit.SECONDS);
        return id;
    }

    public String interset(RedisSetCommands redisConnection, int ttl, String... items) {
        return setCommon(redisConnection, "sInterStore", ttl, items);
    }

    public String union(RedisSetCommands redisConnection, int ttl, String... items) {
        return setCommon(redisConnection, "sUnionStore", ttl, items);
    }

    public String difference(RedisSetCommands redisConnection, int ttl, String... items) {
        return setCommon(redisConnection, "sDiffStore", ttl, items);
    }

    /**
     *
     * @param queryString
     * @return
     */
    public Query parse(String queryString){
        Query query = new Query();
        Set<String> current = new HashSet<String>();
        Matcher matcher = QUERY_RE.matcher(queryString.toLowerCase());
        while(matcher.find()){
            String word = matcher.group().trim();
            char prefix = word.charAt(0);
            if(prefix == '+' || prefix == '-'){
                word = word.substring(1);
            }

            if(word.length()<2 || STOP_WORDS.contains(word)){
                continue;
            }
            if(prefix == '-'){
                query.unwanted.add(word);
                continue;
            }

            if(!current.isEmpty() && prefix !='+'){
                query.all.add(new ArrayList<String>(current));
                current.clear();
            }
            current.add(word);
        }

        if(!current.isEmpty()){
            query.all.add(new ArrayList<String>(current));
        }
        return query;
    }
}

class Query{
    public final List<List<String>> all = new ArrayList<List<String>>();
    public  final Set<String> unwanted = new HashSet<String>();
}