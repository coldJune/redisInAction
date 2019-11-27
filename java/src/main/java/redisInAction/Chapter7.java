package redisInAction;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DefaultSortParameters;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;
import org.springframework.data.redis.core.BulkMapper;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.query.SortQuery;
import org.springframework.data.redis.core.query.SortQueryBuilder;
import org.springframework.stereotype.Component;

import java.lang.reflect.Array;
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

    public String intersect(RedisSetCommands redisConnection, int ttl, String... items) {
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

    /**
     *
     * @param queryString
     * @param ttl
     * @return
     */
    public String parseAndSearch(String queryString, final int ttl){
        final Query query = parse(queryString);
        if(query.all.isEmpty()){
            return null;
        }

        final List<String> toIntersect = new ArrayList<String>();
        redisTemplate.execute(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                for (List<String> syn : query.all) {
                    if (syn.size() > 1) {
                        toIntersect.add(union(connection,ttl, syn.toArray(new String[syn.size()])));
                    }else {
                        toIntersect.add(syn.get(0));
                    }

                }
                return null;
            }
        });

        final ArrayList<String> intersectResults = new ArrayList<String>();
        if(toIntersect.size() > 1){
            redisTemplate.execute(new RedisCallback<Object>() {
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    intersectResults.add(intersect(connection, ttl, toIntersect.toArray(new String[toIntersect.size()])));
                    return null;
                }
            });
        }else{
            intersectResults.add(toIntersect.get(0));
        }
        String intersectResult = intersectResults.get(0);
        intersectResults.clear();

        if(!query.unwanted.isEmpty()){
            final ArrayList<String> keys = new ArrayList<String>(query.unwanted);
            keys.add(keys.size(),intersectResult);
            redisTemplate.execute(new RedisCallback<Object>() {
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    intersectResults.add(difference(connection, ttl, keys.toArray(new String[keys.size()])));
                    return null;
                }
            });
            intersectResult = intersectResults.get(0);
        }

        return intersectResult;
    }

    /**
     *
     * @param queryString
     * @param sort
     * @return
     */
    public SearchResult searchAndSort(String queryString, String sort){
        boolean desc = sort.startsWith("-");
        if(desc){
            sort = sort.substring(1);
        }
        boolean alpha = !"updated".equals(sort) && !"id".equals(sort);
        String by = "kb:doc:*->"+sort;

        String id = parseAndSearch(queryString, 30);
        redisTemplate.multi();
        redisTemplate.opsForSet().size("idx:"+id);
        SortQueryBuilder<String> builder =  SortQueryBuilder.sort("idx:"+id);
        if(desc){
            builder.order(SortParameters.Order.DESC);
        }
        builder.alphabetical(alpha);
        builder.by(by);

        redisTemplate.sort(builder.build());
        List<Object> results =redisTemplate.exec();
        return new SearchResult(id, (Long)results.get(0),(List<String>)results.get(1));
    }
}

class Query{
    public final List<List<String>> all = new ArrayList<List<String>>();
    public  final Set<String> unwanted = new HashSet<String>();
}

class SearchResult {
    public final String id;
    public final long total;
    public final List<String> results;

    public SearchResult(String id, long total, List<String> results) {
        this.id = id;
        this.total = total;
        this.results = results;
    }
}