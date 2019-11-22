package redisInAction;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class Chapter6 {

    private StringRedisTemplate redisTemplate;
    @Autowired
    Chapter6(StringRedisTemplate redisTemplate){
        redisTemplate.setEnableTransactionSupport(true);
        this.redisTemplate = redisTemplate;
    }

    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";
    /**
     *
     * @param user
     * @param contact
     */
    public void addUpdateContact(String user, final String contact){
        final String acList = "recent:"+user;
        redisTemplate.executePipelined(new RedisCallback<Object>() {
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                connection.multi();
                connection.lRem(acList.getBytes(), 0, contact.getBytes());
                connection.lPush(acList.getBytes(), contact.getBytes());
                connection.lTrim(acList.getBytes(),0,99);
                connection.exec();
                return null;
            }
        }, new StringRedisSerializer());
    }

    /**
     *
     * @param user
     * @param contact
     */
    public void removeContact(String user, String contact){
        redisTemplate.opsForList().remove("recent:"+user, 0, contact);
    }

    /**
     *
     * @param user
     * @param prefix
     * @return
     */
    public List<String> fetchAutoCompleteList(String user, String prefix){
        List<String> candidates = redisTemplate.opsForList().range("recent:"+user, 0, -1);
        List<String> matches = new ArrayList<String>();
        for(String candidate : candidates){
            if(candidate.toLowerCase().startsWith(prefix)){
                matches.add(candidate);
            }
        }
        return  matches;
    }

    /**
     *
     * @param prefix
     * @return
     */
    public String[] findPrefixRange(String prefix){
        int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length()-1));
        char suffix = VALID_CHARACTERS.charAt(posn >0 ? posn-1:0);
        String start = prefix.substring(0, prefix.length()-1)+suffix+'{';
        String end = prefix+'{';
        return new String[]{start, end};
    }

    /**
     *
     * @param guild
     * @param user
     */
    public void joinGuild(String guild, String user){
        redisTemplate.opsForZSet().add("members:"+guild, user, 0);
    }

    /**
     *
     * @param guild
     * @param user
     */
    public void leaveGuild(String guild, String user){
        redisTemplate.opsForZSet().remove("members:"+guild,user);
    }

    /**
     *
     * @param guild
     * @param prefix
     * @return
     */

    public Set<String> autocompleteOnPrefix(String guild, String prefix){
        String [] range = findPrefixRange(prefix);
        String identifier = UUID.randomUUID().toString();
        String start = range[0] + identifier;
        String end = range[1]+identifier;
        String zsetName = "members:"+guild;
        redisTemplate.opsForZSet().add(zsetName,start,0);
        redisTemplate.opsForZSet().add(zsetName, end,0);
        redisTemplate.watch(zsetName);
        long sindex = redisTemplate.opsForZSet().rank(zsetName, start);
        long eindex = redisTemplate.opsForZSet().rank(zsetName, end);
        long erange = Math.min(sindex+9, eindex-2);
        redisTemplate.multi();
        redisTemplate.opsForZSet().remove(zsetName, start);
        redisTemplate.opsForZSet().remove(zsetName, end);
        redisTemplate.opsForZSet().range(zsetName,sindex, erange);
        List<Object> resulst = redisTemplate.exec();
        Set<String> items = new HashSet<String>();
        if(resulst!=null){
            items=(Set<String>)resulst.get(resulst.size() -1);
        }
        for(Iterator<String> iterator=items.iterator();iterator.hasNext();){
            if(iterator.next().indexOf('{')!=-1){
                iterator.remove();
            }
        }
        return items;
    }

}
