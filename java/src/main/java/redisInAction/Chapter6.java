package redisInAction;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.javatuples.Tuple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

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

    /**
     *
     * @param lockName
     * @return
     */
    public String acquireLock(String lockName){
        return acquireLock(lockName, 10000);
    }

    /**
     *
     * @param lockName
     * @param timeout
     * @return
     */

    public String acquireLock(String lockName, long timeout){
        String identifier = UUID.randomUUID().toString();
        long end = System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < end){
            if(redisTemplate.opsForValue().setIfAbsent("lock:"+lockName, identifier)){
                return identifier;
            }

            try {
                Thread.sleep(1);
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    /**
     *
     * @param lockName
     * @param acquireTimeout
     * @param lockTimeout
     * @return
     */
    public String acquireLockWithTimeout(String lockName, long acquireTimeout, long lockTimeout){
        String identifier = UUID.randomUUID().toString();
        String lockKey = "lock:"+lockName;
        long end = System.currentTimeMillis() + acquireTimeout;
        while(System.currentTimeMillis() < end){
            if(redisTemplate.opsForValue().setIfAbsent(lockKey, identifier)){
                redisTemplate.expire(lockKey, lockTimeout, TimeUnit.MILLISECONDS);
                return identifier;
            }
            if(redisTemplate.getExpire(lockKey) == -1){
                redisTemplate.expire(lockKey, lockTimeout, TimeUnit.MILLISECONDS);
            }

            try {
                Thread.sleep(1);
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    /**
     *
     * @param lockName
     * @param identifier
     * @return
     */
    public boolean releaseLock(String lockName, String identifier){
        String lockKey = "lock:"+lockName;
        while(true){
            redisTemplate.watch(lockKey);
            if(identifier.equals(redisTemplate.opsForValue().get(lockKey))){
                redisTemplate.multi();
                redisTemplate.delete(lockKey);
                List<Object> results = redisTemplate.exec();
                if(results == null){
                    continue;
                }
                return true;
            }
            redisTemplate.unwatch();
            break;
        }
        return false;
    }

    /**
     *
     * @param semname
     * @param limit
     * @param timeout
     * @return
     */
    public String acquireFairSemaphore(String semname, int limit, long timeout){
        String identifier = UUID.randomUUID().toString();
        String czset = semname+":owner";
        String ctr = semname+":counter";

        long now = System.currentTimeMillis();
        redisTemplate.multi();
        redisTemplate.opsForZSet().removeRangeByScore(semname,Double.MIN_VALUE, now - timeout);
        ArrayList<String> oherKeys = new ArrayList<String>();
        oherKeys.add(semname);
        redisTemplate.opsForZSet().intersectAndStore(czset,oherKeys,czset, RedisZSetCommands.Aggregate.SUM, RedisZSetCommands.Weights.of(1,0));
        redisTemplate.opsForValue().increment(ctr);
        List<Object> results = redisTemplate.exec();
        int counter = ((Long)results.get(results.size() - 1)).intValue();

        redisTemplate.multi();
        redisTemplate.opsForZSet().add(semname, identifier, now);
        redisTemplate.opsForZSet().add(czset, identifier, counter);
        redisTemplate.opsForZSet().rank(czset, identifier);
        results = redisTemplate.exec();
        int result = ((Long)results.get(results.size() - 1)).intValue();
        if(result < limit){
            return identifier;
        }
        redisTemplate.multi();
        redisTemplate.opsForZSet().remove(semname, identifier);
        redisTemplate.opsForZSet().remove(czset, identifier);
        redisTemplate.exec();
        return null;
    }

    /**
     *
     * @param semname
     * @param identifier
     * @return
     */
    public boolean releaseFairSemphore(String semname, String identifier){
        redisTemplate.multi();
        redisTemplate.opsForZSet().remove(semname, identifier);
        redisTemplate.opsForZSet().remove(semname+":owner", identifier);
        List<Object> results = redisTemplate.exec();
        return(Long) results.get(results.size()-1) == 1;
    }

    /**
     *
     * @param semname
     * @param limit
     * @param timeout
     * @return
     */
    public String acquireFairSemphoreWithLock(String semname, int limit, long timeout){
        String identifier = acquireLockWithTimeout(semname,10,10);
        if(identifier!=null){
            try {
              return acquireFairSemaphore(semname, limit, timeout);
            }catch (Exception e){

            }finally {
                releaseLock(semname,identifier);
            }
        }
        return null;
    }

    /**
     *
     * @param sender
     * @param recipients
     * @param message
     * @return
     */
    public String createChat(String sender, Set<String> recipients, String message){
        String chatId =String.valueOf(redisTemplate.opsForValue().increment("ids:chat:"));
        return createChat(sender, recipients, message, chatId);
    }
    /**
     *
     * @param sender
     * @param recipients
     * @param message
     * @param chatId
     * @return
     */
    public String createChat(String sender, Set<String> recipients, String message,String chatId){
        recipients.add(sender);
        redisTemplate.multi();
        for(String recipient: recipients){
            redisTemplate.opsForZSet().add("chat:"+chatId, recipient, 0);
            redisTemplate.opsForZSet().add("seen:"+recipient, chatId, 0);
        }
        redisTemplate.exec();
        return sendMessage(chatId,sender, message);
    }

    /**
     *
     * @param chatId
     * @param sender
     * @param message
     * @return
     */
    public String sendMessage(String chatId, String sender, String message){
        String identifier = acquireLockWithTimeout("chat:"+chatId, 1000, 1000);
        if(identifier == null){
            throw new RuntimeException("could not get the lock");
        }
        try{
            long messageId = redisTemplate.opsForValue().increment("ids:"+chatId);
            HashMap<String, Object> values = new HashMap<String, Object>();
            values.put("id",messageId);
            values.put("ts", System.currentTimeMillis());
            values.put("sender", sender);
            values.put("message", message);
            String packed = new Gson().toJson(values);
            redisTemplate.opsForZSet().add("msgs:"+chatId, packed, messageId);
        }finally {
            releaseLock("chat:"+chatId, identifier);
        }
        return chatId;
    }

    /**
     *
     * @param recipient
     * @return
     */
    public List<ChatMessage> fetchPendingMessage(String recipient){
        Set<ZSetOperations.TypedTuple<String>> seenSet = redisTemplate.opsForZSet().rangeWithScores("seen:"+recipient, 0, -1);
        List<ZSetOperations.TypedTuple<String>> seenList= new ArrayList<ZSetOperations.TypedTuple<String>>(seenSet);

        redisTemplate.multi();
        for(ZSetOperations.TypedTuple tuple: seenList){
            String chatId = (String)tuple.getValue();
            double seenId = tuple.getScore();
            redisTemplate.opsForZSet().rangeByScore("msgs:"+chatId,seenId+1,Double.MAX_VALUE);
        }
        List<Object> resuts = redisTemplate.exec();
        Gson gson = new Gson();
        Iterator<ZSetOperations.TypedTuple<String>> seenIterator = seenList.iterator();
        Iterator<Object> resultsIterator = resuts.iterator();

        List<ChatMessage> chatMessages = new ArrayList<ChatMessage>();
        List<Object[]> seenUpdates = new ArrayList<Object[]>();
        List<Object[]> msgRemoves = new ArrayList<Object[]>();
        while(seenIterator.hasNext()){
            ZSetOperations.TypedTuple<String> seen = seenIterator.next();
            Set<String> messageString = (Set<String>)resultsIterator.next();
            if(messageString.size() == 0){
                continue;
            }

            int seenId = 0;
            String chatId = seen.getValue();
            List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
            for(String messageJson: messageString){
                Map<String, Object> message = (Map<String,Object>) gson.fromJson(messageJson, new TypeToken<Map<String, Object>>(){}.getType());
                int messageId = ((Double)message.get("id")).intValue();
                if(messageId > seenId){
                    seenId = messageId;
                }
                message.put("id", messageId);
                messages.add(message);
            }
            redisTemplate.opsForZSet().add("chat:"+chatId, recipient, seenId);
            seenUpdates.add(new Object[]{"seen:"+recipient, seenId, chatId});

            Set<ZSetOperations.TypedTuple<String>> midIdSet = redisTemplate.opsForZSet().rangeWithScores("chat:"+chatId, 0, 0);
            if(midIdSet.size() >0){
                msgRemoves.add(new Object[]{"msgs:"+chatId, midIdSet.iterator().next().getScore()});
            }
            chatMessages.add(new ChatMessage(chatId, messages));
        }

        redisTemplate.multi();
        for(Object[] seenUpdate: seenUpdates){
            redisTemplate.opsForZSet().add(
                    (String)seenUpdate[0],
                    (String)seenUpdate[2],
                    (Integer)seenUpdate[1]
            );
        }
        for(Object[] msgRemove: msgRemoves){
            redisTemplate.opsForZSet().removeRangeByScore((String)msgRemove[0],0, (Double)msgRemove[1]);
        }
        redisTemplate.exec();
        return chatMessages;
    }
}

class ChatMessage{
    public String chatId;
    public List<Map<String, Object>> message;

    public ChatMessage(String chatId, List<Map<String, Object>> message){
        this.chatId = chatId;
        this.message = message;
    }

    public boolean equals(Object other){
        if(!(other instanceof ChatMessage)){
            return false;
        }

        ChatMessage ohterCm = (ChatMessage)other;
        return chatId.equals(ohterCm.chatId) && message.equals(ohterCm.message);
    }
}