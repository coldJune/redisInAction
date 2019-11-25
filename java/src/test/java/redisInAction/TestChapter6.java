package redisInAction;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.awt.datatransfer.StringSelection;
import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestChapter6 {

    @Autowired
    private Chapter6 chapter6;

    @Autowired
    private StringRedisTemplate redisTemplate;
    @Test
    public void testAddUpdateContact(){
        System.out.println("----------testAddUpdateContact------------");
        redisTemplate.delete("recent:user");
        System.out.println("let's add some contacts");
        for(int i=1; i<10;i++){
            chapter6.addUpdateContact("user","contact-"+((int)Math.floor(i/3))+'-'+i);
        }
        System.out.println("now contacted contacts");
        List<String> contacts = redisTemplate.opsForList().range("recent:user",0,-1);
        for (String contact: contacts){
            System.out.println("    "+contact);
        }
        System.out.println();

        System.out.println("let's remove some contacts");
        for(int i=1; i<5;i++){
            chapter6.removeContact("user","contact-"+((int)Math.floor(i/3))+'-'+i);
        }
        System.out.println("now contacted contacts");
        contacts = redisTemplate.opsForList().range("recent:user",0,-1);
        for (String contact: contacts){
            System.out.println("    "+contact);
        }
        System.out.println();

        System.out.println("let's test Auto Compelte");
        List<String> all = redisTemplate.opsForList().range("recent:user", 0, -1);
        contacts = chapter6.fetchAutoCompleteList("user","c");
        assert all.equals(contacts);
        List<String> conditions = new ArrayList<String>();
        for(String contact:all){
            if(contact.startsWith("contact-2")){
                conditions.add(contact);
            }
        }
        contacts = chapter6.fetchAutoCompleteList("user","contact-2");
        Collections.sort(conditions);
        Collections.sort(contacts);
        assert conditions.equals(contacts);
    }

    @Test
    public void testAddressBookAutocomplete(){
        System.out.println("--------------testAddressBookAutocomplete-----------");
        System.out.println("the start/end range abc  is:"+ Arrays.toString(chapter6.findPrefixRange("abc")));
        System.out.println();

        System.out.println("let'add a few people to guild");
        for(String name: new String[]{"cold","june","pho","july"}){
            chapter6.joinGuild("test", name);
        }
        Set<String> guild = redisTemplate.opsForZSet().range("members:test",0,-1);
        for(String member: guild){
            System.out.print("  "+member);
        }
        System.out.println();

        System.out.println("now fetch autocomplete.....");
        Set<String> results = chapter6.autocompleteOnPrefix("test","ju");
        assert results.size()==2;

        System.out.println("let somebody leave guild....");
        chapter6.leaveGuild("test","july");
        guild = redisTemplate.opsForZSet().range("members:test",0,-1);
        for(String member: guild){
            System.out.print("  "+member);
        }
        System.out.println();

        System.out.println("now autocomplete");
        results = chapter6.autocompleteOnPrefix("test","ju");
        assert results.size() == 1;

    }

    @Test
    public void testLock() throws InterruptedException{
        System.out.println("-----testLock----------");
        redisTemplate.delete("lock:testlock");
        System.out.println("try to acquire lock");
        assert chapter6.acquireLockWithTimeout("testlock",1000, 1000) != null;
        System.out.println("try to acquire the same lock without release");
        assert chapter6.acquireLockWithTimeout("testlock",10, 1000) == null;
        System.out.println("acquire fail");
        System.out.println();
        Thread.sleep(1000);
        System.out.println("try to acquire lock again");
        String lockId = "";
        assert (lockId=chapter6.acquireLockWithTimeout("testlock",1000, 1000)) != null;
        assert chapter6.releaseLock("testlock",lockId);
        System.out.println("release lock");
        System.out.println("try to acquire lock again");
        assert chapter6.acquireLockWithTimeout("testlock",1000, 1000) != null;
        System.out.println("get it");

    }

    @Test
    public void testCountingSemaphore()throws InterruptedException{
        System.out.println("------testCountingSemaphore-----");
        System.out.println("get 3 semphores");
        for(int i =0; i<3; i++){
            assert  chapter6.acquireFairSemphoreWithLock("testsem", 3, 1000) !=null;
        }
        System.out.println("done");
        System.out.println("get more");
        assert chapter6.acquireFairSemphoreWithLock("testsem",3,1000) == null;
        System.out.println("can't\n");

        System.out.println("wait for timeout");
        Thread.sleep(2000);
        System.out.println("try again");
        String id = chapter6.acquireFairSemphoreWithLock("testsem",3, 1000);
        assert id != null;
        System.out.println("we get one");
        System.out.println("release it");
        assert chapter6.releaseFairSemphore("testsem", id);
        System.out.println("get 3 again");
        for(int i =3; i<3; i++){
            assert  chapter6.acquireFairSemphoreWithLock("testsem", 3, 1000) !=null;
        }
        System.out.println("we get again");
    }


    @Test
    public void testFetchingMessage(){
        System.out.println("-----------testFetchingMessage-----------");
        System.out.println("create a new chat session ");
        Set<String> recipients = new HashSet<String>();
        recipients.add("cold");
        recipients.add("june");
        recipients.add("pho");
        String chatId = chapter6.createChat("july", recipients, "message1");
        System.out.println("send messages");
        for(int i = 2; i<5; i++){
            chapter6.sendMessage(chatId, "july", "message "+ i);
        }
        System.out.println();
        List<ChatMessage> result1 = chapter6.fetchPendingMessage("cold");
        List<ChatMessage> result2 = chapter6.fetchPendingMessage("june");
        assert result1.equals(result2);
        System.out.println("there are chat messages");
        for(ChatMessage chat:result1){
            System.out.println("chatId:"+chat.chatId);
            System.out.println("messages:");
            for(Map<String, Object> message:chat.message){
                System.out.println("    "+message);
            }
        }
        result1 = chapter6.fetchPendingMessage("cold");
        assert result1.size() == 0;
        System.out.println("cold has seen all message");
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
