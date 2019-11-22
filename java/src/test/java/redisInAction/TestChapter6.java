package redisInAction;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

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
    public void clean(){
        redisTemplate.delete("members:test");
    }
}
