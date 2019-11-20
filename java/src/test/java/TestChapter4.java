import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

public class TestChapter4 {
    private Chapter4 chapter4 = new Chapter4();
    private Jedis conn = new Jedis();
    @Test
    public void testListItem(){
        System.out.println("**********testListItem*********");
        String seller = "userA";
        String item = "itemA";
        conn.sadd("inventory:"+seller, item);
        Set<String>  inventorys = conn.smembers("inventory:"+seller);
        System.out.println(seller+"'s inventory:");
        for(String inventory:inventorys){
            System.out.println(inventory);
        }
        System.out.println("\n----listItem----");
        chapter4.listItem(conn,item, seller,10);
        inventorys = conn.smembers("inventory:"+seller);
        System.out.println(seller+"'s inventory:");
        for(String inventory:inventorys){
            System.out.println(inventory);
        }
        Set<Tuple> market = conn.zrangeWithScores("market:", 0, -1);
        System.out.println("market's inventory:");
        for(Tuple inventory:market){
            System.out.println(inventory);
        }
    }

    @Test
    public void testPurchaseItem(){
        System.out.println("\n************testPurchaseItem************");
        testListItem();
        conn.hset("users:userB", "funds", "100");
        Map<String, String> userB = conn.hgetAll("users:userB");
        System.out.println("userB's info:");
        for(Map.Entry entry: userB.entrySet()){
            System.out.println(entry.getKey() + ":"+entry.getValue());
        }

        boolean isSuccess = chapter4.purchaseItem(conn, "userB","itemA","userA", 10);
        System.out.println("\nis userB buy itemA succeed?\n"+isSuccess);
        Set<Tuple> market = conn.zrangeWithScores("market:", 0, -1);
        System.out.println("\nmarket's inventory:");
        for(Tuple inventory:market){
            System.out.println(inventory);
        }
        Set<String>  inventorys = conn.smembers("inventory:userA");
        System.out.println("\nuserA's inventory:");
        for(String inventory:inventorys){
            System.out.println(inventory);
        }
        inventorys = conn.smembers("inventory:userB");
        System.out.println("\nuserB's inventory:");
        for(String inventory:inventorys){
            System.out.println(inventory);
        }
        userB = conn.hgetAll("users:userB");
        System.out.println("\nuserB's info:");
        for(Map.Entry entry: userB.entrySet()){
            System.out.println(entry.getKey() + ":"+entry.getValue());
        }
        Map<String,String> userA = conn.hgetAll("users:userA");
        System.out.println("\nuserA's info:");
        for(Map.Entry entry: userA.entrySet()){
            System.out.println(entry.getKey() + ":"+entry.getValue());
        }
        conn.flushAll();
    }

    @Test
    public void testBenchmarkUpdateToken(){
        chapter4.benchmarkUpdateToken(conn, 5);
    }
}
