package cn.doitedu.dynamic_rule.demos;

import com.alibaba.fastjson.JSON;
import redis.clients.jedis.Jedis;

public class RedisDemo {

    public static void main(String[] args) {

        Jedis jedis = new Jedis("hdp02", 6379);

        String res = jedis.ping();
        System.out.println(res);

        // 插入一条扁平数据
        jedis.set("key01","value01");

        String res2 = jedis.get("key01");
        System.out.println(res2);


        // 插入一个json
        jedis.set("key02","{'a':1,'b':2,c:[4,5,6]}");
        String res3 = jedis.get("key02");
        System.out.println(res3);


        // 插入一个person对象
        Person p = new Person("宴梅", 30);
        String pjson = JSON.toJSONString(p);
        jedis.set("yanmei",pjson);
        String res4 = jedis.get("yanmei");
        System.out.println(res4);

    }
}

class Person{
    String name;
    int age;

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
