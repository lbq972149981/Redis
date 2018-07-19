import java.util.*;

import redis.clients.jedis.*;

public class RedisClient {
   private Jedis jedis;//非切片客户端连接
   private JedisPool jedisPool;//非切片连接池
   private ShardedJedis shardedJedis;//切片客户端连接
   private ShardedJedisPool shardedJedisPool;//切片连接池
   public static void main(String[] args) {
      RedisClient redisClient = new RedisClient();
      ShardedJedis shardedJedis = redisClient.getShardedJedis();
      Jedis jedis = redisClient.getJedis();
      JedisPool jedisPool = redisClient.getJedisPool();
      ShardedJedisPool shardedJedisPool = redisClient.getShardedJedisPool();
      jedis.flushDB();
      Scanner sc = new Scanner(System.in);
      String type = null;
      //循环输入各种数据类型
      while(!(type = sc.next()).equals("quit")){
         String value = null;
         String key = null;
         //key操作
         if(type.equals("key")){
            sc = new Scanner(System.in);
            String s = null;
            while(!(s=sc.nextLine()).equals("quit")) {
               String[] arr = s.split(" ");
               if (arr.length > 2) {
                  key = arr[1];
                  value = arr[2];
               }
               //添加K-V
               if ("set".equals(arr[0])) {
                  System.out.println(shardedJedis.set(key, value));
               } else if ("keys".equals(arr[0])) {//输出所有key
                  Set<String> keys = jedis.keys("*");
                  for (String k : keys) {
                     System.out.println(k);
                  }
               } else if ("exists".equals(arr[0])) {//判断k是否存在
                  System.out.println(shardedJedis.exists(key));
               } else if ("del".equals(arr[0])) {//删除k及其v
                  System.out.println(jedis.del(arr[1]));
               } else if ("expire".equals(arr[0])) {//设置过期时间
                  int time = Integer.parseInt(value);
                  System.out.println(jedis.expire(key, time));
               }else if("get".equals(arr[0])){//获取K对应的v
                  System.out.println(jedis.get(arr[1]));
               } else {
                  System.out.println("待续...");
               }
            }
         }
         //list操作
         else if(type.equals("list")) {
            sc = new Scanner(System.in);
            String s = null;
            while (!(s = sc.nextLine()).equals("quit")) {
               String[] arr = s.split(" ");
               if (arr.length > 2) {
                  key = arr[1];
                  value = arr[2];
               }
               if ("lpush".equals(arr[0])) {//左添加元素
                  System.out.println(shardedJedis.lpush(key, value));
               } else if ("lrange".equals(arr[0])) {//返回list中的全部元素
                  System.out.println(shardedJedis.lrange(key,0,-1));
               } else if ("lrem".equals(arr[0])) {//删除指定个数的指定元素
                  long count = Long.parseLong(arr[2]);
                  value = arr[3];
                  System.out.println(shardedJedis.lrem(key,count,value));
               } else if ("lpop".equals(arr[0])) {//弹出元素
                  System.out.println(jedis.lpop(key));
               } else if ("lindex".equals(arr[0])) {//获取index处的元素
                  long index = Long.parseLong(value);
                  System.out.println(shardedJedis.lindex(key,index));
               } else if ("lset".equals(arr[0])) {//覆盖index处的元素为新的v
                  long index = Long.parseLong(value);
                  value = arr[3];
                  System.out.println(shardedJedis.lset(key,index,value));
               } else if("sort".equals(arr[0]))//排序
               {
                  SortingParams sortingParameters = new SortingParams();
                  sortingParameters.alpha();
                  sortingParameters.limit(0, 10);
                  System.out.println(shardedJedis.sort(arr[1],sortingParameters));
               }else {
                  System.out.println("待续...");
               }
            }
         }
         //string操作
         else if(type.equals("string")) {
            sc = new Scanner(System.in);
            String s = null;
            while (!(s = sc.nextLine()).equals("quit")) {
               String[] arr = s.split(" ");
               if (arr.length > 2) {
                  key = arr[1];
                  value = arr[2];
               }
               if ("set".equals(arr[0])) {//新增元素k-v
                  System.out.println(jedis.set(key, value));
               } else if ("get".equals(arr[0])) {//获取元素
                  System.out.println(jedis.get(key));
               } else if ("append".equals(arr[0])) {//将k对应的v后追加value
                  System.out.println(jedis.append(key,value));
               } else {
                  System.out.println("待续...");
               }
            }
         }
         //set操作
         else if(type.equals("set")) {
            sc = new Scanner(System.in);
            String s = null;
            while (!(s = sc.nextLine()).equals("quit")) {
               String[] arr = s.split(" ");
               if (arr.length > 2) {
                  key = arr[1];
                  value = arr[2];
               }
               if ("sadd".equals(arr[0])) {//增加元素
                  String set = arr[1];
                  System.out.println(jedis.sadd(set, value));
               } else if ("smembers".equals(arr[0])) {//列出set中的所有元素
                  String set = arr[1];
                  System.out.println(jedis.smembers(set));
               } else if ("sismember".equals(arr[0])) {//sets里是否有value
                  String sets = arr[1];
                  value = arr[2];
                  System.out.println(jedis.sismember(sets,value));
               } else if("sinter".equals(arr[0])) {//交集
                  String set1 = arr[1];
                  String set2 = arr[2];
                  System.out.println(jedis.sinter(set1,set2));
               } else if("sunion".equals(arr[0])) {//并集
                  String set1 = arr[1];
                  String set2 = arr[2];
                  System.out.println(jedis.sunion(set1,set2));
               }else if("sdiff".equals(arr[0])) {//差集
                  String set1 = arr[1];
                  String set2 = arr[2];
                  System.out.println(jedis.sdiff(set1,set2));
               }else {
                  System.out.println("待续...");
               }
            }
         }
         //hash操作
         else if(type.equals("hash")) {
            sc = new Scanner(System.in);
            String s = null;
            while (!(s = sc.nextLine()).equals("quit")) {
               String[] arr = s.split(" ");
               if (arr.length > 2) {
                  key = arr[1];
                  value = arr[2];
               }
               if ("hset".equals(arr[0])) {//增加元素
                  String sets = key;
                  key = arr[2];
                  value = arr[3];
                  System.out.println(shardedJedis.hset(sets,key,value));
               } else if ("hvals".equals(arr[0])) {//获取sets中所有的values
                  String sets = arr[1];
                  System.out.println(shardedJedis.hvals(sets));
               } else if ("hget".equals(arr[0])) {//获取某个元素
                  String hset = arr[1];
                  key = arr[2];
                  System.out.println(shardedJedis.hget(hset,key));
               } else {

                  System.out.println("待续...");
               }
            }
         }
         //zset操作
         else if(type.equals("zset")) {
            sc = new Scanner(System.in);
            String s = null;
            while (!(s = sc.nextLine()).equals("quit")) {
               String[] arr = s.split(" ");
               if (arr.length > 2) {
                  key = arr[1];
                  value = arr[2];
               }
               if ("zadd".equals(arr[0])) {//添加元素
                  String sets = arr[1];
                  int weight = Integer.parseInt(arr[2]);
                  value = arr[3];
                  System.out.println(shardedJedis.zadd(sets,weight,value));
               } else if ("zrange".equals(arr[0])) {//获取zset集合中所有元素
                  String zset = key;
                  System.out.println(shardedJedis.zrangeWithScores(zset,0,-1));
               } else if ("zcard".equals(arr[0])) {//获取zset集合大小
                  String zset = key;
                  System.out.println(shardedJedis.zcard(zset));
               } else if ("zscore".equals(arr[0])) {//获取指定value的权重
                  String zset = key;
                  value = arr[2];
                  System.out.println(shardedJedis.zscore(zset,value));
               } else if ("zcount".equals(arr[0])) {//获取指定权重范围内元素的个数
                  String zset = key;
                  int weight1 = Integer.parseInt(arr[2]);
                  int weight2 = Integer.parseInt(arr[3]);
                  System.out.println(shardedJedis.zcount(zset,weight1,weight2));
               }else if ("zunionstore".equals(arr[0])) {//获取某个元素
                  String hset = arr[1];
                  String hset1 = arr[2];
                  String hset2 = arr[3];
                  ZParams zParams = new ZParams();
                  zParams.aggregate(ZParams.Aggregate.MAX);
                  System.out.println(jedis.zunionstore(hset,zParams,hset1,hset2));
               }else if ("zinterstore".equals(arr[0])) {//获取某个元素
                  String hset = arr[1];
                  String hset1 = arr[2];
                  String hset2 = arr[3];
                  ZParams zParams = new ZParams();
                  zParams.aggregate(ZParams.Aggregate.MIN);
                  System.out.println(jedis.zinterstore(hset,zParams,hset1,hset2));
               } else {
                  System.out.println("待续...");
               }
            }
         }else {
            System.out.println("待续...");
         }

      }
      //rdb持久化至本地磁盘
      //jedis.save();
      jedisPool.returnResource(jedis);
      shardedJedisPool.returnResource(shardedJedis);
   }
   public RedisClient()
   {
      initialPool();
      initialShardedPool();
      shardedJedis = shardedJedisPool.getResource();
      jedis = jedisPool.getResource();
   }
   /**
    * 初始化非切片池
    */
   private void initialPool()
   {
      // 池基本配置
      JedisPoolConfig config = new JedisPoolConfig();
      config.setMaxTotal(20);
      config.setMaxIdle(5);
      config.setMaxWaitMillis(1000l);
      config.setTestOnBorrow(false);
      jedisPool = new JedisPool(config,"127.0.0.1",6379);
   }
   /**
    * 初始化切片池
    */
   private void initialShardedPool()
   {
      // 池基本配置
      JedisPoolConfig config = new JedisPoolConfig();
      config.setMaxTotal(20);
      config.setMaxIdle(5);
      config.setMaxWaitMillis(1000l);
      config.setTestOnBorrow(false);
      // slave链接
      List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
      shards.add(new JedisShardInfo("127.0.0.1", 6379, "master"));
      // 构造池
      shardedJedisPool = new ShardedJedisPool(config, shards);
   }
   public Jedis getJedis() {
      return jedis;
   }
   public void setJedis(Jedis jedis) {
      this.jedis = jedis;
   }
   public ShardedJedis getShardedJedis() {
      return shardedJedis;
   }
   public void setShardedJedis(ShardedJedis shardedJedis) {
      this.shardedJedis = shardedJedis;
   }

   public JedisPool getJedisPool() {
      return jedisPool;
   }

   public void setJedisPool(JedisPool jedisPool) {
      this.jedisPool = jedisPool;
   }

   public ShardedJedisPool getShardedJedisPool() {
      return shardedJedisPool;
   }

   public void setShardedJedisPool(ShardedJedisPool shardedJedisPool) {
      this.shardedJedisPool = shardedJedisPool;
   }
}
