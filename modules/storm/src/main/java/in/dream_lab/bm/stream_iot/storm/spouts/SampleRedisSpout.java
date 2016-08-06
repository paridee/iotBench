package in.dream_lab.bm.stream_iot.storm.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

/**
 * Created by paride on 04/08/16.
 */
public class SampleRedisSpout extends BaseRichSpout {
    String 	redisUrl	=	"160.80.97.147";
    int		redisPort	=	6379;
    int		redisTimeout=	60000;
    static  long msgId  =   0;
    Jedis jedis;
    SpoutOutputCollector _collector;

    public SampleRedisSpout(String redisUrl, int redisPort, int redisTimeout) {
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
        this.redisTimeout = redisTimeout;
    }

    public SampleRedisSpout(){

    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        jedis 	= new Jedis(redisUrl,redisPort,redisTimeout);
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        try{

            long beg	=	System.currentTimeMillis();
            String sleep	=	null;
            do{
                sleep	=	jedis.get("sleepTime");
                //System.out.println("Reader.java waiting speed factor");
                if(sleep==null){
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }while(sleep==null);
            int sleepTime	=	Integer.parseInt(sleep);
            String json	=	jedis.get("json");
            String lock =   jedis.get("lock");
            if((json!=null)&&(lock==null)){
                jedis.set("lock","locked");
                jedis.del("json");
                JSONArray array	=	new JSONArray(json);
                for(int i=0;i<array.length();i++){
                    JSONObject obj	=	(JSONObject) array.get(i);
                    String row		=	obj.getString("value");
                    msgId++;
                    Values values = new Values();
                    values.add(row);
                    values.add(Long.toString(msgId));
                    this._collector.emit(values,msgId);
                    //System.out.println("Reader.java: row "+row);
                }
                jedis.del("lock");
            }
            beg	=	System.currentTimeMillis()-beg;
            try {
                sleepTime	=	sleepTime-(int)beg;
                sleepTime	=	sleepTime/2;
                if(sleepTime>0){
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }catch(JedisConnectionException e){
            e.printStackTrace();
            jedis 	= new Jedis(redisUrl,redisPort,redisTimeout);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("RowString", "MSGID"));
    }
}
