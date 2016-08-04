package in.dream_lab.bm.stream_iot.storm;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by paride on 05/08/16.
 */
public class PrometheusConsumer implements IMetricsConsumer {
    //private static final Logger LOG 	= 	LoggerFactory.getLogger(PrometheusConsumer.class);
    private static final String PROMURL	=	"10.0.0.1:9091";


    //private static final CollectorRegistry registry = new CollectorRegistry();
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleDataPoints(TaskInfo arg0, Collection<DataPoint> arg1) {
        // TODO Auto-generated method stub
        CollectorRegistry registry = new CollectorRegistry();
        //LOG.info("SONDA-PRE "+arg0.srcComponentId+" "+arg0.srcTaskId+" "+arg0.srcWorkerHost+" "+arg0.srcWorkerPort+" "+arg0.timestamp+" "+arg0.updateIntervalSecs);
        //LOG.info("SONDA-PRE2 "+arg1.size());
        Iterator<DataPoint> it	=	arg1.iterator();
        while(it.hasNext()){
            DataPoint dp	=	it.next();
            //LOG.info("SONDA-INSIDE "+dp.name+" "+dp.value.toString()+" "+dp.value.getClass());
            if(dp.value instanceof HashMap){
                Iterator innerIt	=	((HashMap)dp.value).keySet().iterator();
                while(innerIt.hasNext()){
                    Object innerKey	=	innerIt.next();
                    Object innerValue	=	((HashMap)dp.value).get(innerKey);
                    //LOG.info("SONDA-INSIDE-INSIDE "+innerKey.toString()+" "+innerValue.toString()+" "+innerValue.getClass());
                    String metricName	=	"_storm_"+dp.name+"_"+innerKey.toString();
                    metricName	=	metricName.replace('-', '_');
                    metricName	=	metricName.replace('/', '_');
                    //if(metricName.length()>=30){
                    // metricName	=   metricName.substring(0, 30);
                    //}
                    double gaugeValue	=	-1;
                    if(innerValue instanceof Long){
                        gaugeValue	=	((Long)innerValue).doubleValue();
                    }
                    else if(innerValue instanceof Integer){
                        gaugeValue	=	((Integer)innerValue).doubleValue();
                    }
                    else if(innerValue instanceof Double){
                        gaugeValue	=	((Double)innerValue);
                    }
                    Gauge duration = Gauge.build()
                            .name(metricName)
                            .help(metricName)
                            .register(registry);
                    duration.set(gaugeValue);
                    //LOG.info("SONDA-INSIDE-INSIDE gauge name "+"storm_"+dp.name+"_"+innerKey.toString());
                }
            }
            String metricName	=	dp.name;
            metricName	=	metricName.replace('-', '_');
            metricName	=	metricName.replace('/', '_');
            if(dp.value instanceof Double){
                Gauge duration = Gauge.build()
                        .name(metricName)
                        .help(metricName)
                        .register(registry);
                duration.set((double)dp.value);
                //LOG.info("SONDA-INSIDE-ELSE gauge name "+"storm_"+dp.name);
            }
            else if(dp.value instanceof Long){
                Gauge duration = Gauge.build()
                        .name(metricName)
                        .help(metricName)
                        .register(registry);
                duration.set(((Long)dp.value).doubleValue());
                //LOG.info("SONDA-INSIDE-ELSE gauge name "+"storm_"+dp.name);

            }
            else if(dp.value instanceof Integer){
                Gauge duration = Gauge.build()
                        .name(metricName)
                        .help(metricName)
                        .register(registry);
                duration.set(((Integer)dp.value).doubleValue());
                //LOG.info("SONDA-INSIDE-ELSE gauge name "+"storm_"+dp.name);

            }
            else{
                //LOG.info("SONDA ELSEBLOCK "+dp.value.getClass());
            }
        }

        PushGateway pg = new PushGateway(PROMURL);
        try {
            pg.pushAdd(registry, "stormMetrics",arg0.srcComponentId+"_"+arg0.srcTaskId+"_"+arg0.srcWorkerHost+"_"+arg0.srcWorkerPort);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            //LOG.warn("SONDA######"+e.getMessage());
            e.printStackTrace();
        }
        //LOG.info("############SONDA!!! sent to prometheus");

    }

    @Override
    public void prepare(Map arg0, Object arg1, TopologyContext arg2, IErrorReporter arg3) {
		/* TODO Auto-generated method stub
		org.eclipse.jetty.server.Server server	=	new org.eclipse.jetty.server.Server(PORT);
		ServletContextHandler			context	=	new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
	//	context.addServlet(new ServletHolder(
		//	      new MetricsServlet()), "/metrics");
			  // Put your application setup code here.
			  try {
				server.start();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/
    }
}
