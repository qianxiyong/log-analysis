package com.atguigu.offline;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaToHDFS {

    private static FileSystem fs;
    private static FSDataOutputStream outputStream;
    private static String hdfsPath = "hdfs://hadoop101:9000";
    private static String user = "atguigu";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();

        //添加配置
        properties.put("zookeeper.connect", "hadoop102:2181");
        properties.put("group.id", "g1");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(config);
        Map<String,Integer> topic = new HashMap<>();
        topic.put("log-analysis",1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topic);

        KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get("log-analysis").get(0);
        ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStream.iterator();

        long currentTime = System.currentTimeMillis();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM/dd/HHmm");
        String totalPath = getPath(currentTime,simpleDateFormat);

        try {
            fs = FileSystem.get(new URI(hdfsPath),new Configuration(),user);
            if(fs.exists(new Path(totalPath))) {
                outputStream = fs.append(new Path(totalPath));
            }else{
                outputStream = fs.create(new Path(totalPath));
            }
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }

        while(consumerIterator.hasNext()) {
            byte[] logBytes = consumerIterator.next().message();
            String log = new String(logBytes);
            try {
                long nowTime = System.currentTimeMillis();
                if(nowTime - currentTime >= 6000) {
                    String currentPath = getPath(nowTime, simpleDateFormat);
                    outputStream.close();
                    outputStream = fs.create(new Path(currentPath));
                    currentTime = nowTime;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(log);
            outputStream.write((log+"\r\n").getBytes());
            outputStream.hflush();
        }
    }

    private static String getPath(long currentTime, SimpleDateFormat simpleDateFormat) {
        return hdfsPath + "/" + simpleDateFormat.format(currentTime);
    }
}
