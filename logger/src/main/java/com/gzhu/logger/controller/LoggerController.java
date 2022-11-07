package com.gzhu.logger.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerController.class);
    // 引入kafka，kafka的key相同，会发送到同一个分区，每条消息只会发送到主题的某一个分区
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr){
        // 将日志标记成info的模式，结合logback落盘并打印到控制台
        log.info(jsonStr);
        // ODS层
        // 原始数据到Kafka
        kafkaTemplate.send("ods_base_log",jsonStr);
        return null;
    }
}
