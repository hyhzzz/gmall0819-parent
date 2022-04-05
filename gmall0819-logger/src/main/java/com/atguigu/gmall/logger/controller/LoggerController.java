package com.atguigu.gmall.logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author CoderHyh
 * @create 2022-03-22 17:44
 */
@RestController
@Slf4j
public class LoggerController {
    
    @Autowired
    private KafkaTemplate kafkaTemplate;

    //接收applog请求
    //接口都是给前端返回的，一般都是string
    @RequestMapping("/applog")

    //接收param参数
    // mock.url: "http://192.168.1.10:8081/applog"：这个相当于我拉了一辆车带了很多货  param:货的名字
    //@RequestParam("param") String jsonStr：接收数据
    public String log(@RequestParam("param") String jsonStr) {
        //1.打印输出到控制台
        //System.out.println(jsonStr);

        //2.把数据落盘，将来给离线数据准备
        log.info(jsonStr);

        //3.将json日志数据发送到kafka的主题(ods层)把数据写入到kafka，成为ods层数据
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }
}
