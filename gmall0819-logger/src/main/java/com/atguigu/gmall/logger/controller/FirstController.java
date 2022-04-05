package com.atguigu.gmall.logger.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author CoderHyh
 * @create 2022-03-22 17:43
 * 回顾sparingmvc controller
 * @Controller:将类对象的创建交给spring容器，但是用@Controller标记的类，如果方法的返回值是字符串， 那么认为是进行页面的跳转, 如果要是将字符串直接响应的话，需要在方法上加@ResponseBody
 * @RestController:相当于 @Controller+@ResponseBody 会将返回结果转换为json进行响应
 * @RequestParam:接受请求参数 赋值给方法形参
 */
@RestController
public class FirstController {

    //拦截first请求交给first方法处理
    //http://localhost:8080/first?username=atguigu&password=123456 浏览器输入  返回结果：atguigu::123456
    @RequestMapping("/first")
    //http://localhost:8080/first?hh=atguigu&ll=123456
    public String first(@RequestParam("hh") String username, @RequestParam("ll") String password) {
        System.out.println(username + "::" + password);
        return "success";
    }
}
