package com.mark.cron;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by fellowlei on 2018/5/8.
 */
@Component
public class CronTask {
    @Scheduled(cron = "0/5 * * * * ?")
    public void cronTest() {
        Date date = new Date();
        SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sim.format(date);
        System.out.println("CronTask这是spring定时器1，每五秒执行一次,当前时间：" + dateStr);
    }

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = new ClassPathXmlApplicationContext("spring.xml");
        System.out.println("start");
        Thread.sleep(1000 * 60);
        System.out.println("end");
    }
}
