package com.atguigu.offline;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeTest {

    @Test
    public void testTime(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM/dd/HHmm");
        String format = simpleDateFormat.format(new Date());
        System.out.println(format);
    }
}
