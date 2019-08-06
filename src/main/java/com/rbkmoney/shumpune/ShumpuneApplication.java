package com.rbkmoney.shumpune;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication
public class ShumpuneApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShumpuneApplication.class, args);
    }

}
