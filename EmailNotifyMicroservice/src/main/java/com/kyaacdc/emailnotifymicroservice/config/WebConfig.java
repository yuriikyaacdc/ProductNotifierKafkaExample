package com.kyaacdc.emailnotifymicroservice.config;

import jakarta.servlet.annotation.WebServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class WebConfig {

    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

}
