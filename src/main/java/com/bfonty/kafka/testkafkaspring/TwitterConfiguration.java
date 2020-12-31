package com.bfonty.kafka.testkafkaspring;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@ConfigurationProperties(prefix="twitter")
@Getter
@Setter
public class TwitterConfiguration {
    private String key;
    private String secret;
    private String token;
    private String tokenSecret;
}
