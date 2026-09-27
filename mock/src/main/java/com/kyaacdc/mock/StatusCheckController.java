package com.kyaacdc.mock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/response")
public class StatusCheckController {

    private Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @GetMapping("/200")
    public ResponseEntity<String> response200String() {

        LOGGER.info("Response 200");

        return ResponseEntity.ok("OK 200");
    }

    @GetMapping("/500")
    public ResponseEntity<String> response500String() {

        LOGGER.info("Response 500");

        return ResponseEntity.internalServerError().build();
    }
}
