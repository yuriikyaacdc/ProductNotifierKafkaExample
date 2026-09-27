package com.kyaacdc.productmicroservice.controller;

import com.kyaacdc.productmicroservice.service.ProductService;
import com.kyaacdc.productmicroservice.service.dto.CreateProductDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@RestController
@RequestMapping("/product")
public class ProductController {

    private ProductService productService;
    private Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping
    public ResponseEntity<Object> createProduct(@RequestBody CreateProductDto createProductDto)  {

        String productId = null;
        try {
            productId = productService.createProduct(createProductDto);
        } catch (Exception e) {
            return ResponseEntity.status(INTERNAL_SERVER_ERROR).body(new ErrorMessage(e));
        }

        return ResponseEntity.status(CREATED).body(productId);
    }
}
