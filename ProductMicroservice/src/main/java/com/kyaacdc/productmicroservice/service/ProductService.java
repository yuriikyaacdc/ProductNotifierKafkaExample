package com.kyaacdc.productmicroservice.service;

import com.kyaacdc.productmicroservice.service.dto.CreateProductDto;

import java.util.concurrent.ExecutionException;

public interface ProductService {
    String createProduct(CreateProductDto createProductDto) throws ExecutionException, InterruptedException;
}
