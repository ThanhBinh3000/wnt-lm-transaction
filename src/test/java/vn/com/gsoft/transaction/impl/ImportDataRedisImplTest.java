package vn.com.gsoft.transaction.impl;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import vn.com.gsoft.transaction.service.GiaoDichHangHoaService;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@Slf4j
class ImportDataRedisImplTest {
    @Autowired
    private GiaoDichHangHoaService giaoDichHangHoaService;
//
//    @BeforeAll
//    static void beforeAll() {
//
//    }
//    @Test
//    void pushData() throws Exception {
//        giaoDichHangHoaService.pushData();
//    }
}