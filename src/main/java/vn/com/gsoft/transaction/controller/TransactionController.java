package vn.com.gsoft.transaction.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.response.BaseResponse;
import vn.com.gsoft.transaction.service.GiaoDichHangHoaService;
import vn.com.gsoft.transaction.util.system.ResponseUtils;

@Slf4j
@RestController
@RequestMapping("/thong-ke")
public class TransactionController {
    @Autowired
    private GiaoDichHangHoaService service;

    @PostMapping(value = "top-doanh-thu", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> topDoanhThu(@RequestBody GiaoDichHangHoaReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.topDoanhThuBanChay(objReq)));
    }

    @PostMapping(value = "top-so-luong", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> topSoLuong(@RequestBody GiaoDichHangHoaReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.topSLBanChay(objReq)));
    }

    @PostMapping(value = "top-tsln", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> topTSLN(@RequestBody GiaoDichHangHoaReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.topTSLNCaoNhat(objReq)));
    }
}
