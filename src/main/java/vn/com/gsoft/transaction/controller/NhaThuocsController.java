package vn.com.gsoft.transaction.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import vn.com.gsoft.transaction.model.dto.*;
import vn.com.gsoft.transaction.response.BaseResponse;
import vn.com.gsoft.transaction.service.NhaThuocsService;
import vn.com.gsoft.transaction.constant.PathConstant;
import vn.com.gsoft.transaction.util.system.ResponseUtils;

@Slf4j
@RestController
@RequestMapping("/thanh-vien")
public class NhaThuocsController {

    @Autowired
    private NhaThuocsService service;


    @PostMapping(value = PathConstant.URL_SEARCH_PAGE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> colection(@RequestBody NhaThuocsReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchPage(objReq)));
    }

    @PostMapping(value = PathConstant.URL_SEARCH_LIST, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> colectionList(@RequestBody NhaThuocsReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchList(objReq)));
    }


    @PostMapping(value = PathConstant.URL_CREATE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> insert(@Valid @RequestBody NhaThuocsReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.create(objReq)));
    }


    @PostMapping(value = PathConstant.URL_UPDATE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> update(@Valid @RequestBody NhaThuocsReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.update(objReq)));
    }


    @GetMapping(value = PathConstant.URL_DETAIL, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> detail(@PathVariable("id") Long id) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.detail(id)));
    }


    @PostMapping(value = PathConstant.URL_DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> delete(@Valid @RequestBody NhaThuocsReq idSearchReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.delete(idSearchReq.getId())));
    }

    @PostMapping(value =  PathConstant.URL_SEARCH_LIST + "-cap-thanh-vien", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> searchListCapThanhVien(@Valid @RequestBody EntityReq rq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchListEntity(rq)));
    }
    @PostMapping(value =  PathConstant.URL_SEARCH_LIST + "-lich-su-thanh-vien", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> searchListLichSuThanhVien(@Valid @RequestBody LichSuCapNhatThanhVienReq rq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchLichSuCapNhatThanhVien(rq.getMaThanhVien())));
    }
}
