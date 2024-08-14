package vn.com.gsoft.transaction.controller;


import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import vn.com.gsoft.transaction.model.dto.ChangePasswordReq;
import vn.com.gsoft.transaction.model.dto.UserProfileReq;
import vn.com.gsoft.transaction.response.BaseResponse;
import vn.com.gsoft.transaction.service.UserProfileService;
import vn.com.gsoft.transaction.constant.PathConstant;
import vn.com.gsoft.transaction.util.system.ResponseUtils;

@RestController
@RequestMapping(value = PathConstant.URL_NGUOI_DUNG)
@Slf4j
@Tag(name = "Thông tin người dùng ( Quản trị/Chủ/nhân viên)")
public class UserProfileController {

    @Autowired
    UserProfileService service;

    @Operation(summary = "Tra cứu", description = "Tra cứu")
    @PostMapping(value = PathConstant.URL_SEARCH_PAGE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> colection(@RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchPage(objReq)));
    }

    @Operation(summary = "Tra cứu", description = "Tra cứu")
    @PostMapping(value = PathConstant.URL_SEARCH_LIST, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> colectionList(@RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchList(objReq)));
    }

    @Operation(summary = "Tra cứu", description = "Tra cứu")
    @PostMapping(value = PathConstant.URL_SEARCH_PAGE + "-staff-management", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> colectionPageStaffManagement(@RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchPageStaffManagement(objReq)));
    }

    @Operation(summary = "Tra cứu", description = "Tra cứu")
    @PostMapping(value = PathConstant.URL_SEARCH_LIST + "-staff-management", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> colectionListStaffManagement(@RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.searchListStaffManagement(objReq)));
    }

    @Operation(summary = "Tạo mới", description = "Tạo mới")
    @PostMapping(value = PathConstant.URL_CREATE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> insert(@Valid @RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.create(objReq)));
    }

    @Operation(summary = "Update", description = "Update")
    @PostMapping(value = PathConstant.URL_UPDATE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> update(@Valid @RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.update(objReq)));
    }

    @Operation(summary = "Tạo mới user", description = "Tạo mới user")
    @PostMapping(value = PathConstant.URL_CREATE + "-user", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> insertUser(@Valid @RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.insertUser(objReq)));
    }

    @Operation(summary = "Update user", description = "Update user")
    @PostMapping(value = PathConstant.URL_UPDATE + "-user", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> updateUser(@Valid @RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.updateUser(objReq)));
    }

    @Operation(summary = "Tạo mới staff", description = "Tạo mới staff")
    @PostMapping(value = PathConstant.URL_CREATE + "-staff", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> insertStaff(@Valid @RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.insertStaff(objReq)));
    }

    @Operation(summary = "Update staff", description = "Update staff")
    @PostMapping(value = PathConstant.URL_UPDATE + "-staff", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> updateStaff(@Valid @RequestBody UserProfileReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.updateStaff(objReq)));
    }


    @Operation(summary = "Đổi mật khẩu", description = "Đổi mật khẩu")
    @PostMapping(value = "change-password", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> changePassword(@Valid @RequestBody ChangePasswordReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.changePassword(objReq)));
    }

    @Operation(summary = "Reset mật khẩu", description = "Reset mật khẩu")
    @PostMapping(value = "reset-password", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<BaseResponse> resetPassword(@Valid @RequestBody ChangePasswordReq objReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.resetPassword(objReq)));
    }

    @Operation(summary = "Lấy chi tiết", description = "Lấy chi tiết")
    @GetMapping(value = PathConstant.URL_DETAIL, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> detail(@PathVariable("id") Long id) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.detail(id)));
    }


    @Operation(summary = "Xóa thông tin", description = "Xóa thông tin")
    @PostMapping(value = PathConstant.URL_DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<BaseResponse> delete(@Valid @RequestBody UserProfileReq idSearchReq) throws Exception {
        return ResponseEntity.ok(ResponseUtils.ok(service.delete(idSearchReq.getId())));
    }

}
