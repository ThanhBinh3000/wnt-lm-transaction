package vn.com.gsoft.transaction.model.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import vn.com.gsoft.transaction.model.system.BaseRequest;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
public class NhaThuocsReq extends BaseRequest {
    private String maNhaThuoc;
    private String tenNhaThuoc;
    private String diaChi;
    private String dienThoai;
    private String nguoiDaiDien;
    private String email;
    private String mobile;
    private String duocSy;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
    private Date created;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd/MM/yyyy HH:mm:ss")
    private Date modified;
    private Long createdByUserId;
    private Long modifiedByUserId;
    private Boolean hoatDong;
    private Long tinhThanhId;
    private Boolean isConnectivity;
    private String description;
    private Long regionId;
    private Long cityId;
    private Long wardId;
    private Integer entityId;
    private String userName;
    private String password;
}
