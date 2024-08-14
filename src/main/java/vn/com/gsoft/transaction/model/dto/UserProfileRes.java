package vn.com.gsoft.transaction.model.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class UserProfileRes {
    private Long id;
    private String userName;
    private String nhaThuocs;
    private String nhomQuyens;
    private String email;
    private Boolean HoatDong;
    private String tenDayDu;
    private String soDienThoai;
    private String soCMT;
}
