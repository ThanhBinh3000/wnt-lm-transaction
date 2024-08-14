package vn.com.gsoft.transaction.model.dto;

import lombok.Data;

@Data
public class ChangePasswordReq {
    private Long userId;
    private String oldPassword;
    private String newPassword;
    private String confirmPassword;
}
