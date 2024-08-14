package vn.com.gsoft.transaction.service;

import org.springframework.data.domain.Page;
import vn.com.gsoft.transaction.entity.UserProfile;
import vn.com.gsoft.transaction.model.dto.ChangePasswordReq;
import vn.com.gsoft.transaction.model.dto.UserProfileReq;
import vn.com.gsoft.transaction.model.dto.UserStaffProfileRes;

import java.util.List;

public interface UserProfileService extends BaseService<UserProfile, UserProfileReq, Long> {

    Page<UserStaffProfileRes> searchPageStaffManagement(UserProfileReq objReq) throws Exception;

    Boolean changePassword(ChangePasswordReq objReq) throws Exception;

    Boolean resetPassword(ChangePasswordReq objReq) throws Exception;

    UserProfile insertUser(UserProfileReq objReq) throws Exception;

    UserProfile updateUser(UserProfileReq objReq) throws Exception;

    UserProfile insertStaff(UserProfileReq objReq) throws Exception;

    UserProfile updateStaff(UserProfileReq objReq) throws Exception;

    List<UserStaffProfileRes> searchListStaffManagement(UserProfileReq objReq) throws Exception;
}