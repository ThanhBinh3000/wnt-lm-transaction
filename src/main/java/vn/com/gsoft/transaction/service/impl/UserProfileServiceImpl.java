package vn.com.gsoft.transaction.service.impl;


import jakarta.transaction.Transactional;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.RoleTypeConstant;
import vn.com.gsoft.transaction.entity.Role;
import vn.com.gsoft.transaction.entity.UserProfile;
import vn.com.gsoft.transaction.model.dto.*;
import vn.com.gsoft.transaction.model.system.Profile;
import vn.com.gsoft.transaction.repository.UserProfileRepository;
import vn.com.gsoft.transaction.service.*;
import vn.com.gsoft.transaction.util.system.DataUtils;

import java.util.List;
import java.util.Optional;

@Service
@Log4j2
public class UserProfileServiceImpl extends BaseServiceImpl<UserProfile, UserProfileReq, Long> implements UserProfileService {

    private UserProfileRepository hdrRepo;
    private PasswordEncoder passwordEncoder;
    private NhaThuocsService nhaThuocsService;
    private RoleService roleService;
    private UserRoleService userRoleService;

    @Autowired
    public UserProfileServiceImpl(UserProfileRepository hdrRepo, PasswordEncoder passwordEncoder, NhaThuocsService nhaThuocsService, RoleService roleService, UserRoleService userRoleService) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
        this.passwordEncoder = passwordEncoder;
        this.nhaThuocsService = nhaThuocsService;
        this.roleService = roleService;
        this.userRoleService = userRoleService;
    }

    @Override
    public Page<UserProfile> searchPage(UserProfileReq req) throws Exception {
        Pageable pageable = PageRequest.of(req.getPaggingReq().getPage(), req.getPaggingReq().getLimit());
        req.setRecordStatusIds(List.of(0L, 1L, 2L));
        req.setMaNhaThuoc(getLoggedUser().getNhaThuoc().getMaNhaThuoc());
        return hdrRepo.searchPage(req, pageable);
    }

    @Override
    public Page<UserStaffProfileRes> searchPageStaffManagement(UserProfileReq req) throws Exception {
        Pageable pageable = PageRequest.of(req.getPaggingReq().getPage(), req.getPaggingReq().getLimit());
        req.setMaNhaThuoc(getLoggedUser().getNhaThuoc().getMaNhaThuoc());
        return DataUtils.convertPage(hdrRepo.searchPageStaffManagement(req, pageable), UserStaffProfileRes.class);
    }

    @Override
    public Boolean changePassword(ChangePasswordReq req) throws Exception {
        Long userId = getLoggedUser().getId();
        if (!req.getNewPassword().equals(req.getConfirmPassword())) {
            throw new Exception("Mật khẩu mới và xác nhận mật khẩu không trùng khớp!");
        }
        Optional<UserProfile> userProfile = this.hdrRepo.findById(userId);
        if (userProfile.isPresent()) {
            if (this.passwordEncoder.matches(req.getOldPassword(), userProfile.get().getPassword())) {
                userProfile.get().setPassword(this.passwordEncoder.encode(req.getNewPassword()));
                return true;
            } else {
                throw new Exception("Mật khẩu cũ không đúng!");
            }
        }

        return false;
    }

    @Override
    public Boolean resetPassword(ChangePasswordReq req) throws Exception {
        Long userId = null;
        if (!req.getNewPassword().equals(req.getConfirmPassword())) {
            throw new Exception("Mật khẩu mới và xác nhận mật khẩu không trùng khớp!");
        }
        if (req.getUserId() == null) {
            throw new Exception("Không được để trống userId!");
        } else {
            userId = req.getUserId();
        }
        Optional<UserProfile> userProfile = this.hdrRepo.findById(userId);
        if (userProfile.isPresent()) {
            userProfile.get().setPassword(this.passwordEncoder.encode(req.getNewPassword()));
            return true;
        }

        return false;
    }

    @Override
    @Transactional
    public UserProfile insertUser(UserProfileReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        if (req.getMaNhaThuoc() == null)
            throw new Exception("maNhaThuoc không được để trống!");

        Optional<UserProfile> userProfile = this.hdrRepo.findByUserName(req.getUserName());
        if (userProfile.isPresent()) {
            throw new Exception("UserName đã tồn tại!");
        }
        UserProfile e = new UserProfile();
        BeanUtils.copyProperties(req, e, "id");

        e.setPassword(this.passwordEncoder.encode(e.getPassword()));
        if (e.getHoatDong() == null) {
            e.setHoatDong(true);
        }

        e = hdrRepo.save(e);

        Optional<Role> role = this.roleService.findByTypeAndIsDefaultAndRoleName(0, true, RoleTypeConstant.ADMIN);
        if (role.isEmpty()) {
            throw new Exception("Không tìm thấy role mặc định!");
        }
        UserRoleReq ur = new UserRoleReq();
        ur.setUserId(e.getId());
        ur.setRoleId(role.get().getId());
        this.userRoleService.create(ur);

        return e;
    }

    @Override
    @Transactional
    public UserProfile updateUser(UserProfileReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        Optional<UserProfile> optional = hdrRepo.findById(req.getId());
        if (optional.isEmpty()) {
            throw new Exception("Không tìm thấy dữ liệu.");
        }
        if (!optional.get().getUserName().equals(req.getUserName())) {
            Optional<UserProfile> userProfile = this.hdrRepo.findByUserName(req.getUserName());
            if (userProfile.isPresent()) {
                throw new Exception("UserName đã tồn tại!");
            }
        }
        UserProfile e = optional.get();
        BeanUtils.copyProperties(req, e, "id", "password", "hoatDong", "enableNT", "created", "createdByUserId", "modified", "modifiedByUserId", "recordStatusId");
        hdrRepo.save(e);
        return e;
    }

    @Override
    @Transactional
    public UserProfile insertStaff(UserProfileReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        Optional<UserProfile> userProfile = this.hdrRepo.findByUserName(req.getUserName());
        if (userProfile.isPresent()) {
            throw new Exception("UserName đã tồn tại!");
        }
        UserProfile e = new UserProfile();
        BeanUtils.copyProperties(req, e, "id");

        e.setPassword(this.passwordEncoder.encode(e.getPassword()));
        if (e.getHoatDong() == null) {
            e.setHoatDong(true);
        }
        e = hdrRepo.save(e);

        Optional<Role> role = this.roleService.findByMaNhaThuocAndTypeAndIsDefaultAndRoleName(getLoggedUser().getNhaThuoc().getMaNhaThuoc(), 1, true, RoleTypeConstant.USER);
        if (role.isEmpty()) {
            role = this.roleService.findByTypeAndIsDefaultAndRoleName(0, true, RoleTypeConstant.USER);
            if (role.isEmpty()) {
                throw new Exception("Không tìm thấy role mặc định!");
            }
        }
        UserRoleReq ur = new UserRoleReq();
        ur.setUserId(e.getId());
        ur.setRoleId(role.get().getId());
        this.userRoleService.create(ur);
        return e;
    }

    @Override
    @Transactional
    public UserProfile updateStaff(UserProfileReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");

        Optional<UserProfile> optional = hdrRepo.findById(req.getId());
        if (optional.isEmpty()) {
            throw new Exception("Không tìm thấy dữ liệu.");
        }
        if (!optional.get().getUserName().equals(req.getUserName())) {
            Optional<UserProfile> userProfile = this.hdrRepo.findByUserName(req.getUserName());
            if (userProfile.isPresent()) {
                throw new Exception("UserName đã tồn tại!");
            }
        }
        UserProfile e = optional.get();
        BeanUtils.copyProperties(req, e, "id", "password", "hoatDong", "enableNT", "created", "createdByUserId", "modified", "modifiedByUserId", "recordStatusId");

        hdrRepo.save(e);
        return e;
    }

    @Override
    public List<UserStaffProfileRes> searchListStaffManagement(UserProfileReq req) throws Exception {
        req.setMaNhaThuoc(getLoggedUser().getNhaThuoc().getMaNhaThuoc());
        return DataUtils.convertList(hdrRepo.searchListStaffManagement(req), UserStaffProfileRes.class);
    }
}
