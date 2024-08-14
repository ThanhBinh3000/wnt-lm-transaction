package vn.com.gsoft.transaction.service.impl;

import vn.com.gsoft.transaction.entity.*;
import vn.com.gsoft.transaction.model.dto.UserRoleReq;
import vn.com.gsoft.transaction.repository.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.service.UserRoleService;


@Service
@Log4j2
public class UserRoleServiceImpl extends BaseServiceImpl<UserRole, UserRoleReq, Long> implements UserRoleService {

    private UserRoleRepository hdrRepo;

    @Autowired
    public UserRoleServiceImpl(UserRoleRepository hdrRepo) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
    }

}
