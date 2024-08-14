package vn.com.gsoft.transaction.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.entity.RolePrivilege;
import vn.com.gsoft.transaction.model.dto.RolePrivilegeReq;
import vn.com.gsoft.transaction.repository.RolePrivilegeRepository;
import vn.com.gsoft.transaction.service.RolePrivilegeService;

@Service
public class RolePrivilegeServiceImpl extends BaseServiceImpl<RolePrivilege, RolePrivilegeReq, Long> implements RolePrivilegeService {
    private RolePrivilegeRepository hdrRepo;

    @Autowired
    public RolePrivilegeServiceImpl(RolePrivilegeRepository hdrRepo) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
    }
}
