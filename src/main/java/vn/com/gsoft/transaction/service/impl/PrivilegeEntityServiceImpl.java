package vn.com.gsoft.transaction.service.impl;

import vn.com.gsoft.transaction.entity.*;
import vn.com.gsoft.transaction.model.dto.PrivilegeEntityReq;
import vn.com.gsoft.transaction.repository.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.service.PrivilegeEntityService;


@Service
@Log4j2
public class PrivilegeEntityServiceImpl extends BaseServiceImpl<PrivilegeEntity, PrivilegeEntityReq, Long> implements PrivilegeEntityService {

    private PrivilegeEntityRepository hdrRepo;

    @Autowired
    public PrivilegeEntityServiceImpl(PrivilegeEntityRepository hdrRepo) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
    }

}
