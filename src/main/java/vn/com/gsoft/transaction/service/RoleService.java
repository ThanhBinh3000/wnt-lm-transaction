package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.entity.Role;
import vn.com.gsoft.transaction.model.dto.RoleReq;

import java.util.Optional;

public interface RoleService extends BaseService<Role, RoleReq, Long> {


    Optional<Role> findByTypeAndIsDefaultAndRoleName(int type, boolean isDefault, String roleName);

    Optional<Role> findByMaNhaThuocAndTypeAndIsDefaultAndRoleName(String maNhaThuoc, int type, boolean isDefault, String roleName);
}