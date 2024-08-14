package vn.com.gsoft.transaction.repository;

import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import vn.com.gsoft.transaction.constant.RecordStatusContains;
import vn.com.gsoft.transaction.entity.Role;
import vn.com.gsoft.transaction.model.dto.RoleReq;

import java.util.List;
import java.util.Optional;

@Repository
public interface RoleRepository extends BaseRepository<Role, RoleReq, Long> {
    @Query("SELECT c FROM Role c " +
            "WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.roleName} IS NULL OR lower(c.roleName) LIKE lower(concat('%',CONCAT(:#{#param.roleName},'%'))))"
            + " AND (:#{#param.maNhaThuoc} IS NULL OR lower(c.maNhaThuoc) LIKE lower(concat('%',CONCAT(:#{#param.maNhaThuoc},'%'))))"
            + " AND (:#{#param.description} IS NULL OR lower(c.description) LIKE lower(concat('%',CONCAT(:#{#param.description},'%'))))"
            + " AND (:#{#param.type} IS NULL OR c.type = :#{#param.type}) "
            + " ORDER BY c.id desc"
    )
    Page<Role> searchPage(@Param("param") RoleReq param, Pageable pageable);


    @Query("SELECT c FROM Role c " +
            "WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.roleName} IS NULL OR lower(c.roleName) LIKE lower(concat('%',CONCAT(:#{#param.roleName},'%'))))"
            + " AND (:#{#param.maNhaThuoc} IS NULL OR lower(c.maNhaThuoc) LIKE lower(concat('%',CONCAT(:#{#param.maNhaThuoc},'%'))))"
            + " AND (:#{#param.description} IS NULL OR lower(c.description) LIKE lower(concat('%',CONCAT(:#{#param.description},'%'))))"
            + " AND (:#{#param.type} IS NULL OR c.type = :#{#param.type}) "
            + " ORDER BY c.id desc"
    )
    List<Role> searchList(@Param("param") RoleReq param);

    @Query("SELECT c FROM Role c " +
            "join RoleType rt on rt.roleId = c.id " +
            "WHERE 1=1 "
            + " AND (c.type = ?1) "
            + " AND (c.recordStatusId = " + RecordStatusContains.ACTIVE + ")"
            + " AND (c.isDefault = ?2)"
            + " AND (rt.roleName = ?3)"
    )
    Optional<Role> findByTypeAndIsDefault(int type, boolean isDefault, String roleName);

    @Query("SELECT c FROM Role c " +
            "join RoleType rt on rt.roleId = c.id " +
            "WHERE 1=1 "
            + " AND (c.maNhaThuoc = ?1) "
            + " AND (c.type = ?2) "
            + " AND (c.recordStatusId = " + RecordStatusContains.ACTIVE + ")"
            + " AND (c.isDefault = ?3)"
            + " AND (rt.roleName = ?4)"
    )
    Optional<Role> findByMaNhaThuocAndTypeAndIsDefault(String maNhaThuoc, int type, boolean isDefault, String roleName);
}
