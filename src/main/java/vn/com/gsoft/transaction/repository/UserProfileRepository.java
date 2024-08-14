package vn.com.gsoft.transaction.repository;

import jakarta.persistence.Tuple;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import vn.com.gsoft.transaction.entity.UserProfile;
import vn.com.gsoft.transaction.model.dto.UserProfileReq;

import java.util.List;
import java.util.Optional;

@Repository
public interface UserProfileRepository extends BaseRepository<UserProfile, UserProfileReq, Long> {
    @Query("SELECT c FROM UserProfile c " +
            " WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.userName} IS NULL OR lower(c.userName) LIKE lower(concat('%',CONCAT(:#{#param.userName},'%'))))"
            + " AND (:#{#param.tenDayDu} IS NULL OR lower(c.tenDayDu) LIKE lower(concat('%',CONCAT(:#{#param.tenDayDu},'%'))))"
            + " AND (:#{#param.email} IS NULL OR lower(c.email) LIKE lower(concat('%',CONCAT(:#{#param.email},'%'))))"
            + " AND (:#{#param.soDienThoai} IS NULL OR lower(c.soDienThoai) LIKE lower(concat('%',CONCAT(:#{#param.soDienThoai},'%'))))"
            + " AND (:#{#param.maNhaThuoc} IS NULL OR lower(c.maNhaThuoc) LIKE lower(concat('%',CONCAT(:#{#param.maNhaThuoc},'%'))))"
            + " AND (:#{#param.soCMT} IS NULL OR lower(c.soCMT) LIKE lower(concat('%',CONCAT(:#{#param.soCMT},'%'))))"
            + " AND (:#{#param.regionId} IS NULL OR c.regionId = :#{#param.regionId}) "
            + " AND (:#{#param.cityId} IS NULL OR c.cityId = :#{#param.cityId}) "
            + " AND (:#{#param.wardId} IS NULL OR c.wardId = :#{#param.wardId}) "
            + " AND (:#{#param.addresses} IS NULL OR lower(c.addresses) LIKE lower(concat('%',CONCAT(:#{#param.addresses},'%'))))"
            + " AND (:#{#param.tokenDevice} IS NULL OR lower(c.tokenDevice) LIKE lower(concat('%',CONCAT(:#{#param.tokenDevice},'%'))))"
            + " AND (:#{#param.tokenBrowser} IS NULL OR lower(c.tokenBrowser) LIKE lower(concat('%',CONCAT(:#{#param.tokenBrowser},'%'))))"
            + " AND (:#{#param.password} IS NULL OR lower(c.password) LIKE lower(concat('%',CONCAT(:#{#param.password},'%'))))"
            + " AND (:#{#param.entityId} IS NULL OR c.entityId = :#{#param.entityId}) "
            + " AND (:#{#param.hoatDong} IS NULL OR c.hoatDong = :#{#param.hoatDong}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.recordStatusIds} IS NULL OR c.recordStatusId in :#{#param.recordStatusIds}) "
            + " ORDER BY c.id desc"
    )
    Page<UserProfile> searchPage(@Param("param") UserProfileReq param, Pageable pageable);


    @Query("SELECT c FROM UserProfile c " +
            " WHERE 1=1 "
            + " AND (:#{#param.id} IS NULL OR c.id = :#{#param.id}) "
            + " AND (:#{#param.userName} IS NULL OR lower(c.userName) LIKE lower(concat('%',CONCAT(:#{#param.userName},'%'))))"
            + " AND (:#{#param.tenDayDu} IS NULL OR lower(c.tenDayDu) LIKE lower(concat('%',CONCAT(:#{#param.tenDayDu},'%'))))"
            + " AND (:#{#param.email} IS NULL OR lower(c.email) LIKE lower(concat('%',CONCAT(:#{#param.email},'%'))))"
            + " AND (:#{#param.soDienThoai} IS NULL OR lower(c.soDienThoai) LIKE lower(concat('%',CONCAT(:#{#param.soDienThoai},'%'))))"
            + " AND (:#{#param.maNhaThuoc} IS NULL OR lower(c.maNhaThuoc) LIKE lower(concat('%',CONCAT(:#{#param.maNhaThuoc},'%'))))"
            + " AND (:#{#param.soCMT} IS NULL OR lower(c.soCMT) LIKE lower(concat('%',CONCAT(:#{#param.soCMT},'%'))))"
            + " AND (:#{#param.regionId} IS NULL OR c.regionId = :#{#param.regionId}) "
            + " AND (:#{#param.cityId} IS NULL OR c.cityId = :#{#param.cityId}) "
            + " AND (:#{#param.wardId} IS NULL OR c.wardId = :#{#param.wardId}) "
            + " AND (:#{#param.addresses} IS NULL OR lower(c.addresses) LIKE lower(concat('%',CONCAT(:#{#param.addresses},'%'))))"
            + " AND (:#{#param.tokenDevice} IS NULL OR lower(c.tokenDevice) LIKE lower(concat('%',CONCAT(:#{#param.tokenDevice},'%'))))"
            + " AND (:#{#param.tokenBrowser} IS NULL OR lower(c.tokenBrowser) LIKE lower(concat('%',CONCAT(:#{#param.tokenBrowser},'%'))))"
            + " AND (:#{#param.password} IS NULL OR lower(c.password) LIKE lower(concat('%',CONCAT(:#{#param.password},'%'))))"
            + " AND (:#{#param.entityId} IS NULL OR c.entityId = :#{#param.entityId}) "
            + " AND (:#{#param.hoatDong} IS NULL OR c.hoatDong = :#{#param.hoatDong}) "
            + " AND (:#{#param.recordStatusId} IS NULL OR c.recordStatusId = :#{#param.recordStatusId})"
            + " AND (:#{#param.recordStatusIds} IS NULL OR c.recordStatusId in :#{#param.recordStatusIds}) "
            + " ORDER BY c.id desc"
    )
    List<UserProfile> searchList(@Param("param") UserProfileReq param);

    @Query(value =
            "SELECT  up.id as id, up.UserName as userName, up.SoDienThoai AS soDienThoai , nv.Role AS role, " +
                    "  up.Email as email, up.HoatDong as hoatDong, up.TenDayDu as tenDayDu " +
                    "FROM UserProfile up " +
                    " WHERE 1 = 1" +
                    " AND (:#{#param.hoatDong} IS NULL OR up.HoatDong = :#{#param.hoatDong}) " +
                    " AND (:#{#param.recordStatusId} IS NULL OR up.recordStatusId = :#{#param.recordStatusId})" +
                    " AND (:#{#param.maNhaThuoc} IS NULL OR nv.NhaThuoc_MaNhaThuoc = :#{#param.maNhaThuoc}) " +
                    " AND ((:#{#param.textSearch} IS NULL OR lower(up.UserName) LIKE lower(concat('%',CONCAT(:#{#param.textSearch},'%'))))" +
                    " OR (:#{#param.textSearch} IS NULL OR lower(up.TenDayDu) LIKE lower(concat('%',CONCAT(:#{#param.textSearch},'%')))))" +
                    " ORDER BY up.id desc", nativeQuery = true
    )
    Page<Tuple> searchPageStaffManagement(@Param("param") UserProfileReq param, Pageable pageable);

    Optional<UserProfile> findByUserName(String userName);

    @Query(value =
            "SELECT  up.id as id, up.UserName as userName, up.SoDienThoai AS soDienThoai , nv.Role AS role, " +
                    "  up.Email as email, up.HoatDong as hoatDong, up.TenDayDu as tenDayDu " +
                    "FROM UserProfile up " +
                    " WHERE 1 = 1" +
                    " AND (:#{#param.hoatDong} IS NULL OR up.HoatDong = :#{#param.hoatDong}) " +
                    " AND (:#{#param.recordStatusId} IS NULL OR up.recordStatusId = :#{#param.recordStatusId})" +
                    " AND (:#{#param.maNhaThuoc} IS NULL OR nv.NhaThuoc_MaNhaThuoc = :#{#param.maNhaThuoc}) " +
                    " AND ((:#{#param.textSearch} IS NULL OR lower(up.UserName) LIKE lower(concat('%',CONCAT(:#{#param.textSearch},'%'))))" +
                    " OR (:#{#param.textSearch} IS NULL OR lower(up.TenDayDu) LIKE lower(concat('%',CONCAT(:#{#param.textSearch},'%')))))" +
                    " ORDER BY up.id desc", nativeQuery = true
    )
    List<Tuple> searchListStaffManagement(@Param("param") UserProfileReq param);
    Optional<UserProfile> findByUserNameAndHoatDong(String userName, Boolean hoatDong);
}
