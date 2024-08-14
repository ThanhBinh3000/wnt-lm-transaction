package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.entity.Entity;
import vn.com.gsoft.transaction.entity.LichSuCapNhatThanhVien;
import vn.com.gsoft.transaction.entity.NhaThuocs;
import vn.com.gsoft.transaction.model.dto.*;

import java.util.List;

public interface NhaThuocsService extends BaseService<NhaThuocs, NhaThuocsReq, Long> {

    List<Entity> searchListEntity(EntityReq rq) throws Exception;

    List<LichSuCapNhatThanhVien> searchLichSuCapNhatThanhVien(String maThanhVien) throws Exception;

    Boolean deleteByMaNhaThuoc(NhaThuocsReq req) throws Exception;
}