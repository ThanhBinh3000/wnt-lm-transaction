package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;

import java.util.List;

public interface GiaoDichHangHoaService extends BaseService<GiaoDichHangHoa, GiaoDichHangHoaReq, Long> {
    List<GiaoDichHangHoa> topDoanhThuBanChay(GiaoDichHangHoaReq req) throws Exception;
}