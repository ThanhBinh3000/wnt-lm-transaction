package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.model.dto.TopMatHangRes;

import java.util.List;

public interface GiaoDichHangHoaService extends BaseService<GiaoDichHangHoa, GiaoDichHangHoaReq, Long> {
    List<TopMatHangRes> topDoanhThuBanChay(GiaoDichHangHoaReq req) throws Exception;
    List<TopMatHangRes> topTSLNCaoNhat(GiaoDichHangHoaReq req) throws Exception;
    List<TopMatHangRes> topSLBanChay(GiaoDichHangHoaReq req) throws Exception;
    void pushData() throws Exception;
    void pushDataThreeLastMonth() throws Exception;
}