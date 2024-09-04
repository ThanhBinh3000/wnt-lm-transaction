package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaRes;
import vn.com.gsoft.transaction.model.dto.TopMatHangRes;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface RedisListService {

    List<Object> getGiaoDichHangHoaValues(GiaoDichHangHoaReq rep);
    void pushDataRedis(List<GiaoDichHangHoa> giaoDichHangHoas);
    void pushDataToRedisByTime(List<TopMatHangRes> dataList, String time, String type);
    List<TopMatHangRes> getAllDataFromRedis(String code);
}
