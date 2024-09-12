package vn.com.gsoft.transaction.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.*;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisListService {

    List<Object> getGiaoDichHangHoaValues(GiaoDichHangHoaReq rep);
    void pushDataRedis(List<GiaoDichHangHoa> giaoDichHangHoas);
    void pushDataToRedisByTime(HangHoaDaTinhToanCache data, String key);
    List<GiaoDichHangHoaCache> getAllDataKey(String key);
    List<HangHoaDaTinhToanCache> getAllDataDetailByKeys(List<String> keys, Integer type) throws Exception;
    void pushDataToRedis3T(List<TopMatHangRes> dataList, String time, String type);
}
