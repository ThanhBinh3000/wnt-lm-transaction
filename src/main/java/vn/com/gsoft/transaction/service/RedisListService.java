package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaCache;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaRes;
import vn.com.gsoft.transaction.model.dto.TopMatHangRes;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface RedisListService {

    List<Object> getGiaoDichHangHoaValues(GiaoDichHangHoaReq rep);
    void pushDataRedis(List<GiaoDichHangHoa> giaoDichHangHoas);
    void pushDataToRedisByTime(List<GiaoDichHangHoaCache> dataList, String key);
    List<GiaoDichHangHoaCache> getAllDataKey(String key);
    List<GiaoDichHangHoaCache> getAllDataDetailByKeys(List<String> keys) throws Exception;
    void pushDataToRedis(List<TopMatHangRes> dataList, String time, String type);
    void saveTransaction(String transactionId, long transactionTimestamp, Map<String, GiaoDichHangHoaCache> transactionDetails);
}
