package vn.com.gsoft.transaction.service;

import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface RedisListService {

    List<Object> getGiaoDichHangHoaValues(GiaoDichHangHoaReq rep);
    void pushDataRedis(List<GiaoDichHangHoa> giaoDichHangHoas);
    List<Map<Object, Object>> getTransactionsByTimeRangeAcrossDays(Date startDate, Date endDate);
}
