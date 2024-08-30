package vn.com.gsoft.transaction.service.impl;

import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.service.RedisListService;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RedisListServiceImpl implements RedisListService {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");


    @Override
    // Lấy toàn bộ danh sách giá trị
    public List<Object> getGiaoDichHangHoaValues(GiaoDichHangHoaReq req) {
        var fromDate = Double.parseDouble(new SimpleDateFormat("yyyyMMdd").format(req.getFromDate()));
        var toDate = Double.parseDouble(new SimpleDateFormat("yyyyMMdd").format(req.getToDate()));
        Set<Object> results = redisTemplate.opsForZSet().rangeByScore("transactions", fromDate, toDate);
        return Arrays.asList(results.toArray());
    }

    public void saveTransaction(GiaoDichHangHoa data) {
        String key = "transactions:" + sdf.format(data.getNgayGiaoDich());
        long timestamp = data.getNgayGiaoDich().getTime() / 1000;

        redisTemplate.opsForZSet().add(key, data.getMaPhieuChiTiet() + "_" + data.getLoaiGiaoDich(), timestamp);

        String transactionKey = "transaction:" + data.getMaPhieuChiTiet() + "_" + data.getLoaiGiaoDich();
        redisTemplate.opsForHash().put(transactionKey, "ngay", sdf.format(data.getNgayGiaoDich()));
        redisTemplate.opsForHash().put(transactionKey, "ten", data.getTenThuoc());
        redisTemplate.opsForHash().put(transactionKey, "donvi", data.getTenDonVi());
        redisTemplate.opsForHash().put(transactionKey, "hoatChatId", data.getNhomHoatChatId());
        redisTemplate.opsForHash().put(transactionKey, "duocLyId", data.getNhomDuocLyId());
        redisTemplate.opsForHash().put(transactionKey, "nhomNganhHangId", data.getNhomNganhHangId());
        redisTemplate.opsForHash().put(transactionKey, "tenNhomNganhHang", data.getTenNhomNganhHang());
        redisTemplate.opsForHash().put(transactionKey, "giaBan", data.getGiaBan());
        redisTemplate.opsForHash().put(transactionKey, "giaNhap", data.getGiaNhap());
        redisTemplate.opsForHash().put(transactionKey, "soLuong", data.getSoLuong());
        redisTemplate.opsForHash().put(transactionKey, "maCoSo", data.getMaCoSo());
    }

    public List<Map<Object, Object>> getTransactionsByTimeRangeAcrossDays(Date startDate, Date endDate) {
        List<Map<Object, Object>> transactionDetails = new ArrayList<>();

        long startTimestamp = startDate.getTime() / 1000;
        long endTimestamp = endDate.getTime() / 1000;

        // Lấy tất cả transactionId trong khoảng thời gian từ startDate đến endDate
        Set<Object> transactionIds = redisTemplate.opsForZSet().rangeByScore("transactions", 1724921420, 1724921447);

        if (transactionIds != null) {
            // Lấy chi tiết giao dịch cho từng transactionId
            transactionDetails = transactionIds.stream()
                    .map(transactionId -> redisTemplate.opsForHash().entries("transaction:" + transactionId))
                    .collect(Collectors.toList());
        }

        return transactionDetails;
    }

    public void pushDataRedis(List<GiaoDichHangHoa> giaoDichHangHoas){
        //            var timestamp = Double.parseDouble(new SimpleDateFormat("yyyyMMdd").format(x.getNgayGiaoDich()));
        //            redisTemplate.opsForZSet().add("transactions", x, timestamp);
        giaoDichHangHoas.forEach(this::saveTransaction);
    }
}
