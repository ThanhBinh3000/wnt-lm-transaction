package vn.com.gsoft.transaction.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.CachingConstant;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaRes;
import vn.com.gsoft.transaction.service.RedisListService;

import java.math.BigDecimal;
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
    @Autowired
    private ObjectMapper objectMapper;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");


    @Override
    // Lấy toàn bộ danh sách giá trị
    public List<Object> getGiaoDichHangHoaValues(GiaoDichHangHoaReq req) {
        double startTimestamp = req.getFromDate().getTime() / 1000.0;
        double endTimestamp = req.getToDate().getTime() / 1000.0;

        // 2. Lấy các transactionId theo ngày
        Set<Object> transactionIdsByDate = redisTemplate.opsForZSet().rangeByScore("CachingConstant.GIAO_DICH_HANG_HOA_THEO_NGAY", startTimestamp, endTimestamp);
        return transactionIdsByDate.stream()
                .map(id -> redisTemplate.opsForHash().entries(CachingConstant.GIAO_DICH_HANG_HOA + ":" + id))
                .map(data -> objectMapper.convertValue(data, GiaoDichHangHoa.class))
                .collect(Collectors.toList());
    }

    public void saveTransaction(GiaoDichHangHoa data) {
        String key = "transactions:" + sdf.format(data.getNgayGiaoDich());
        double timestamp = data.getNgayGiaoDich().getTime() / 1000.0;

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

//    public List<Map<Object, Object>> getTransactionsByTimeRangeAcrossDays(Date startDate, Date endDate) {
//        List<Map<Object, Object>> transactionDetails = new ArrayList<>();
//
//        var fromDate = Double.parseDouble(new SimpleDateFormat("yyyyMMdd").format(startDate));
//        var toDate = Double.parseDouble(new SimpleDateFormat("yyyyMMdd").format(endDate));
//
//        // Lấy tất cả transactionId trong khoảng thời gian từ startDate đến endDate
//        Set<Object> transactionIds = redisTemplate.opsForZSet().rangeByScore("transactions", fromDate, toDate);
//
//        if (transactionIds != null) {
//            // Lấy chi tiết giao dịch cho từng transactionId
//            transactionDetails = transactionIds.stream()
//                    .map(transactionId -> redisTemplate.opsForHash().entries("transaction:" + transactionId))
//                    .collect(Collectors.toList());
//        }
//
//        return transactionDetails;
//    }

    public List<GiaoDichHangHoaRes> getTransactionsByTimeRangeAcrossDays(Date fromDate, Date toDate) {
        // Chuyển đổi ngày sang timestamp (score)
        double fromTimestamp = fromDate.getTime() / 1000.0;
        double toTimestamp = toDate.getTime() / 1000.0;

        // Lấy key dựa trên ngày (có thể thay đổi key nếu bạn lưu theo ngày khác nhau)
        String keyPattern = "transactions:*";

        // Lấy tất cả các keys phù hợp với key pattern
        Set<String> keys = redisTemplate.keys(keyPattern);

        // Duyệt qua các keys để lấy dữ liệu từ ZSet
        return keys.stream()
                .flatMap(key -> {
                    Set<Object> transactionIds = redisTemplate.opsForZSet()
                            .rangeByScore(key, fromTimestamp, toTimestamp);

                    // Truy vấn chi tiết các giao dịch từ Redis Hash
                    return transactionIds.stream().map(x->getTransactionDetails(x.toString()));
                })
                .collect(Collectors.toList());
    }

    private GiaoDichHangHoaRes getTransactionDetails(String transactionId) {
        String transactionKey = "transaction:" + transactionId;

        // Truy xuất dữ liệu từ Redis Hash
        String ngay = (String) redisTemplate.opsForHash().get(transactionKey, "ngay");
        String ten = (String) redisTemplate.opsForHash().get(transactionKey, "ten");
        String donvi = (String) redisTemplate.opsForHash().get(transactionKey, "donvi");
        String hoatChatId = (String) redisTemplate.opsForHash().get(transactionKey, "hoatChatId");
        String duocLyId = (String) redisTemplate.opsForHash().get(transactionKey, "duocLyId");
        String nhomNganhHangId = (String) redisTemplate.opsForHash().get(transactionKey, "nhomNganhHangId");
        String tenNhomNganhHang = (String) redisTemplate.opsForHash().get(transactionKey, "tenNhomNganhHang");
        String giaBan = (String) redisTemplate.opsForHash().get(transactionKey, "giaBan");
        String giaNhap = (String) redisTemplate.opsForHash().get(transactionKey, "giaNhap");
        String soLuong = (String) redisTemplate.opsForHash().get(transactionKey, "soLuong");
        String maCoSo = (String) redisTemplate.opsForHash().get(transactionKey, "maCoSo");

        // Tạo object GiaoDichHangHoa từ dữ liệu
        return new GiaoDichHangHoaRes(ngay, ten, donvi, hoatChatId, duocLyId, nhomNganhHangId, tenNhomNganhHang, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, maCoSo);
    }

    public void pushDataRedis(List<GiaoDichHangHoa> giaoDichHangHoas){
        //            var timestamp = Double.parseDouble(new SimpleDateFormat("yyyyMMdd").format(x.getNgayGiaoDich()));
        //            redisTemplate.opsForZSet().add("transactions", x, timestamp);
        giaoDichHangHoas.forEach(this::saveTransaction);
    }
}
