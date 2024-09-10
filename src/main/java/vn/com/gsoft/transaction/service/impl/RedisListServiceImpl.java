package vn.com.gsoft.transaction.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaCache;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaReq;
import vn.com.gsoft.transaction.model.dto.GiaoDichHangHoaRes;
import vn.com.gsoft.transaction.model.dto.TopMatHangRes;
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

    @Override
    public void pushDataRedis(List<GiaoDichHangHoa> giaoDichHangHoas){
        giaoDichHangHoas.forEach(x->{
            var timestamp = Double.parseDouble(new SimpleDateFormat("yyyyMMdd").format(x.getNgayGiaoDich()));
            redisTemplate.opsForZSet().add("transactions", x, timestamp);
        });
    }

    public void pushDataToRedisByTime(List<GiaoDichHangHoaCache> dataList, String key) {
        dataList.forEach(x->{
            try {
                redisTemplate.opsForHash().put("transaction-detail", x.getMaPhieuChiTiet() + "_" + x.getLoaiGiaoDich(),
                        objectMapper.writeValueAsString(x));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
//        try {
//            var ids = dataList.stream().map(x->x.getMaPhieuChiTiet() + "_"+ x.getLoaiGiaoDich()).collect(Collectors.toList());
//            redisTemplate.opsForHash().put("transaction-keys", key,
//                    objectMapper.writeValueAsString(ids));
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
    }

    public void pushDataToRedis(List<TopMatHangRes> dataList, String time, String type) {
        for (int i = 0; i < dataList.size(); i++) {
            String key = time + "-" + type + ":" + i;
            redisTemplate.opsForValue().set(key, dataList.get(i));
        }
    }
    public List<GiaoDichHangHoaCache> getAllDataKey(String key) {
        try {
            if(redisTemplate.opsForHash().hasKey("transaction", key)){
                // Lấy dữ liệu dưới dạng chuỗi JSON từ Redis Hash
                String jsonData = (String) redisTemplate.opsForHash().get("transaction", key);

                // Chuyển đổi JSON thành List<MyData>
                return objectMapper.readValue(jsonData, objectMapper.getTypeFactory().constructCollectionType(List.class, GiaoDichHangHoaCache.class));
            }
            return new ArrayList<>();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<GiaoDichHangHoaCache> getAllDataDetailByKeys(List<String> keys) {
        List<GiaoDichHangHoaCache> hangHoaCaches = new ArrayList<>();
        keys.forEach(x->{
            String jsonData = (String) redisTemplate.opsForHash().get("transaction-detail", x);
            try {
                GiaoDichHangHoaCache data = objectMapper.readValue(jsonData, GiaoDichHangHoaCache.class);
                hangHoaCaches.add(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        return  hangHoaCaches;
    }

    // Hàm lưu giao dịch vào Redis
    public void saveTransaction(String transactionId, long transactionTimestamp, Map<String, GiaoDichHangHoaCache> transactionDetails) {
        // 1. Lưu chi tiết giao dịch vào Hash
        String transactionKey = "transaction:" + transactionId;
        redisTemplate.opsForHash().putAll(transactionKey, transactionDetails);

        // 2. Lưu transactionId vào Sorted Set với timestamp là score (để truy vấn theo ngày)
        redisTemplate.opsForZSet().add("transactionsByDate", transactionId, transactionTimestamp);
    }
}
