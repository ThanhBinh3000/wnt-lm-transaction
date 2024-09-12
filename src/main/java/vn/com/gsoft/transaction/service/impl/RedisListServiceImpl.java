package vn.com.gsoft.transaction.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.BaoCaoContains;
import vn.com.gsoft.transaction.constant.CachingConstant;
import vn.com.gsoft.transaction.entity.GiaoDichHangHoa;
import vn.com.gsoft.transaction.model.dto.*;
import vn.com.gsoft.transaction.service.RedisListService;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
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

    public void pushDataToRedis3T(List<TopMatHangRes> dataList, String time, String type) {
        for (int i = 0; i < dataList.size(); i++) {
            String key = time + "-" + type + ":" + i;
            redisTemplate.opsForValue().set(key, dataList.get(i));
        }
    }

    public void pushDataToRedisByTime(HangHoaDaTinhToanCache data, String key) {
        redisTemplate.opsForHash().put("transaction-12-month:transaction-" + key, data.getThuocId(),
                data);
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

    public List<HangHoaDaTinhToanCache> getAllDataDetailByKeys(List<String> keys, Integer type) {
       List<HangHoaDaTinhToanCache> data = new ArrayList<>();
       Map<Object, Object> dataKeyFirst = new HashMap<>();
        for(var i = 0; i< keys.size(); i++){
            var dataByKey = getAllDataFromRedisByKey(keys.get(i));
            if(dataByKey != null){
                if(i == 0){
                    dataKeyFirst = dataByKey;
                }
                if(i > 0 && dataKeyFirst != null) {
                    for (Map.Entry<Object, Object> entry : dataByKey.entrySet()) {
                        Object key = entry.getKey();
                        var itemFromMapData1 = (HangHoaDaTinhToanCache) entry.getValue();
                        if (dataKeyFirst.containsKey(key)) {
                            var itemFromMapData2 = (HangHoaDaTinhToanCache)dataKeyFirst.get(key);
                            if(type == BaoCaoContains.SO_LUONG){
                                itemFromMapData2.setSoLieuThiTruong(itemFromMapData2.getSoLuong().add(itemFromMapData1.getSoLuong()));
                            }
                            if(type == BaoCaoContains.DOANH_THU){
                                itemFromMapData2.setSoLieuThiTruong(itemFromMapData2.getTongBan().add(itemFromMapData1.getTongBan()));
                            }
                            if(type == BaoCaoContains.TSLN){
                            }
                        } else {
                            if(type == BaoCaoContains.SO_LUONG){
                                itemFromMapData1.setSoLieuThiTruong(itemFromMapData1.getSoLuong());
                            }
                            if(type == BaoCaoContains.DOANH_THU){
                                itemFromMapData1.setSoLieuThiTruong(itemFromMapData1.getTongBan());
                            }
                            if(type == BaoCaoContains.TSLN){
                                itemFromMapData1.setSoLieuThiTruong(BigDecimal.ZERO);
                            }
                            dataKeyFirst.put(key, itemFromMapData1);
                        }
                    }
                }
            }
        }
        if(dataKeyFirst != null){
            data = dataKeyFirst.values().stream()
                    .filter(obj -> obj instanceof HangHoaDaTinhToanCache && ((HangHoaDaTinhToanCache) obj).getSoLieuThiTruong() != null)
                    .map(obj -> (HangHoaDaTinhToanCache) obj)
                    .collect(Collectors.toList());
            data.sort(Comparator.comparing(HangHoaDaTinhToanCache::getSoLieuThiTruong).reversed());
            return data;
        }
        return null;
    }

    //tính toán top dữ liệu
    public Map<Object, Object> getAllDataFromRedisByKey(String key) {
        Map<Object, Object> allData = redisTemplate.opsForHash().entries("transaction-" + key);
        if (!allData.isEmpty()) {
            return allData;
        } else {
            return Collections.emptyMap();
        }
    }
}
