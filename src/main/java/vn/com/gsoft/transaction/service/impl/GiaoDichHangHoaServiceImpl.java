package vn.com.gsoft.transaction.service.impl;


import com.ctc.wstx.util.DataUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.persistence.criteria.From;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.BaoCaoContains;
import vn.com.gsoft.transaction.constant.LimitPageConstant;
import vn.com.gsoft.transaction.entity.*;
import vn.com.gsoft.transaction.model.dto.*;
import vn.com.gsoft.transaction.model.system.Profile;
import vn.com.gsoft.transaction.repository.*;
import vn.com.gsoft.transaction.service.GiaoDichHangHoaService;
import vn.com.gsoft.transaction.service.RedisListService;
import vn.com.gsoft.transaction.util.system.DataUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Log4j2
public class GiaoDichHangHoaServiceImpl extends BaseServiceImpl<GiaoDichHangHoa, GiaoDichHangHoaReq, Long> implements GiaoDichHangHoaService {


    private GiaoDichHangHoaRepository hdrRepo;
    @Autowired
    private RedisListService redisListService;
    @Autowired
    private HangHoaRepository hangHoaRepo;

    @Autowired
    public GiaoDichHangHoaServiceImpl(GiaoDichHangHoaRepository hdrRepo,
                                      RedisListService redisListService,
                                      HangHoaRepository hangHoaRepo
                                ) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
        this.redisListService = redisListService;
        this.hangHoaRepo = hangHoaRepo;
    }
    @Override
    public List<HangHoaDaTinhToanCache> topDoanhThuBanChay(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);

        var items = getDataRedis(req, BaoCaoContains.DOANH_THU);
        if(items == null || items.isEmpty()) return new ArrayList<>();
        if(req.getNhomNganhHangId() != null && req.getNhomNganhHangId() > 0){
            items = items.stream().filter(item->item.getNhomNganhHangId().equals(req.getNhomNganhHangId()))
                    .collect(Collectors.toList());
        }
        if(req.getNhomDuocLyId() != null && req.getNhomDuocLyId() > 0){
            items = items.stream().filter(item->item.getNhomDuocLyId().equals(req.getNhomDuocLyId()))
                    .collect(Collectors.toList());
        }
        if(req.getNhomHoatChatId() != null && req.getNhomHoatChatId() > 0){
            items = items.stream().filter(item->item.getNhomHoatChatId().equals(req.getNhomHoatChatId()))
                    .collect(Collectors.toList());
        }
        items = items.stream().limit(req.getPageSize()).toList();
        //lấy ra doanh so cs
        if(userInfo.getMaCoSo() != null && userInfo.getAuthorities().stream().filter(x->x.getAuthority() =="DLGDHH") != null){
            List<Long> ids = items.stream().map(x->x.getThuocId()).toList();
            req.setThuocIds(ids.toArray(new Long[ids.size()]));
            var dataCS = DataUtils.convertList(hdrRepo.groupByTopDoanhThuCS(req), DoanhThuCS.class);
            var groupBy = dataCS.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
            items.forEach(x->{
                if(groupBy.containsKey(x.getThuocId())){
                    var value = groupBy.get(x.getThuocId());
                    x.setSoLieuCoSo(value.get(0).getBan());
                }
            });
        }
        return  items;
    }

    @Override
    public List<HangHoaDaTinhToanCache> topSLBanChay(GiaoDichHangHoaReq req) throws Exception{
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);
        List<GiaoDichHangHoaCache> dataArchive = new ArrayList<>();
        //kiểm tra xem thời gian xem báo cáo có lớn hơn thời điểm archive không;
        var toDate = req.getToDate();
        if(req.getFromDate().before(dateArchive.getTime())){
            req.setToDate(dateArchive.getTime());
            var listArchive = hdrRepo.searchListCache(req);
            if(!listArchive.stream().isParallel()){
                dataArchive.addAll(DataUtils.convertList(listArchive, GiaoDichHangHoaCache.class));
            }
            req.setFromDate(dateArchive.getTime());
            req.setToDate(toDate);
        }

        var items = getDataRedis(req, BaoCaoContains.SO_LUONG);
        if(items == null || items.isEmpty()) return new ArrayList<>();

        items = items.stream().limit(req.getPageSize()).toList();
        //lấy ra doanh so cs
        if(userInfo.getMaCoSo() != null && userInfo.getAuthorities().stream().filter(x->x.getAuthority() =="DLGDHH") != null){
            List<Long> ids = items.stream().map(x->x.getThuocId()).toList();
            req.setThuocIds(ids.toArray(new Long[ids.size()]));
            var dataCS = DataUtils.convertList(hdrRepo.groupByTopSLCS(req), DoanhThuCS.class);
            var groupBy = dataCS.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
            items.forEach(x->{
                if(groupBy.containsKey(x.getThuocId())){
                    var value = groupBy.get(x.getThuocId());
                    x.setSoLieuCoSo(value.get(0).getSoLuong());
                }
            });
        }
        return  items;
    }

    @Override
    public List<HangHoaDaTinhToanCache> topTSLNCaoNhat(GiaoDichHangHoaReq req) throws Exception {
        Profile userInfo = this.getLoggedUser();
        if (userInfo == null)
            throw new Exception("Bad request.");
        setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);
        List<GiaoDichHangHoaCache> dataArchive = new ArrayList<>();
        //kiểm tra xem thời gian xem báo cáo có lớn hơn thời điểm archive không;
        var toDate = req.getToDate();
        if(req.getFromDate().before(dateArchive.getTime())){
            req.setToDate(dateArchive.getTime());
            var listArchive = hdrRepo.searchListCache(req);
            if(!listArchive.stream().isParallel()){
                dataArchive.addAll(DataUtils.convertList(listArchive, GiaoDichHangHoaCache.class));
            }
            req.setFromDate(dateArchive.getTime());
            req.setToDate(toDate);
        }
        var items = getDataRedis(req, BaoCaoContains.TSLN);
        if(items == null || items.isEmpty()) return new ArrayList<>();
        if(req.getNhomNganhHangId() != null && req.getNhomNganhHangId() > 0){
            items = items.stream().filter(item->item.getNhomNganhHangId().equals(req.getNhomNganhHangId()))
                    .collect(Collectors.toList());
        }
        if(req.getNhomDuocLyId() != null && req.getNhomDuocLyId() > 0){
            items = items.stream().filter(item->item.getNhomDuocLyId().equals(req.getNhomDuocLyId()))
                    .collect(Collectors.toList());
        }
        if(req.getNhomHoatChatId() != null && req.getNhomHoatChatId() > 0){
            items = items.stream().filter(item->item.getNhomHoatChatId().equals(req.getNhomHoatChatId()))
                    .collect(Collectors.toList());
        }

        return  items.stream().limit(req.getPageSize()).toList();
    }

    private void setDefaultDates(GiaoDichHangHoaReq req) {
        // Tạo đối tượng Calendar để tính toán ngày mặc định
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.YEAR, -5); // Trừ 5 năm so với năm hiện tại
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        // Cài đặt ngày mặc định cho req nếu req.getFromDate() hoặc req.getToDate() là null
        if (req.getFromDate() == null) {
            req.setFromDate(calendar.getTime());
        }
        if (req.getToDate() == null) {
            req.setToDate(new Date());
        }
    }

    @Override
    public void pushData() throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        LocalDate today = LocalDate.now();
        LocalDate oneYearAgo = today.minusYears(1);

        LocalDate date = oneYearAgo;
        List<String> keys = new ArrayList<>();
        List<GiaoDichHangHoaCache> allItems = new ArrayList<>();
        while (!date.isAfter(today)) {
            LocalDateTime startOfDayTime = date.atStartOfDay();
            LocalDateTime endOfDayTime = date.atTime(23, 59, 59);

            Date startOfDay = Date.from(startOfDayTime.atZone(ZoneId.systemDefault()).toInstant());
            Date endOfDay = Date.from(endOfDayTime.atZone(ZoneId.systemDefault()).toInstant());

            var req = new GiaoDichHangHoaReq();
            req.setFromDate(startOfDay);
            req.setToDate(endOfDay);
           Date todayWithZeroTime = formatter.parse(formatter.format(req.getToDate()));
           String pattern = "dd/MM/yyyy";
            DateFormat df = new SimpleDateFormat(pattern);
            var items = DataUtils.convertList(hdrRepo.searchListCache(req), GiaoDichHangHoaCache.class);
           if (items.stream().count() > 0) {
               var groupBy = items.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
               groupBy.keySet().forEach(x->{
                   var key = df.format(todayWithZeroTime);
                   var values = groupBy.get(x);
                   HangHoaDaTinhToanCache hh = new HangHoaDaTinhToanCache();
                   hh.setThuocId(x);
                   hh.setTenThuoc(values.get(0).getTenThuoc());
                   hh.setTenNhomNganhHang(values.get(0).getTenNhomNganhHang());
                   hh.setTenDonVi(values.get(0).getTenDonVi());
                   hh.setNhomDuocLyId(values.get(0).getNhomDuocLyId());
                   hh.setNhomNganhHangId(values.get(0).getNhomNganhHangId());
                   hh.setNhomHoatChatId(values.get(0).getNhomHoatChatId());
                   hh.setTongBan(values.stream().filter(xx->xx.getLoaiGiaoDich() == 2)
                           .map(xx->xx.getGiaBan().multiply(xx.getSoLuong()))
                           .reduce(BigDecimal.ZERO, BigDecimal::add));
                   hh.setTongNhap(values.stream().filter(xx->xx.getLoaiGiaoDich() == 1)
                           .map(xx->xx.getGiaNhap().multiply(xx.getSoLuong()))
                           .reduce(BigDecimal.ZERO, BigDecimal::add));
                   hh.setSoLuong(values.stream().filter(xx->xx.getLoaiGiaoDich() == 2)
                           .map(xx->xx.getSoLuong())
                           .reduce(BigDecimal.ZERO, BigDecimal::add));
                   redisListService.pushDataToRedisByTime(hh, key);
               });
            }
            date = date.plusDays(1);
        }
    }

    public void pushDataThreeLastMonth() throws Exception {
        var req = new GiaoDichHangHoaReq();
        Calendar fdate = Calendar.getInstance();
        fdate.add(Calendar.MONTH, -3);
        req.setLoaiGiaoDich(2);
        req.setFromDate(fdate.getTime());
        req.setDongBang(false);
        var list = DataUtils.convertList(hdrRepo.groupByTopDoanhSoLuongPushRedis(req, 2000), TopMatHangRes.class);
        //redisListService.pushDataToRedis(list, "top-sl", "3-thang-gan-nhat");
    }

    private List<HangHoaDaTinhToanCache> getDataRedis(GiaoDichHangHoaReq req, int type) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        LocalDate toDate = convertToLocalDate(req.getToDate());
        LocalDate fromDate = convertToLocalDate(req.getFromDate());
        List<String> keys = new ArrayList<>();

        while (!fromDate.isAfter(toDate)) {
            String pattern = "dd/MM/yyyy";
            var datekey =  Date.from(fromDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
            Date todayWithZeroTime = formatter.parse(formatter.format(datekey));
            DateFormat df = new SimpleDateFormat(pattern);
            keys.add(df.format(todayWithZeroTime));
            fromDate = fromDate.plusDays(1);
        }

        return redisListService.getAllDataDetailByKeys(keys, type);
    }

    private LocalDate convertToLocalDate(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }
}
