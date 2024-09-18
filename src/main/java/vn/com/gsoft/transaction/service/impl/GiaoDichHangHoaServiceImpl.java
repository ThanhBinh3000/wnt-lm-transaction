package vn.com.gsoft.transaction.service.impl;


import com.ctc.wstx.util.DataUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.persistence.Tuple;
import jakarta.persistence.criteria.From;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import vn.com.gsoft.transaction.constant.BaoCaoContains;
import vn.com.gsoft.transaction.constant.LimitPageConstant;
import vn.com.gsoft.transaction.constant.LoaiTableContains;
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
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
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
        //setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        if(req.getFromDate() != null && req.getToDate() != null){
            LocalDate fDate = req.getFromDate().toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            LocalDate tDate = req.getToDate().toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            var year = ChronoUnit.YEARS.between(fDate, tDate);
            if(year == 0){
                if(fDate.getYear() == tDate.getYear()){
                    items = chuaQua12ThangCungNam(req, fDate, tDate, BaoCaoContains.DOANH_THU);
                }else {
                    items = chuaQua12ThangKhacNam(req, fDate, tDate, BaoCaoContains.DOANH_THU);
                }
            }else {
                items = tren1Nam(req, fDate, tDate, BaoCaoContains.DOANH_THU);
            }
        }else {
            req.setType(0);
            items = searchTop_T0(0, req, req.getPageSize(), BaoCaoContains.DOANH_THU);
        }

        //lấy ra doanh so cs
        if(userInfo.getMaCoSo() != null
                && userInfo.getAuthorities().stream().filter(x->x.getAuthority() =="DLGDHH") != null
                && items != null){
            List<Long> ids = items.stream().map(x->x.getThuocId()).toList();
            req.setThuocIds(ids.toArray(new Long[ids.size()]));
            var dataCS = DataUtils.convertList(hdrRepo.groupByTopDoanhThuCS(req), DoanhThuCS.class);
            var groupBy = dataCS.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
            if(groupBy.size() > 0){
                items.forEach(x->{
                    if(groupBy.containsKey(x.getThuocId())){
                        var value = groupBy.get(x.getThuocId());
                        x.setSoLieuCoSo(value.get(0).getBan());
                    }
                });
            }
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
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();

        if(req.getFromDate() != null && req.getToDate() != null){
            LocalDate fDate = req.getFromDate().toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            LocalDate tDate = req.getToDate().toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            var year = ChronoUnit.YEARS.between(fDate, tDate);
            if(year == 0){
                if(fDate.getYear() == tDate.getYear()){
                    items = chuaQua12ThangCungNam(req, fDate, tDate, BaoCaoContains.SO_LUONG);
                }else {
                    items = chuaQua12ThangKhacNam(req, fDate, tDate, BaoCaoContains.SO_LUONG);
                }
            }else {
                items = tren1Nam(req, fDate, tDate, BaoCaoContains.SO_LUONG);
            }
        }else {
            req.setType(0);
            items = searchTop_T0(0, req, req.getPageSize(), BaoCaoContains.SO_LUONG);
        }

        if(userInfo.getMaCoSo() != null
                && userInfo.getAuthorities().stream().filter(x->x.getAuthority() =="DLGDHH") != null
                && items != null){
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
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();

        if(req.getFromDate() != null && req.getToDate() != null){
            LocalDate fDate = req.getFromDate().toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            LocalDate tDate = req.getToDate().toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();
            var year = ChronoUnit.YEARS.between(fDate, tDate);
            if(year == 0){
                if(fDate.getYear() == tDate.getYear()){
                    items = chuaQua12ThangCungNam(req, fDate, tDate, BaoCaoContains.TSLN);
                }else {
                    items = chuaQua12ThangKhacNam(req, fDate, tDate, BaoCaoContains.TSLN);
                }
            }else {
                items = tren1Nam(req, fDate, tDate, BaoCaoContains.TSLN);
            }
        }else {
            req.setType(0);
            items = searchTop_T0(0, req, req.getPageSize(), BaoCaoContains.TSLN);
        }

        //lấy ra doanh so cs
        if(userInfo.getMaCoSo() != null
                && userInfo.getAuthorities().stream().filter(x->x.getAuthority() =="DLGDHH") != null
                && items != null){
            List<Long> ids = items.stream().map(x->x.getThuocId()).toList();
            req.setThuocIds(ids.toArray(new Long[ids.size()]));
            var dataCS = DataUtils.convertList(hdrRepo.groupByTopTSLNCS(req), DoanhThuCS.class);
            var groupBy = dataCS.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
            items.forEach(x->{
                if(groupBy.containsKey(x.getThuocId())){
                    var value = groupBy.get(x.getThuocId());
                    x.setSoLieuCoSo(value.get(0).getSoLuong());
                }
            });
        }
        return items;
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

    @Override
    public void pushDataThreeLastMonth() throws Exception {
        var req = new GiaoDichHangHoaReq();
        Calendar fdate = Calendar.getInstance();
        fdate.add(Calendar.MONTH, -3);
        req.setLoaiGiaoDich(2);
        req.setFromDate(fdate.getTime());
        req.setDongBang(false);
        var list = DataUtils.convertList(hdrRepo.groupByTopDoanhSoLuongPushRedis(req, 2000), TopMatHangRes.class);
        redisListService.pushDataToRedis3T(list, "top-sl", "3-thang-gan-nhat");
    }

    @Override

    public void pushDataByMonth() throws Exception {

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

    private List<HangHoaDaTinhToanCache> chuaQua12ThangCungNam(GiaoDichHangHoaReq req,
                                                               LocalDate fDate, LocalDate tDate,
                                                               int type) throws Exception {
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        var months = ChronoUnit.MONTHS.between(fDate, tDate);
        LocalDate startfDate = fDate.with(TemporalAdjusters.firstDayOfMonth());
        LocalDate endtDate = tDate.with(TemporalAdjusters.lastDayOfMonth());
        var isStart = startfDate.getDayOfMonth() == fDate.getDayOfMonth();
        var isEnd = endtDate.getDayOfMonth() == tDate.getDayOfMonth();
        if(months >= 1 && months < 11 ){
            if(isStart && isEnd){
                var arrMonth = new ArrayList<Integer>();
                for (var  i = 0 ; i < months + 1; i++){
                    arrMonth.add(fDate.getMonthValue() + i);
                }
                req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
                items = groupByTop_T0(fDate.getYear(), req, req.getPageSize(), type);
            }else {
                items = cacThangKhacNhau(req, fDate, tDate, type, isStart, isEnd, months);
                items = items.stream().limit(req.getPageSize()).toList();
            }

        }
        else if((months == 0 || months == 11) && isStart && isEnd
                && fDate.getYear() == tDate.getYear()){
            req.setType( months == 0 ? fDate.getMonthValue() : 0);
            items = searchTop_T0(endtDate.getYear(), req, req.getPageSize(), type);
        }
        else if(months == 0 && fDate.getMonthValue() != tDate.getMonthValue()){
            items = chuaQua1ThangO2ThangKhacNhau(
                    req, fDate, tDate, type, req.getPageSize());
        }
        else {
            items = groupByTop_T_ANY(fDate.getYear(),
                    fDate.getMonthValue() ,
                    req, fDate, tDate, req.getPageSize(), type);
        }
        return items;
    }

    private List<HangHoaDaTinhToanCache> chuaQua12ThangKhacNam(GiaoDichHangHoaReq req,
                                                               LocalDate fDate, LocalDate tDate, int type) throws Exception {
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        LocalDate startfDate = fDate.with(TemporalAdjusters.firstDayOfMonth());
        LocalDate endtDate = tDate.with(TemporalAdjusters.lastDayOfMonth());
        var isStart = startfDate.getDayOfMonth() == fDate.getDayOfMonth();
        var isEnd = endtDate.getDayOfMonth() == tDate.getDayOfMonth();
        if(isStart && isEnd){
            var fMonths = new ArrayList<Integer>();
            for (var i = 0; i <= 12 - fDate.getMonthValue(); i ++){
                fMonths.add(fDate.getMonthValue() + i);
            }

            var giaoDichReq1 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, giaoDichReq1);
            giaoDichReq1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            giaoDichReq1.setTypes(fMonths.toArray(new Integer[fMonths.size()]));
            var tMonths = new ArrayList<Integer>();
            for (var i = 0; i < tDate.getMonthValue(); i ++){
                tMonths.add(tDate.getMonthValue() - i);
            }

            var giaoDichReq2 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, giaoDichReq2);
            giaoDichReq2.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            giaoDichReq2.setTypes(tMonths.toArray(new Integer[tMonths.size()]));

            items = groupByTop_T0(fDate.getYear(), giaoDichReq1, giaoDichReq1.getPageSize(), type);
            List<HangHoaDaTinhToanCache> lastItems = groupByTop_T0(tDate.getYear(), giaoDichReq2, giaoDichReq2.getPageSize(), type);

            if(!lastItems.isEmpty() && items != null){
                var groupItems = lastItems.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
                for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                    boolean found = false;
                    for (HangHoaDaTinhToanCache hh : items) {
                        if (hh.getThuocId().equals(entry.getKey())) {
                            if(type == BaoCaoContains.TSLN){
                                if(hh.getSoLieuThiTruong().compareTo(entry.getValue().get(0).getSoLieuThiTruong()) < 0){
                                    hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
                                }
                            }else {
                                var sum = hh.getSoLieuThiTruong().add(entry.getValue().get(0).getSoLieuThiTruong());
                                hh.setSoLieuThiTruong(sum);
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        items.add(entry.getValue().get(0));
                    }
                }
            }else {
                items = items != null ? items : lastItems;
            }
            if(items != null){
                items.sort((hh1, hh2) -> hh2.getSoLieuThiTruong().compareTo(hh1.getSoLieuThiTruong()));
            }
            items = items.stream().limit(req.getPageSize()).toList();
        }else {
            var req1 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, req1);
            req1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            LocalDate tDate1 = fDate.with(TemporalAdjusters.lastDayOfYear());
            var month1s = ChronoUnit.MONTHS.between(fDate, tDate1);
            items = cacThangKhacNhau(req, fDate, tDate1, type, isStart, isEnd, month1s);

            var req2 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, req2);
            req1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            LocalDate fDate2 = tDate.with(TemporalAdjusters.firstDayOfYear());
            var month2s = ChronoUnit.MONTHS.between(fDate2, tDate);
            var itemExtra = cacThangKhacNhau(req, fDate2, tDate, type, isStart, isEnd, month2s);

            if(!itemExtra.isEmpty() && items != null){
                var groupItems = itemExtra.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
                for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                    boolean found = false;
                    for (HangHoaDaTinhToanCache hh : items) {
                        if (hh.getThuocId().equals(entry.getKey())) {
                            if(type == BaoCaoContains.TSLN){
                                if(hh.getSoLieuThiTruong().compareTo(entry.getValue().get(0).getSoLieuThiTruong()) < 0){
                                    hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
                                }
                            }else {
                                var sum = hh.getSoLieuThiTruong().add(entry.getValue().get(0).getSoLieuThiTruong());
                                hh.setSoLieuThiTruong(sum);
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        items.add(entry.getValue().get(0));
                    }
                }
            }else {
                items = items != null ? items : itemExtra;
            }
            if(items != null){
                items.sort((hh1, hh2) -> hh2.getSoLieuThiTruong().compareTo(hh1.getSoLieuThiTruong()));
            }
            items = items.stream().limit(req.getPageSize()).toList();
        }
        return items;
    }

    private List<HangHoaDaTinhToanCache> chuaQua1ThangO2ThangKhacNhau(GiaoDichHangHoaReq req,
                                                                      LocalDate fDate, LocalDate tDate, int type
    , int top) throws Exception {
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        List<HangHoaDaTinhToanCache> itemLast = new ArrayList<>();
        var pageSize = req.getPageSize();
        req.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
        switch (type){
            case BaoCaoContains.DOANH_THU -> {
                items =groupByTop_T_ANY(fDate.getYear(),
                        fDate.getMonthValue(), req, fDate, tDate, top, BaoCaoContains.DOANH_THU);
                itemLast = groupByTop_T_ANY(tDate.getYear(),
                        tDate.getMonthValue(), req, fDate, tDate, top, BaoCaoContains.DOANH_THU);
                break;
            }
            case BaoCaoContains.SO_LUONG -> {
                items =groupByTop_T_ANY(fDate.getYear(),
                        fDate.getMonthValue(), req, fDate, tDate, top, BaoCaoContains.SO_LUONG);
                itemLast = groupByTop_T_ANY(tDate.getYear(),
                        tDate.getMonthValue(), req, fDate, tDate, top, BaoCaoContains.SO_LUONG);
                break;
            }
            case BaoCaoContains.TSLN -> {
                items =groupByTop_T_ANY(fDate.getYear(),
                        fDate.getMonthValue(), req, fDate, tDate, top, BaoCaoContains.TSLN);
                itemLast = groupByTop_T_ANY(tDate.getYear(),
                        tDate.getMonthValue(), req, fDate, tDate, top, BaoCaoContains.TSLN);
                break;
            }
        }
        if(itemLast != null){
            var groupItems = itemLast.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
            for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                boolean found = false;
                for (HangHoaDaTinhToanCache hh : items) {
                    if (hh.getThuocId().equals(entry.getKey())) {
                        if(type == BaoCaoContains.TSLN){
                            if(hh.getSoLieuThiTruong().compareTo(entry.getValue().get(0).getSoLieuThiTruong()) < 0){
                                hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
                            }
                        }else {
                            var sum = hh.getSoLieuThiTruong().add(entry.getValue().get(0).getSoLieuThiTruong());
                            hh.setSoLieuThiTruong(sum);
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    items.add(entry.getValue().get(0));
                }
            }
        }

        if(items != null){
            items.sort((hh1, hh2) -> hh2.getSoLieuThiTruong().compareTo(hh1.getSoLieuThiTruong()));
        }
        return items.stream().limit(pageSize).toList();
    }
    private List<HangHoaDaTinhToanCache> cacThangKhacNhau(GiaoDichHangHoaReq req,
                                                          LocalDate fDate, LocalDate tDate,
                                                          int type, boolean isStart, boolean isEnd, long months){
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        var arrMonth = new ArrayList<Integer>();

        var startMonth = fDate.getMonthValue();
        var endMonth = tDate.getMonthValue();
        for (int m = startMonth + 1; m <= endMonth - 1; m++) {
            arrMonth.add(m);
        }
        if (isStart) {
            arrMonth.add(0, startMonth);
        }
        if (isEnd) {
            arrMonth.add(endMonth);
        }

        req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
        if(arrMonth.size() > 0){
            items = groupByTop_T0(fDate.getYear(), req, req.getPageSize() + 100, type);
        }
        //tinh khoang thua cua thang con lai
        List<HangHoaDaTinhToanCache> itemExtra  = new ArrayList<>();
        if(!isStart){
            var giaoDichReq1 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, giaoDichReq1);
            giaoDichReq1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            YearMonth yearMonth = YearMonth.from(fDate);
            LocalDate tDate1 = yearMonth.atEndOfMonth();
            var item1 = groupByTop_T_ANY(fDate.getYear(), fDate.getMonthValue(),
                    giaoDichReq1, fDate, tDate1, giaoDichReq1.getPageSize(), type);
            if (item1 != null){
                itemExtra.addAll(item1);
            }
        }
        if(!isEnd){
            var giaoDichReq2 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, giaoDichReq2);
            giaoDichReq2.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            LocalDate fDate1 = tDate.withDayOfMonth(1);
            var item2 = groupByTop_T_ANY(fDate.getYear(), fDate.getMonthValue(),
                    giaoDichReq2, fDate1, tDate, giaoDichReq2.getPageSize(), type);
            if (item2 != null){
                itemExtra.addAll(item2);
            }
        }
        if(!itemExtra.isEmpty()){
            var groupItems = itemExtra.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
            for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                boolean found = false;
                for (HangHoaDaTinhToanCache hh : items) {
                    if (hh.getThuocId().equals(entry.getKey())) {
                        if(type == BaoCaoContains.TSLN){
                            if(hh.getSoLieuThiTruong().compareTo(entry.getValue().get(0).getSoLieuThiTruong()) < 0){
                                hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
                            }
                        }else {
                            var sum = hh.getSoLieuThiTruong().add(entry.getValue().get(0).getSoLieuThiTruong());
                            hh.setSoLieuThiTruong(sum);
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    items.add(entry.getValue().get(0));
                }
            }
        }
        if(items != null){
            items.sort((hh1, hh2) -> hh2.getSoLieuThiTruong().compareTo(hh1.getSoLieuThiTruong()));
        }
        return items;
    }
    private List<HangHoaDaTinhToanCache> tren1Nam(GiaoDichHangHoaReq req, LocalDate fDate,
                                                  LocalDate tDate, int type) throws Exception{
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        List<Integer> years = new ArrayList<>();

        int startYear = fDate.getYear();
        int endYear = tDate.getYear();
        LocalDate lastDayOfYear = fDate.with(TemporalAdjusters.lastDayOfYear());
        LocalDate firstDayOfYear = tDate.with(TemporalAdjusters.firstDayOfYear());

        var isStart = fDate == lastDayOfYear;
        var isEnd = tDate == lastDayOfYear;

        for (int year = startYear + 1; year <= endYear - 1; year++) {
            years.add(year);
        }
        if (isStart) {
            years.add(0, startYear);
        }
        if (isEnd) {
            years.add(endYear);
        }
        if(years.size() > 0 && isStart && isEnd){
            req.setTypes(years.toArray(new Integer[years.size()]));
            items = groupByTop_T0(0, req, req.getPageSize(), type);
        }else {
            items = groupByTop_T0(0, req, req.getPageSize() + 100, type);
            List<HangHoaDaTinhToanCache> itemsExtra = new ArrayList<>();

            GiaoDichHangHoaReq req1 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, req1);
            req1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            LocalDate tDate1 = fDate.with(TemporalAdjusters.lastDayOfYear());
            var item1 = chuaQua12ThangCungNam(req1, fDate, tDate1, type);
            if(item1 != null){
                itemsExtra.addAll(item1);
            }

            GiaoDichHangHoaReq req2 = new GiaoDichHangHoaReq();
            BeanUtils.copyProperties(req, req2);
            req2.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
            LocalDate fDate2 = tDate.with(TemporalAdjusters.firstDayOfYear());
            var item2 = chuaQua12ThangCungNam(req1, fDate2, tDate, type);

            if (item2 != null){
                itemsExtra.addAll(item2);
            }

            if(!itemsExtra.isEmpty() && items != null){
                var groupItems = itemsExtra.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
                for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                    boolean found = false;
                    for (HangHoaDaTinhToanCache hh : items) {
                        if (hh.getThuocId().equals(entry.getKey())) {
                            if(type == BaoCaoContains.TSLN){
                                if(hh.getSoLieuThiTruong().compareTo(entry.getValue().get(0).getSoLieuThiTruong()) < 0){
                                    hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
                                }
                            }else {
                                var sum = hh.getSoLieuThiTruong().add(entry.getValue().get(0).getSoLieuThiTruong());
                                hh.setSoLieuThiTruong(sum);
                            }

                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        items.add(entry.getValue().get(0));
                    }
                }
            }
            if(items != null){
                items.sort((hh1, hh2) -> hh2.getSoLieuThiTruong().compareTo(hh1.getSoLieuThiTruong()));
            }
            items = items.stream().limit(req.getPageSize()).toList();
        }

        return items;
    }
    //region GROUP BY T0
    private List<HangHoaDaTinhToanCache> groupByTop_T0(int year, GiaoDichHangHoaReq req, int top, int type){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        Optional<Tuple> table = hdrRepo.checkTableExit(entityName);
        if(table.isEmpty()) return new ArrayList<>();
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        String query = null;
        switch (type){
            case BaoCaoContains.DOANH_THU ->{
                query = "SELECT TOP(" + top + ") " +
                        "s.tenNhomNganhHang, s.ThuocId, " +
                        "s.tenThuoc, s.tenDonVi" +
                        ", s.Tong as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " +
                        "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
                        ", SUM(c.tongBan) as Tong FROM "+ entityName +" c" +
                        " WHERE 1=1 " +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND (c.type in (" + StringUtils.join(req.getTypes(), ',') + ")) " +
                        " AND c.tongBan > 0" +
                        " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                        " ORDER BY s.Tong desc";
                items = DataUtils.convertList(hdrRepo.groupByTopDT_T0(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.SO_LUONG ->{
                query = "SELECT TOP(" + top + ") " +
                        "s.tenNhomNganhHang, s.ThuocId, " +
                        "s.tenThuoc, s.tenDonVi" +
                        ", s.Tong as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " +
                        "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
                        ", SUM(c.TongSoLuong) as Tong FROM "+ entityName + " c" +
                        " WHERE 1=1 " +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND (c.type in (" + StringUtils.join(req.getTypes(), ',') + ")) " +
                        " AND c.TongSoLuong > 0" +
                        " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                        " ORDER BY s.Tong desc";
                items = DataUtils.convertList(hdrRepo.groupByTopSL_T0(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.TSLN ->{
                query = "SELECT TOP(" + top + ") " +
                        "s.tenNhomNganhHang, s.ThuocId, " +
                        "s.tenThuoc, s.tenDonVi, " +
                        "((s.gb-s.gn) /s.gn) * 100  as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " +
                        "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi"  +
                        ",avg(c.giabancs) as gb, avg(c.gianhapcs) as gn " +
                        " FROM " + entityName + " c" +
                        " WHERE 1=1 " +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND (c.type in (" + StringUtils.join(req.getTypes(), ',') + ")) " +
                        " AND c.giabancs > 0" +
                        " AND c.gianhapcs > 0" +
                        " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                        " ORDER BY ((s.gb-s.gn) /s.gn) * 100 desc";
                items = DataUtils.convertList(hdrRepo.groupByTopTSLN_T0(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
        }
        return  items;
    }
    //endregion
    //region Search Top T0
    private List<HangHoaDaTinhToanCache> searchTop_T0(int year, GiaoDichHangHoaReq req, int top, int type){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        Optional<Tuple> table = hdrRepo.checkTableExit(entityName);
        if(table.isEmpty()) return null;
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        String query = null;
        switch (type){
            case BaoCaoContains.DOANH_THU -> {
                query = "SELECT TOP(" + top + ") " +
                        "c.tenNhomNganhHang, c.ThuocId, " +
                        "c.tenThuoc, c.tenDonVi" +
                        ", c.TongBan as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " + entityName + " c" +
                        " WHERE 1=1 " +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND (" + req.getType() + " IS NULL OR c.type = "+ req.getType() +") " +
                        " AND c.tongBan > 0" +
                        " ORDER BY c.TongBan desc";
                items = DataUtils.convertList(hdrRepo.searchListTop_DT_T0(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.SO_LUONG -> {
                query = "SELECT TOP(" + top + ") " +
                        "c.tenNhomNganhHang, c.ThuocId, " +
                        "c.tenThuoc, c.tenDonVi" +
                        ", c.TongSoLuong as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " + entityName + " c" +
                        " WHERE 1=1 " +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND (" + req.getType() + " IS NULL OR c.type = "+ req.getType() +") " +
                        " AND c.tongSoLuong > 0" +
                        " ORDER BY c.TongSoLuong desc";
                items = DataUtils.convertList(hdrRepo.searchListTop_SL_T0(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.TSLN -> {
                query = "SELECT TOP(" + top + ") " +
                        "c.tenNhomNganhHang, c.ThuocId, " +
                        "c.tenThuoc, c.tenDonVi ," +
                        "((c.giaBanCs-c.giaNhapCs) / c.giaNhapCs)* 100 as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " + entityName + " c" +
                        " WHERE 1=1 " +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND (" + req.getType() + " IS NULL OR c.type = "+ req.getType() +") " +
                        " AND c.gianhapcs >0"+
                        " ORDER BY soLieuThiTruong desc";
                items = DataUtils.convertList(hdrRepo.searchListTop_TSLN_T0(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
        }
        return items;
    }
    //endregion
    //region Group By Day And Month
    private List<HangHoaDaTinhToanCache> groupByTop_T_ANY(int year, int month, GiaoDichHangHoaReq req,
                                                          LocalDate fromDate, LocalDate toDate,
                                                          int top, int type){
        String entityName = "GiaoDichHangHoa_T" + month + "_"+ year;
        Optional<Tuple> table = hdrRepo.checkTableExit(entityName);
        if(table.isEmpty()) return null;
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        String query = null;
        switch (type){
            case BaoCaoContains.DOANH_THU -> {
                query = "SELECT TOP(" + top + ") " +
                        "s.tenNhomNganhHang, s.ThuocId, " +
                        "s.tenThuoc, s.tenDonVi" +
                        ", s.Tong as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " +
                        "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
                        ", SUM(c.tongBan) as Tong FROM " + entityName + " c" +
                        " WHERE 1=1 " +
                        " AND ('" + fromDate + "' IS NULL OR c.NgayGiaoDich >= '" + fromDate + "')" +
                        " AND ('" + toDate + "' IS NULL OR c.NgayGiaoDich <= '"+ toDate +"')" +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND c.tongBan > 0" +
                        " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                        " ORDER BY s.Tong desc";
                items = DataUtils.convertList(hdrRepo.groupByTopDT_T_ANY(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.SO_LUONG -> {
                query = "SELECT TOP(" + top + ") " +
                        "s.tenNhomNganhHang, s.ThuocId, " +
                        "s.tenThuoc, s.tenDonVi" +
                        ", s.Tong as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " +
                        "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
                        ", SUM(c.TongSoLuong) as Tong FROM " + entityName + " c" +
                        " WHERE 1=1 " +
                        " AND ('" + fromDate + "' IS NULL OR c.NgayGiaoDich >= '" + fromDate + "')" +
                        " AND ('" + toDate + "' IS NULL OR c.NgayGiaoDich <= '"+ toDate +"')" +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND c.TongSoLuong > 0" +
                        " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                        " ORDER BY s.Tong desc";
                items = DataUtils.convertList(hdrRepo.groupByTopSL_T_ANY(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.TSLN -> {
                query = "SELECT TOP(" + top + ") " +
                        "s.tenNhomNganhHang, s.ThuocId, " +
                        "s.tenThuoc, s.tenDonVi, " +
                        "((s.gb-s.gn) /s.gn) * 100  as 'soLieuThiTruong'" +
                        ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                        "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                        " FROM " +
                        "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
                        ",avg(c.giabancs) as gb, avg(c.gianhapcs) as gn " +
                        "FROM " + entityName + " c" +
                        " WHERE 1=1 " +
                        " AND ('" + fromDate + "' IS NULL OR c.NgayGiaoDich >= '" + fromDate + "')" +
                        " AND ('" + toDate + "' IS NULL OR c.NgayGiaoDich <= '"+ toDate +"')" +
                        " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                        " AND (" + req.getNhomNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNhomNganhHangId() + ") "+
                        " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                        " AND c.giabancs > 0" +
                        " AND c.gianhapcs > 0" +
                        " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                        " ORDER BY ((s.gb-s.gn) /s.gn) * 100 desc";
                items = DataUtils.convertList(hdrRepo.groupByTopTSLN_T_ANY(query),
                        HangHoaDaTinhToanCache.class);
                break;
            }
        }
        return items;
    }
    //endregion

    private LocalDate convertToLocalDate(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

}
