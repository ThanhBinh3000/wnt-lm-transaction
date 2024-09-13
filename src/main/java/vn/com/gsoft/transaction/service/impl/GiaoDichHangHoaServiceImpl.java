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
        setDefaultDates(req);
        req.setMaCoSo(userInfo.getMaCoSo());
        req.setPageSize(req.getPageSize() == null || req.getPageSize() < 1 ? LimitPageConstant.DEFAULT : req.getPageSize());

        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        List<Tuple> tops= new ArrayList<>();
        LocalDate fDate = req.getFromDate().toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        LocalDate tDate = req.getToDate().toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        var months = ChronoUnit.MONTHS.between(fDate, tDate);
        LocalDate startfDate = fDate.with(TemporalAdjusters.firstDayOfMonth());
        LocalDate endtDate = tDate.with(TemporalAdjusters.lastDayOfMonth());
        var isStart = startfDate.getDayOfMonth() == fDate.getDayOfMonth();
        var isEnd = endtDate.getDayOfMonth() == tDate.getDayOfMonth();
        if(months >= 1 && months < 11 && isStart && isEnd){
            var arrMonth = new ArrayList<Integer>();
            for (var  i = 0 ; i < months + 1; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            items = DataUtils.convertList(hdrRepo.groupByTopDT_T0(groupByTopDT_T0(endtDate.getYear()
                            , req, req.getPageSize())),
                    HangHoaDaTinhToanCache.class);
        }
        else if(months >= 1 && months < 11 && (!isStart || !isEnd)){
            var arrMonth = new ArrayList<Integer>();
            var count = 0;
            var addMonth = 0;
            long totalDays = ChronoUnit.DAYS.between(fDate, tDate);
            count = isStart ? 0 : 1;
            addMonth = isEnd ? 1 : 0;
            for (var  i = count ; i < months + addMonth; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            if(months >= 1 && !isFirstAndLastMonthFull(fDate, tDate)){
                arrMonth.add((int)(tDate.getMonthValue() - 1L));
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            if(arrMonth.size() > 0){
                items = DataUtils.convertList(hdrRepo.groupByTopDT_T0(
                        groupByTopDT_T0(endtDate.getYear(), req, req.getPageSize() + 100)),
                        HangHoaDaTinhToanCache.class);
            }
            //tinh khoang thua cua thang con lai
            List<HangHoaDaTinhToanCache> itemExtra  = new ArrayList<>();
            if(!isStart){
                var giaoDichReq1 = new GiaoDichHangHoaReq();
                BeanUtils.copyProperties(req, giaoDichReq1);
                giaoDichReq1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
                YearMonth yearMonth = YearMonth.from(fDate);
                LocalDate tDate1 = yearMonth.atEndOfMonth();
                itemExtra.addAll(chuaQua1Thang(
                        giaoDichReq1, fDate, tDate1, BaoCaoContains.DOANH_THU, giaoDichReq1.getPageSize()));
            }
            if(!isEnd){
                var giaoDichReq2 = new GiaoDichHangHoaReq();
                BeanUtils.copyProperties(req, giaoDichReq2);
                giaoDichReq2.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
                LocalDate fDate1 = tDate.withDayOfMonth(1);
                itemExtra.addAll(chuaQua1Thang(
                        giaoDichReq2, fDate1, tDate, BaoCaoContains.DOANH_THU, giaoDichReq2.getPageSize()));
            }
            if(!itemExtra.isEmpty()){
                var groupItems = itemExtra.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
                for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                    boolean found = false;
                    for (HangHoaDaTinhToanCache hh : items) {
                        if (hh.getThuocId().equals(entry.getKey())) {
                            hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
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
        else if(months == 0 && isStart && isEnd || months == 11){
            req.setType( months == 0 ? fDate.getMonthValue() : 0);
            items = DataUtils.convertList(hdrRepo.searchListTop_DT_T0(
                    searchTopDT_T0(endtDate.getYear(), req, req.getPageSize())),
                    HangHoaDaTinhToanCache.class);
        }
        else if(months == 0 && fDate.getMonthValue() != tDate.getMonthValue()){
            items = chuaQua1ThangO2ThangKhacNhau(
                    req, fDate, tDate, BaoCaoContains.DOANH_THU, req.getPageSize());
        }
        else {
            items = chuaQua1Thang(req, fDate, tDate, BaoCaoContains.DOANH_THU, req.getPageSize());
        }
        //lấy ra doanh so cs
        if(userInfo.getMaCoSo() != null && userInfo.getAuthorities().stream().filter(x->x.getAuthority() =="DLGDHH") != null){
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
        List<Tuple> tops= new ArrayList<>();
        LocalDate fDate = req.getFromDate().toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        LocalDate tDate = req.getToDate().toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        var months = ChronoUnit.MONTHS.between(fDate, tDate);
        LocalDate startfDate = fDate.with(TemporalAdjusters.firstDayOfMonth());
        LocalDate endtDate = tDate.with(TemporalAdjusters.lastDayOfMonth());
        var isStart = startfDate.getDayOfMonth() == fDate.getDayOfMonth();
        var isEnd = endtDate.getDayOfMonth() == tDate.getDayOfMonth();
        if(months >= 1 && months < 11 && isStart && isEnd){
            var arrMonth = new ArrayList<Integer>();
            for (var  i = 0 ; i < months + 1; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            items = DataUtils.convertList(hdrRepo.groupByTopSL_T0(
                    groupByTopSL_T0(endtDate.getYear(), req, req.getPageSize())),
                    HangHoaDaTinhToanCache.class);
        }
        else if(months >= 1 && months < 11 && (!isStart || !isEnd)){
            var arrMonth = new ArrayList<Integer>();
            var count = 0;
            var addMonth = 0;
            long totalDays = ChronoUnit.DAYS.between(fDate, tDate);
            count = isStart ? 0 : 1;
            addMonth = isEnd ? 1 : 0;
            for (var  i = count ; i < months + addMonth; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            if(months >= 1 && !isFirstAndLastMonthFull(fDate, tDate)){
                arrMonth.add((int)(tDate.getMonthValue() - 1L));
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            if(arrMonth.size() > 0){
                items = DataUtils.convertList(hdrRepo.groupByTopSL_T0(
                        groupByTopSL_T0(endtDate.getYear(), req, req.getPageSize() + 100)),
                        HangHoaDaTinhToanCache.class);
            }

            //tinh khoang thua cua thang con lai
            List<HangHoaDaTinhToanCache> itemExtra  = new ArrayList<>();
            if(!isStart){
                var giaoDichReq1 = new GiaoDichHangHoaReq();
                BeanUtils.copyProperties(req, giaoDichReq1);
                giaoDichReq1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
                YearMonth yearMonth = YearMonth.from(fDate);
                LocalDate tDate1 = yearMonth.atEndOfMonth();
                itemExtra.addAll(chuaQua1Thang(giaoDichReq1, fDate, tDate1, BaoCaoContains.SO_LUONG,
                        giaoDichReq1.getPageSize()));
            }
            if(!isEnd){
                var giaoDichReq2 = new GiaoDichHangHoaReq();
                BeanUtils.copyProperties(req, giaoDichReq2);
                giaoDichReq2.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
                LocalDate fDate1 = tDate.withDayOfMonth(1);
                itemExtra.addAll(chuaQua1Thang(giaoDichReq2, fDate1, tDate, BaoCaoContains.SO_LUONG,
                        giaoDichReq2.getPageSize()));
            }
            if(!itemExtra.isEmpty()){
                var groupItems = itemExtra.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
                for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                    boolean found = false;
                    for (HangHoaDaTinhToanCache hh : items) {
                        if (hh.getThuocId().equals(entry.getKey())) {
                            hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
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
        else if(months == 0 && isStart && isEnd || months == 11){
            req.setType( months == 0 ? fDate.getMonthValue() : 0);
            items = DataUtils.convertList(hdrRepo.searchListTop_SL_T0(searchTopSL_T0(endtDate.getYear(), req)),
                    HangHoaDaTinhToanCache.class);
        }
        else if(months == 0 && fDate.getMonthValue() != tDate.getMonthValue()){
            items = chuaQua1ThangO2ThangKhacNhau(req, fDate, tDate, BaoCaoContains.SO_LUONG, req.getPageSize());
        }
        else {
            items = chuaQua1Thang(req, fDate, tDate, BaoCaoContains.SO_LUONG, req.getPageSize());
        }

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
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        List<Tuple> tops= new ArrayList<>();
        LocalDate fDate = req.getFromDate().toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        LocalDate tDate = req.getToDate().toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        var months = ChronoUnit.MONTHS.between(fDate, tDate);
        LocalDate startfDate = fDate.with(TemporalAdjusters.firstDayOfMonth());
        LocalDate endtDate = tDate.with(TemporalAdjusters.lastDayOfMonth());
        var isStart = startfDate.getDayOfMonth() == fDate.getDayOfMonth();
        var isEnd = endtDate.getDayOfMonth() == tDate.getDayOfMonth();
        if(months >= 1 && months < 11 && isStart && isEnd){
            var arrMonth = new ArrayList<Integer>();
            for (var  i = 0 ; i < months + 1; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            items = DataUtils.convertList(hdrRepo.groupByTopTSLN_T0(
                    groupByTopTSLN_T0(endtDate.getYear(), req, req.getPageSize())),
                    HangHoaDaTinhToanCache.class);
        }
        else if(months >= 1 && months < 11 && (!isStart || !isEnd)){
            var arrMonth = new ArrayList<Integer>();
            var count = 0;
            var addMonth = 0;
            long totalDays = ChronoUnit.DAYS.between(fDate, tDate);
            count = isStart ? 0 : 1;
            addMonth = isEnd ? 1 : 0;
            for (var  i = count ; i < months + addMonth; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            if(months >= 1 && !isFirstAndLastMonthFull(fDate, tDate)){
                arrMonth.add((int)(tDate.getMonthValue() - 1L));
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            if(arrMonth.size() > 0){
                items = DataUtils.convertList(hdrRepo.groupByTopTSLN_T0(
                        groupByTopTSLN_T0(endtDate.getYear(), req, req.getPageSize() + 100)),
                        HangHoaDaTinhToanCache.class);
            }

            //tinh khoang thua cua thang con lai
            List<HangHoaDaTinhToanCache> itemExtra  = new ArrayList<>();
            if(!isStart){
                var giaoDichReq1 = new GiaoDichHangHoaReq();
                BeanUtils.copyProperties(req, giaoDichReq1);
                giaoDichReq1.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
                YearMonth yearMonth = YearMonth.from(fDate);
                LocalDate tDate1 = yearMonth.atEndOfMonth();
                itemExtra.addAll(chuaQua1Thang(giaoDichReq1, fDate, tDate1, BaoCaoContains.TSLN,
                        giaoDichReq1.getPageSize()));
            }
            if(!isEnd){
                var giaoDichReq2 = new GiaoDichHangHoaReq();
                BeanUtils.copyProperties(req, giaoDichReq2);
                giaoDichReq2.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
                LocalDate fDate1 = tDate.withDayOfMonth(1);
                itemExtra.addAll(chuaQua1Thang(giaoDichReq2, fDate1, tDate, BaoCaoContains.TSLN,
                        giaoDichReq2.getPageSize()));
            }
            if(!itemExtra.isEmpty()){
                var groupItems = itemExtra.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
                for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
                    boolean found = false;
                    for (HangHoaDaTinhToanCache hh : items) {
                        if (hh.getThuocId().equals(entry.getKey())) {
                            hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
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
        else if(months == 0 && isStart && isEnd || months == 11){
            req.setType( months == 0 ? fDate.getMonthValue() : 0);
            items = DataUtils.convertList(hdrRepo.searchListTop_TSLN_T0(searchTopTSLN_T0(endtDate.getYear(), req)),
                    HangHoaDaTinhToanCache.class);
        }
        else if(months == 0 && fDate.getMonthValue() != tDate.getMonthValue()){
            items = chuaQua1ThangO2ThangKhacNhau(req, fDate, tDate, BaoCaoContains.TSLN, req.getPageSize());
        }
        else {
            items = chuaQua1Thang(req, fDate, tDate, BaoCaoContains.TSLN, req.getPageSize());
        }
        //lấy ra doanh so cs
        if(userInfo.getMaCoSo() != null && userInfo.getAuthorities().stream().filter(x->x.getAuthority() =="DLGDHH") != null){
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

    private LocalDate convertToLocalDate(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    private List<HangHoaDaTinhToanCache> chuaQua1Thang(GiaoDichHangHoaReq req, LocalDate fDate,
                                                       LocalDate tDate , int type, int top){
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        switch (type){
            case BaoCaoContains.DOANH_THU -> {
                items = DataUtils.convertList(hdrRepo.groupByTopDT_T_ANY(groupByTopDT_T_ANY(fDate.getYear(),
                                fDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.SO_LUONG -> {
                items = DataUtils.convertList(hdrRepo.groupByTopSL_T_ANY(groupByTopSL_T_ANY(fDate.getYear(),
                                fDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.TSLN -> {
                items = DataUtils.convertList(hdrRepo.groupByTopTSLN_T_ANY(groupByTopTSLN_T_ANY(fDate.getYear(),
                                fDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);
                break;
            }
        }

        return items;
    }

    private List<HangHoaDaTinhToanCache> chuaQua1ThangO2ThangKhacNhau(GiaoDichHangHoaReq req, LocalDate fDate, LocalDate tDate, int type
    , int top) throws Exception {
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        List<HangHoaDaTinhToanCache> itemLast = new ArrayList<>();
        var pageSize = req.getPageSize();
        req.setPageSize(req.getPageSize() + BaoCaoContains.MAX);
        switch (type){
            case BaoCaoContains.DOANH_THU -> {
                items = DataUtils.convertList(hdrRepo.groupByTopDT_T_ANY(groupByTopDT_T_ANY(fDate.getYear(),
                                fDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);

                itemLast = DataUtils.convertList(hdrRepo.groupByTopDT_T_ANY(groupByTopDT_T_ANY(fDate.getYear(),
                                tDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);
            }
            case BaoCaoContains.SO_LUONG -> {
                items = DataUtils.convertList(hdrRepo.groupByTopSL_T_ANY(groupByTopSL_T_ANY(fDate.getYear(),
                                fDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);

                itemLast = DataUtils.convertList(hdrRepo.groupByTopSL_T_ANY(groupByTopSL_T_ANY(fDate.getYear(),
                                tDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);
                break;
            }
            case BaoCaoContains.TSLN -> {
                items = DataUtils.convertList(hdrRepo.groupByTopTSLN_T_ANY(groupByTopTSLN_T_ANY(fDate.getYear(),
                                fDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);

                itemLast = DataUtils.convertList(hdrRepo.groupByTopTSLN_T_ANY(groupByTopTSLN_T_ANY(fDate.getYear(),
                                tDate.getMonthValue(), req, fDate, tDate, top)),
                        HangHoaDaTinhToanCache.class);
                break;
            }
        }
        var groupItems = itemLast.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
        for (Map.Entry<Long, List<HangHoaDaTinhToanCache>> entry : groupItems.entrySet()) {
            boolean found = false;
            for (HangHoaDaTinhToanCache hh : items) {
                if (hh.getThuocId().equals(entry.getKey())) {
                    hh.setSoLieuThiTruong(entry.getValue().get(0).getSoLieuThiTruong());
                    found = true;
                    break;
                }
            }
            if (!found) {
                items.add(entry.getValue().get(0));
            }
        }
        if(items != null){
            items.sort((hh1, hh2) -> hh2.getSoLieuThiTruong().compareTo(hh1.getSoLieuThiTruong()));
        }
        return items.stream().limit(pageSize).toList();
    }

    private static boolean isFirstAndLastMonthFull(LocalDate startDate, LocalDate endDate) {
        LocalDate endOfFirstMonth = YearMonth.from(startDate).atEndOfMonth();
        long daysInFirstMonth = ChronoUnit.DAYS.between(startDate, endOfFirstMonth.plusDays(1));
        LocalDate startOfLastMonth = YearMonth.from(endDate).atDay(1);
        long daysInLastMonth = ChronoUnit.DAYS.between(startOfLastMonth, endDate.plusDays(1));
        long totalDays = daysInFirstMonth + daysInLastMonth;
        return totalDays > 31;
    }

    //region GROUP BY T0
    private String groupByTopSL_T0(int year, GiaoDichHangHoaReq req, int top){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        return  "SELECT TOP(" + top + ") " +
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
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " AND (c.type in (" + StringUtils.join(req.getTypes(), ',') + ")) " +
                " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                " ORDER BY s.Tong desc";
    }

    private String groupByTopDT_T0(int year, GiaoDichHangHoaReq req, int top){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        return  "SELECT TOP(" + top + ") " +
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
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " AND (c.type in (" + StringUtils.join(req.getTypes(), ',') + ")) " +
                " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                " ORDER BY s.Tong desc";
    }

    private String groupByTopTSLN_T0(int year, GiaoDichHangHoaReq req, int top){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        return  "SELECT TOP(" + top + ") " +
                "s.tenNhomNganhHang, s.ThuocId, " +
                "s.tenThuoc, s.tenDonVi, " +
                "(CASE WHEN s.tongNhap > 0 THEN ((s.tongBan - s.tongNhap) / s.tongNhap) * 100 ELSE NULL END)  as 'soLieuThiTruong'" +
                ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                " FROM " +
                "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi"  +
                ", SUM(c.tongNhap) as tongNhap, SUM(c.tongBan) as tongBan " +
                " FROM " + entityName + " c" +
                " WHERE 1=1 " +
                " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " AND (c.type in (" + StringUtils.join(req.getTypes(), ',') + ")) " +
                " AND c.tongNhap > 0"+
                " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                " ORDER BY (CASE WHEN s.tongNhap > 0 THEN ((s.tongBan - s.tongNhap) / s.tongNhap) * 100 ELSE NULL END) desc";
    }
    //endregion
    //region Search Top T0
    private String searchTopDT_T0(int year, GiaoDichHangHoaReq req, int top){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        return "SELECT TOP(" + top + ")" +
                "c.tenNhomNganhHang, c.ThuocId, " +
                "c.tenThuoc, c.tenDonVi" +
                ", c.TongBan as 'soLieuThiTruong'" +
                ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                " FROM " + entityName + " c" +
                " WHERE 1=1 " +
                " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " AND (" + req.getType() + " IS NULL OR c.type = "+ req.getType() +") " +
                " ORDER BY c.TongBan desc";
    }

    private String searchTopSL_T0(int year, GiaoDichHangHoaReq req){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        return "SELECT TOP(" + req.getPageSize() + ") " +
                "c.tenNhomNganhHang, c.ThuocId, " +
                "c.tenThuoc, c.tenDonVi" +
                ", c.TongSoLuong as 'soLieuThiTruong'" +
                ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                " FROM " + entityName + " c" +
                " WHERE 1=1 " +
                " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " AND (" + req.getType() + " IS NULL OR c.type = "+ req.getType() +") " +
                " ORDER BY c.TongSoLuong desc";
    }

    private String searchTopTSLN_T0(int year, GiaoDichHangHoaReq req){
        String entityName = "GiaoDichHangHoa_T0_"+ year;
        return "SELECT TOP(" + req.getPageSize() + ") " +
                "c.tenNhomNganhHang, c.ThuocId, " +
                "c.tenThuoc, c.tenDonVi ," +
                "(CASE WHEN c.tongNhap > 0 THEN ((c.tongBan - c.tongNhap) / c.tongNhap) * 100 ELSE NULL END)  as 'soLieuThiTruong'" +
                ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                " FROM " + entityName + " c" +
                " WHERE 1=1 " +
                " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " AND (" + req.getType() + " IS NULL OR c.type = "+ req.getType() +") " +
                " AND c.tongNhap > 0"+
                " ORDER BY soLieuThiTruong desc";
    }
    //endregion
    //region Group By Day And Month
    private String groupByTopSL_T_ANY(int year, int month, GiaoDichHangHoaReq req,
                                      LocalDate fromDate, LocalDate toDate,
                                      int top){
        String entityName = "GiaoDichHangHoa_T" + month + "_"+ year;
        return "SELECT TOP(" + top + ") " +
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
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                " ORDER BY s.Tong desc";
    }

    private String groupByTopDT_T_ANY(int year, int month, GiaoDichHangHoaReq req,
                                      LocalDate fromDate, LocalDate toDate,
                                      int top){
        String entityName = "GiaoDichHangHoa_T" + month + "_"+ year;
        return "SELECT TOP(" + top + ") " +
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
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                " ORDER BY s.Tong desc";
    }

    private String groupByTopTSLN_T_ANY(int year, int month, GiaoDichHangHoaReq req,
                                        LocalDate fromDate, LocalDate toDate,
                                        int top){
        String entityName = "GiaoDichHangHoa_T" + month + "_"+ year;
        return "SELECT TOP(" + top + ") " +
                "s.tenNhomNganhHang, s.ThuocId, " +
                "s.tenThuoc, s.tenDonVi, " +
                "(CASE WHEN s.tongNhap > 0 THEN ((s.tongBan - s.tongNhap) / s.tongNhap) * 100 ELSE NULL END)  as 'soLieuThiTruong'" +
                ",0 as 'nhomDuocLyId', 0 as 'nhomHoatChatId', 0 as 'nhomNganhHangId'," +
                "0.0 as 'tongNhap', 0.0 as 'tongBan', 0.0 as 'soLuong', 0.0 as 'soLieuCoSo'" +
                " FROM " +
                "(SELECT c.ThuocId,c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi" +
                ", SUM(c.tongNhap) as tongNhap, SUM(c.tongBan) as tongBan " +
                "FROM " + entityName + " c" +
                " WHERE 1=1 " +
                " AND ('" + fromDate + "' IS NULL OR c.NgayGiaoDich >= '" + fromDate + "')" +
                " AND ('" + toDate + "' IS NULL OR c.NgayGiaoDich <= '"+ toDate +"')" +
                " AND (" + req.getNhomDuocLyId() + " IS NULL OR c.nhomDuocLyId = " + req.getNhomDuocLyId() + ") "+
                " AND (" + req.getNganhHangId() + " IS NULL OR c.nhomNganhHangId = " + req.getNganhHangId() + ") "+
                " AND ("+ req.getNhomHoatChatId() +" IS NULL OR c.nhomHoatChatId = "+ req.getNhomHoatChatId() +") " +
                " AND c.tongNhap > 0"+
                " GROUP BY c.thuocId, c.tenThuoc, c.tenNhomNganhHang, c.tenDonVi) s" +
                " ORDER BY (CASE WHEN s.tongNhap > 0 THEN ((s.tongBan - s.tongNhap) / s.tongNhap) * 100 ELSE NULL END) desc";
    }
    //endregion

}
