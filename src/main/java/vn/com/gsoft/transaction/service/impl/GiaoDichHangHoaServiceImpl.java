package vn.com.gsoft.transaction.service.impl;


import com.ctc.wstx.util.DataUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.persistence.Tuple;
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
    private GiaoDichHH_T8_2024Repository hdrRepoT8;
    @Autowired
    private GiaoDichHH_T7_2024Repository hdrRepoT7;
    @Autowired
    private GiaoDichHH_T6_2024Repository hdrRepoT6;
    @Autowired
    private GiaoDichHH_T5_2024Repository hdrRepoT5;
    @Autowired
    private GiaoDichHH_T4_2024Repository hdrRepoT4;
    @Autowired
    private GiaoDichHH_T3_2024Repository hdrRepoT3;
    @Autowired
    private GiaoDichHH_T2_2024Repository hdrRepoT2;
    @Autowired
    private GiaoDichHH_T1_2024Repository hdrRepoT1;

    @Autowired
    public GiaoDichHangHoaServiceImpl(GiaoDichHangHoaRepository hdrRepo,
                                      RedisListService redisListService,
                                      HangHoaRepository hangHoaRepo,
                                      GiaoDichHH_T3_2024Repository hdrRepoT3,
                                      GiaoDichHH_T4_2024Repository hdrRepoT4,
                                      GiaoDichHH_T5_2024Repository hdrRepoT5,
                                      GiaoDichHH_T6_2024Repository hdrRepoT6,
                                      GiaoDichHH_T7_2024Repository hdrRepoT7,
                                      GiaoDichHH_T8_2024Repository hdrRepoT8,
                                      GiaoDichHH_T1_2024Repository hdrRepoT1,
                                      GiaoDichHH_T2_2024Repository hdrRepoT2
                                ) {
        super(hdrRepo);
        this.hdrRepo = hdrRepo;
        this.redisListService = redisListService;
        this.hangHoaRepo = hangHoaRepo;
        this.hdrRepoT1 = hdrRepoT1;
        this.hdrRepoT2 = hdrRepoT2;
        this.hdrRepoT3 = hdrRepoT3;
        this.hdrRepoT4 = hdrRepoT4;
        this.hdrRepoT5 = hdrRepoT5;
        this.hdrRepoT6 = hdrRepoT6;
        this.hdrRepoT7 = hdrRepoT7;
        this.hdrRepoT8 = hdrRepoT8;
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
        var isEnd = endtDate.getMonthValue() == tDate.getMonthValue();
        if(months >= 1 && months < 11 && isStart && isEnd){
            var arrMonth = new ArrayList<Integer>();
            for (var  i = 0 ; i < months + 1; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            items = DataUtils.convertList(hdrRepo.groupByTopDTT0_2024(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
        }else if(months == 0 && isStart && isEnd || months == 11){
            req.setType( months == 0 ? fDate.getMonthValue() : 0);
            items = DataUtils.convertList(hdrRepo.searchListTop_DT_T0_2024(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
        }
        else if(months == 0 && fDate.getMonthValue() != tDate.getMonthValue()){
            items = chuaQua1ThangO2ThangKhacNhau(req, fDate, tDate, BaoCaoContains.DOANH_THU);
        }
        else {
            items = chuaQua1Thang(req, fDate, tDate, BaoCaoContains.DOANH_THU);
        }
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
        var isEnd = endtDate.getMonthValue() == tDate.getMonthValue();
        if(months >= 1 && months < 11 && isStart && isEnd){
            List<Integer> arrMonth = new ArrayList<>();
            for (var  i = 0 ; i < months + 1; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            items = DataUtils.convertList(hdrRepo.groupByTopDTT0_2024(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
        }else if(months == 0 && isStart && isEnd || months == 11){
            req.setType( months == 0 ? fDate.getMonthValue() : 0);
            items = DataUtils.convertList(hdrRepo.searchListTop_DT_T0_2024(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
        }
        else if(months == 0 && fDate.getMonthValue() != tDate.getMonthValue()){
            items = chuaQua1ThangO2ThangKhacNhau(req, fDate, tDate, BaoCaoContains.SO_LUONG);
        }
        else {
            items = chuaQua1Thang(req, fDate, tDate, BaoCaoContains.SO_LUONG);
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
        Calendar dateArchive = Calendar.getInstance();
        dateArchive.add(Calendar.YEAR, -1);
        List<GiaoDichHangHoaCache> dataArchive = new ArrayList<>();

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
        var isEnd = endtDate.getMonthValue() == tDate.getMonthValue();
        if(months >= 1 && months < 11 && isStart && isEnd){
            List<Integer> arrMonth = new ArrayList<>();
            for (var  i = 0 ; i < months + 1; i++){
                arrMonth.add(fDate.getMonthValue() + i);
            }
            req.setTypes(arrMonth.toArray(new Integer[arrMonth.size()]));
            items = DataUtils.convertList(hdrRepo.groupByTopDTT0_2024(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
        }else if(months == 0 && isStart && isEnd || months == 11){
            req.setType( months == 0 ? fDate.getMonthValue() : 0);
            items = DataUtils.convertList(hdrRepo.searchListTop_DT_T0_2024(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
        }
        else if(months == 0 && fDate.getMonthValue() != tDate.getMonthValue()){
            items = chuaQua1ThangO2ThangKhacNhau(req, fDate, tDate, BaoCaoContains.TSLN);
        }
        else {
            items = chuaQua1Thang(req, fDate, tDate, BaoCaoContains.TSLN);
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
            var items = DataUtils.convertList(hdrRepoT8.searchList(new GiaoDichHangHoaReq()),
                    GiaoDichHangHoa_T8_2024.class);
            if (items.stream().count() > 0) {
                var groupBy = items.stream().collect(Collectors.groupingBy(x -> x.getThuocId()));
                groupBy.keySet().forEach(x->{
                    var key = "T8";
                    var values = groupBy.get(x);
                    HangHoaDaTinhToanCache hh = new HangHoaDaTinhToanCache();
                    hh.setThuocId(x);
                    hh.setTenThuoc(values.get(0).getTenThuoc());
                    hh.setTenNhomNganhHang(values.get(0).getTenNhomNganhHang());
                    hh.setTenDonVi(values.get(0).getTenDonVi());
                    hh.setNhomDuocLyId(values.get(0).getNhomDuocLyId());
                    hh.setNhomNganhHangId(values.get(0).getNhomNganhHangId());
                    hh.setNhomHoatChatId(values.get(0).getNhomHoatChatId());
                    hh.setTongBan(values.stream()
                            .map(xx->xx.getTongBan())
                            .reduce(BigDecimal.ZERO, BigDecimal::add));
                    hh.setTongNhap(values.stream()
                            .map(xx->xx.getTongNhap())
                            .reduce(BigDecimal.ZERO, BigDecimal::add));
                    hh.setSoLuong(values.stream()
                            .map(xx->xx.getTongSoLuong())
                            .reduce(BigDecimal.ZERO, BigDecimal::add));
                    redisListService.pushDataToRedisByTime(hh, key);
                });
            }
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

    private List<HangHoaDaTinhToanCache> chuaQua1Thang(GiaoDichHangHoaReq req, LocalDate fDate, LocalDate tDate , int type){
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        switch (type){
            case BaoCaoContains.DOANH_THU -> {
                if(fDate.getMonthValue() == 8 && tDate.getMonthValue() == 8){
                    items = DataUtils.convertList(hdrRepoT8.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 7 && tDate.getMonthValue() == 7){
                    req.setType(7);
                    items = DataUtils.convertList(hdrRepoT7.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 6 && tDate.getMonthValue() == 6){
                    items = DataUtils.convertList(hdrRepoT6.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 5 && tDate.getMonthValue() == 5){
                    items = DataUtils.convertList(hdrRepoT5.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 4 && tDate.getMonthValue() == 4){
                    items = DataUtils.convertList(hdrRepoT4.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 3 && tDate.getMonthValue() == 3){
                    items = DataUtils.convertList(hdrRepoT3.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 2 && tDate.getMonthValue() == 2){
                    items = DataUtils.convertList(hdrRepoT2.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 1 && tDate.getMonthValue() == 1){
                    items = DataUtils.convertList(hdrRepoT1.groupByTopDT(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                break;
            }
            case BaoCaoContains.SO_LUONG -> {
                if(fDate.getMonthValue() == 8 && tDate.getMonthValue() == 8){
                    items = DataUtils.convertList(hdrRepoT8.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 7 && tDate.getMonthValue() == 7){
                    req.setType(7);
                    items = DataUtils.convertList(hdrRepoT7.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 6 && tDate.getMonthValue() == 6){
                    items = DataUtils.convertList(hdrRepoT6.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 5 && tDate.getMonthValue() == 5){
                    items = DataUtils.convertList(hdrRepoT5.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 4 && tDate.getMonthValue() == 4){
                    items = DataUtils.convertList(hdrRepoT4.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 3 && tDate.getMonthValue() == 3){
                    items = DataUtils.convertList(hdrRepoT3.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 2 && tDate.getMonthValue() == 2){
                    items = DataUtils.convertList(hdrRepoT2.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 1 && tDate.getMonthValue() == 1){
                    items = DataUtils.convertList(hdrRepoT1.groupByTopSL(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                break;
            }
            case BaoCaoContains.TSLN -> {
                if(fDate.getMonthValue() == 8 && tDate.getMonthValue() == 8){
                    items = DataUtils.convertList(hdrRepoT8.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 7 && tDate.getMonthValue() == 7){
                    req.setType(7);
                    items = DataUtils.convertList(hdrRepoT7.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 6 && tDate.getMonthValue() == 6){
                    items = DataUtils.convertList(hdrRepoT6.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 5 && tDate.getMonthValue() == 5){
                    items = DataUtils.convertList(hdrRepoT5.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 4 && tDate.getMonthValue() == 4){
                    items = DataUtils.convertList(hdrRepoT4.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 3 && tDate.getMonthValue() == 3){
                    items = DataUtils.convertList(hdrRepoT3.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 2 && tDate.getMonthValue() == 2){
                    items = DataUtils.convertList(hdrRepoT2.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 1 && tDate.getMonthValue() == 1){
                    items = DataUtils.convertList(hdrRepoT1.groupByTopTSLN(req, req.getPageSize()), HangHoaDaTinhToanCache.class);
                }
                break;
            }
        }

        return items;
    }

    private List<HangHoaDaTinhToanCache> chuaQua1ThangO2ThangKhacNhau(GiaoDichHangHoaReq req, LocalDate fDate, LocalDate tDate, int type) throws Exception {
        List<HangHoaDaTinhToanCache> items = new ArrayList<>();
        List<HangHoaDaTinhToanCache> itemLast = new ArrayList<>();
        switch (type){
            case BaoCaoContains.DOANH_THU -> {
                if(fDate.getMonthValue() == 8){
                    items = DataUtils.convertList(hdrRepoT8.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 7){
                    items = DataUtils.convertList(hdrRepoT7.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 6){
                    items = DataUtils.convertList(hdrRepoT6.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 5){
                    items = DataUtils.convertList(hdrRepoT5.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 4){
                    items = DataUtils.convertList(hdrRepoT4.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 3){
                    items = DataUtils.convertList(hdrRepoT3.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 2){
                    items = DataUtils.convertList(hdrRepoT2.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else{
                    items = DataUtils.convertList(hdrRepoT1.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }

                if(tDate.getMonthValue() == 8){
                    itemLast = DataUtils.convertList(hdrRepoT8.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 7){
                    itemLast = DataUtils.convertList(hdrRepoT7.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 6){
                    itemLast = DataUtils.convertList(hdrRepoT6.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 5){
                    itemLast = DataUtils.convertList(hdrRepoT5.groupByTopDT(req,5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 4){
                    itemLast = DataUtils.convertList(hdrRepoT4.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 3){
                    itemLast = DataUtils.convertList(hdrRepoT3.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 2){
                    itemLast = DataUtils.convertList(hdrRepoT2.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else{
                    itemLast = DataUtils.convertList(hdrRepoT1.groupByTopDT(req, 5000), HangHoaDaTinhToanCache.class);
                }
            }
            case BaoCaoContains.SO_LUONG -> {
                if(fDate.getMonthValue() == 8){
                    items = DataUtils.convertList(hdrRepoT8.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 7){
                    items = DataUtils.convertList(hdrRepoT7.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 6){
                    items = DataUtils.convertList(hdrRepoT6.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 5){
                    items = DataUtils.convertList(hdrRepoT5.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 4){
                    items = DataUtils.convertList(hdrRepoT4.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 3){
                    items = DataUtils.convertList(hdrRepoT3.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 2){
                    items = DataUtils.convertList(hdrRepoT2.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else{
                    items = DataUtils.convertList(hdrRepoT1.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }

                if(tDate.getMonthValue() == 8){
                    itemLast = DataUtils.convertList(hdrRepoT8.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 7){
                    itemLast = DataUtils.convertList(hdrRepoT7.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 6){
                    itemLast = DataUtils.convertList(hdrRepoT6.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 5){
                    itemLast = DataUtils.convertList(hdrRepoT5.groupByTopSL(req,5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 4){
                    itemLast = DataUtils.convertList(hdrRepoT4.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 3){
                    itemLast = DataUtils.convertList(hdrRepoT3.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 2){
                    itemLast = DataUtils.convertList(hdrRepoT2.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else{
                    itemLast = DataUtils.convertList(hdrRepoT1.groupByTopSL(req, 5000), HangHoaDaTinhToanCache.class);
                }
                break;
            }
            case BaoCaoContains.TSLN -> {
                if(fDate.getMonthValue() == 8){
                    items = DataUtils.convertList(hdrRepoT8.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 7){
                    items = DataUtils.convertList(hdrRepoT7.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 6){
                    items = DataUtils.convertList(hdrRepoT6.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 5){
                    items = DataUtils.convertList(hdrRepoT5.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 4){
                    items = DataUtils.convertList(hdrRepoT4.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 3){
                    items = DataUtils.convertList(hdrRepoT3.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(fDate.getMonthValue() == 2){
                    items = DataUtils.convertList(hdrRepoT2.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else{
                    items = DataUtils.convertList(hdrRepoT1.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }

                if(tDate.getMonthValue() == 8){
                    itemLast = DataUtils.convertList(hdrRepoT8.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 7){
                    itemLast = DataUtils.convertList(hdrRepoT7.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 6){
                    itemLast = DataUtils.convertList(hdrRepoT6.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 5){
                    itemLast = DataUtils.convertList(hdrRepoT5.groupByTopTSLN(req,5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 4){
                    itemLast = DataUtils.convertList(hdrRepoT4.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 3){
                    itemLast = DataUtils.convertList(hdrRepoT3.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else if(tDate.getMonthValue() == 2){
                    itemLast = DataUtils.convertList(hdrRepoT2.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
                else{
                    itemLast = DataUtils.convertList(hdrRepoT1.groupByTopTSLN(req, 5000), HangHoaDaTinhToanCache.class);
                }
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
        items.sort((hh1, hh2) -> hh2.getSoLieuThiTruong().compareTo(hh1.getSoLieuThiTruong()));

        return items.stream().limit(req.getPageSize()).toList();
    }
}
