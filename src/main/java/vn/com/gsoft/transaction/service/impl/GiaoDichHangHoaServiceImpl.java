package vn.com.gsoft.transaction.service.impl;


import com.ctc.wstx.util.DataUtil;
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
    public List<TopMatHangRes> topDoanhThuBanChay(GiaoDichHangHoaReq req) throws Exception{
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

        var items = getDataRedis(req);
        if(req.getNganhHangId() != null && req.getNganhHangId() > 0){
            items = items.stream().filter(item->item.getNhomNganhHangId().equals(req.getNganhHangId()))
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
        items.addAll(dataArchive);

        List<TopMatHangRes> data = items.stream()
                .collect(Collectors.groupingBy(
                        GiaoDichHangHoaCache::getThuocId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) {
                                        return null;
                                    }
                                    var duLieuCoSo =  x.stream().filter(item->item.getMaCoSo().equals(userInfo.getMaCoSo()));
                                    BigDecimal doanhSoCoSo = BigDecimal.valueOf(0);

                                    if(!duLieuCoSo.isParallel()){
                                        doanhSoCoSo = duLieuCoSo.map(item-> item.getGiaBan().multiply(item.getSoLuong())).reduce(BigDecimal.ZERO, BigDecimal::add);
                                    }
                                    return new TopMatHangRes(
                                            x.get(0).getTenThuoc(),
                                            x.get(0).getTenNhomNganhHang(),
                                            x.get(0).getTenDonVi(),
                                            x.stream().map(item->item.getGiaBan().multiply(item.getSoLuong())).reduce(BigDecimal.ZERO, BigDecimal::add),
                                            doanhSoCoSo
                                    );
                                }
                        )
                ))
                .values().stream()
                .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong()))
                .limit(req.getPageSize())
                .collect(Collectors.toList());
        return  data;
    }

    @Override
    public List<TopMatHangRes> topSLBanChay(GiaoDichHangHoaReq req) throws Exception{
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

        var items = getDataRedis(req);
        if(req.getNganhHangId() != null && req.getNganhHangId() > 0){
            items = items.stream().filter(item->item.getNhomNganhHangId().equals(req.getNganhHangId()))
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
        items.addAll(dataArchive);

        List<TopMatHangRes> data = items.stream()
                .collect(Collectors.groupingBy(
                        GiaoDichHangHoaCache::getThuocId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) {
                                        return null;
                                    }
                                    var duLieuCoSo =  x.stream().filter(item->item.getMaCoSo().equals(userInfo.getMaCoSo()));
                                    BigDecimal doanhSoCoSo = BigDecimal.valueOf(0);

                                    if(!duLieuCoSo.isParallel()){
                                        doanhSoCoSo = duLieuCoSo.map(item-> item.getSoLuong()).reduce(BigDecimal.ZERO, BigDecimal::add);
                                    }
                                    return new TopMatHangRes(
                                            x.get(0).getTenThuoc(),
                                            x.get(0).getTenNhomNganhHang(),
                                            x.get(0).getTenDonVi(),
                                            x.stream().map(item->item.getSoLuong()).reduce(BigDecimal.ZERO, BigDecimal::add),
                                            doanhSoCoSo
                                    );
                                }
                        )
                ))
                .values().stream()
                .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong()))
                .limit(req.getPageSize())
                .collect(Collectors.toList());
        return  data;
    }

    @Override
    public List<TopMatHangRes> topTSLNCaoNhat(GiaoDichHangHoaReq req) throws Exception {
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

        var items = getDataRedis(req);
        if(req.getNganhHangId() != null && req.getNganhHangId() > 0){
            items = items.stream().filter(item->item.getNhomNganhHangId().equals(req.getNganhHangId()))
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
        items.addAll(dataArchive);
        List<TopMatHangRes> data = items.parallelStream()
                .collect(Collectors.groupingByConcurrent(
                        GiaoDichHangHoaCache::getThuocId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                x -> {
                                    if (x.isEmpty()) return null;

                                    var duLieuCoSo = x.parallelStream()
                                            .filter(item -> item.getMaCoSo().equals(userInfo.getMaCoSo()))
                                            .collect(Collectors.toList());

                                    BigDecimal tslnCoSo = BigDecimal.ZERO;
                                    if (!duLieuCoSo.isEmpty()) {
                                        BigDecimal tongGB = duLieuCoSo.parallelStream()
                                                .filter(xx->xx.getGiaBan() != null && xx.getLoaiGiaoDich() == 2 && xx.getGiaBan().compareTo(BigDecimal.ZERO) > 0)
                                                .map(GiaoDichHangHoaCache::getGiaBan)
                                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                                        BigDecimal tongGN = duLieuCoSo.parallelStream()
                                                .filter(xx->xx.getGiaNhap() != null).map(GiaoDichHangHoaCache::getGiaNhap)
                                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                                        if (tongGN.compareTo(BigDecimal.ZERO) > 0) {
                                            tslnCoSo = (tongGB.subtract(tongGN))
                                                    .divide(tongGN, 2, RoundingMode.HALF_UP)
                                                    .multiply(BigDecimal.valueOf(100));
                                        }
                                    }

                                    BigDecimal tongGNTT = x.parallelStream()
                                            .filter(xx->xx.getGiaNhap() != null).map(GiaoDichHangHoaCache::getGiaNhap)
                                            .reduce(BigDecimal.ZERO, BigDecimal::add);

                                    BigDecimal tongGBTT = x.parallelStream()
                                            .filter(xx->xx.getGiaBan() != null).map(GiaoDichHangHoaCache::getGiaBan)
                                            .reduce(BigDecimal.ZERO, BigDecimal::add);

                                    BigDecimal tslnTT = BigDecimal.ZERO;
                                    if (tongGNTT.compareTo(BigDecimal.ZERO) > 0) {
                                        tslnTT = (tongGBTT.subtract(tongGNTT))
                                                .divide(tongGNTT, 2, RoundingMode.HALF_UP)
                                                .multiply(BigDecimal.valueOf(100));
                                    }

                                    return new TopMatHangRes(
                                            x.get(0).getTenThuoc(),
                                            x.get(0).getTenNhomNganhHang(),
                                            x.get(0).getTenDonVi(),
                                            tslnTT,
                                            tslnCoSo
                                    );
                                }
                        )
                ))
                .values().parallelStream()
                .filter(Objects::nonNull)
                .sorted((g1, g2) -> g2.getSoLieuThiTruong().compareTo(g1.getSoLieuThiTruong()))
                .limit(req.getPageSize())
                .collect(Collectors.toList());

        return data;
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
        // Khởi tạo định dạng ngày tháng
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Lấy ngày hiện tại
        LocalDate today = LocalDate.now();

        // Lấy ngày hôm nay của năm trước
        LocalDate oneYearAgo = today.minusYears(1);

        // Tạo danh sách các ngày từ ngày hiện tại trở về ngày của năm trước
        LocalDate date = oneYearAgo;
        List<String> keys = new ArrayList<>();
        while (!date.isAfter(today)) {
            // Chuyển LocalDate thành Date để in
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
                var key = "transaction-" + df.format(todayWithZeroTime);
                redisListService.pushDataToRedisByTime(items, key);
            }
            date = date.plusDays(1);
        }

    }

    private List<GiaoDichHangHoaCache> getDataRedis(GiaoDichHangHoaReq req) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        LocalDate toDate = convertToLocalDate(req.getToDate());

        LocalDate fromDate = convertToLocalDate(req.getFromDate());
        List<String> keys = new ArrayList<>();
        List<GiaoDichHangHoaCache> dataLst = new ArrayList<GiaoDichHangHoaCache>();
        while (!fromDate.isAfter(toDate)) {
            String pattern = "dd/MM/yyyy";
            Date todayWithZeroTime = formatter.parse(formatter.format(req.getToDate()));
            DateFormat df = new SimpleDateFormat(pattern);
            var data = redisListService.getAllDataKey("transaction-" + df.format(todayWithZeroTime));
            dataLst.addAll(data);
            fromDate = fromDate.plusDays(1);
        }
       return dataLst;
    }

    // Hàm chuyển đổi Date sang LocalDate
    private LocalDate convertToLocalDate(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }
}
