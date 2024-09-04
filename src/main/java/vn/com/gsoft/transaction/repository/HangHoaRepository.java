package vn.com.gsoft.transaction.repository;

import jakarta.persistence.Tuple;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import vn.com.gsoft.transaction.entity.HangHoa;

import java.util.List;

@Repository
public interface HangHoaRepository extends CrudRepository<HangHoa, Long> {
    HangHoa findByThuocId(long l);
}
