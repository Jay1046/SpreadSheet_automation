from db.controller import rdb_pandas_extractor
from datetime import datetime, timedelta
import warnings


warnings.filterwarnings(action="ignore")

### 1) 쿠폰 별 사용일자 별 사용금액
def get_used_amount_by_used_date_common(
        start_date:str,
        end_date:str,
        db_connector,
        host_info,
        coupon_ids,
        coupon_kind=None,
        coupon_group_ids=None
):
    """
    사용금액에 대한 데이터 추출
    coupon_group이 필요한 경우와 아닌 경우를 나누어, 주석처리를 통해 데이터를 유연하게 획득
    """
    start_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")
    if coupon_kind==None:
        coupon_kind = "ic.coupon_identifier AS 쿠폰식별자"
    if coupon_group_ids==None:
        cg_annot = '--'
        c_annot = ''
    else:
        cg_annot = ''
        c_annot = '--'

    query = f"""
        SELECT 
            DATE(ic.used_date + '9 hours') AS 사용일자,
            {coupon_kind},
            cih.issue_kind as 발급종류,
            ic.benefit AS 쿠폰금액,
            c.name AS 쿠폰명,
            COUNT(*) as 사용장수,
            SUM(ic.benefit) 사용금액
        FROM issued_coupon AS ic
            INNER JOIN coupon AS c ON c.identifier = ic.coupon_identifier
            INNER JOIN coupon_issued_history cih ON ic.identifier = cih.issued_coupon_identifier
        WHERE 1=1
            AND ic.used_date BETWEEN '{start_date} 15:00:00' and '{end_date} 14:59:59' 
            {c_annot} AND ic.coupon_identifier IN {coupon_ids}
            {cg_annot} AND c.coupon_group_identifier in {coupon_group_ids}
            AND ic.status = 'USED'
        GROUP BY 1,2,3,4,5
    """
    used_amount_by_date = rdb_pandas_extractor(
        db_connector=db_connector(**host_info),
        query=query
    )

    return used_amount_by_date



### 2) 쿠폰 별 발급일자 별 사용율

def get_using_rate_by_got_date_common(
        start_date:str,
        end_date:str,
        db_connector,
        host_info,
        coupon_ids,
        coupon_kind=None,
        coupon_group_ids=None
):
    """
    start_date : 발급일자 조회 시작일
    end_date : 발급일자 조회 마지막일
    coupon_ids : 튜플 형식의 쿠폰식별자 목록
    db_connector : DB 연결 인스턴스
    host_info : 서버 호스트 정보
    """
    start_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")

    if coupon_kind==None:
        coupon_kind = "ic.coupon_identifier AS 쿠폰식별자"
    if coupon_group_ids==None:
        cg_annot = '--'
        c_annot = ''
    else:
        cg_annot = ''
        c_annot = '--'



    query = f"""
        SELECT 
            DATE(ic.got_date + '9 hours') AS 발급일자,
            {coupon_kind},
            ic.min_order_amount AS 최소주문금액,
            ic.benefit AS 쿠폰금액,
            cih.issue_kind AS 발급종류,
            count(ic.identifier) as 발급쿠폰수,
            count(distinct ic.user_identifier) as 발급유저수,
            SUM(CASE WHEN ic.status = 'AVAILABLE' then 1 ELSE 0 END) as 사용가능수,
            SUM(CASE WHEN ic.status = 'USED' then 1 ELSE 0 END) as 사용쿠폰수,
            COUNT(DISTINCT CASE WHEN ic.status = 'USED' THEN ic.user_identifier ELSE NULL END) AS 사용유저수,
            SUM(CASE WHEN ic.status = 'EXPIRED' then 1 ELSE 0 END) as 만료수,
            SUM(CASE WHEN ic.status NOT IN ('AVAILABLE', 'USED', 'EXPIRED') then 1 ELSE 0 END) as 그외,
            SUM(CASE WHEN ic.status = 'USED' then ic.benefit ELSE 0 END) as 리워드사용금액
        FROM issued_coupon AS ic
            INNER JOIN coupon AS c ON c.identifier = ic.coupon_identifier
            INNER JOIN coupon_issued_history cih ON ic.identifier = cih.issued_coupon_identifier
        WHERE 1=1
            AND ic.got_date BETWEEN '{start_date} 15:00:00' and '{end_date} 15:00:00' 
            {c_annot} AND ic.coupon_identifier IN {coupon_ids}
            {cg_annot} AND c.coupon_group_identifier IN {coupon_group_ids}
        GROUP BY 1,2,3,4,5
    """
    using_rate_by_date = rdb_pandas_extractor(
        db_connector=db_connector(**host_info),
        query=query
    )

    return using_rate_by_date






