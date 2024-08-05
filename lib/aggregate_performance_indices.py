from lib.extract_performance_indices import get_used_amount_by_used_date_common, get_using_rate_by_got_date_common

import pandas as pd

from db.conn import DBConnector
from db.controller import rdb_pandas_extractor
from db.settings import DB_info
from datetime import datetime, timedelta

import warnings
warnings.filterwarnings(action="ignore")


class Aggregator:

    """
    데이터를 추출하고 병합을 진행하는 함수들을 모아놓은 클래스입니다
    각 용도에 따라서 해당 클래스에 update 및 append 
    """


    @staticmethod
    def aggregate_used_amount(
        start_date:str,
        end_date:str,
        coupon_ids,
        host_info,
        transform_cac_form,
        coupon_kind=None,
        coupon_group_ids=None
    ):
        """
        사용금액을 추출하고 병합
        """

        used_amount_by_date = get_used_amount_by_used_date_common(
            start_date=start_date,
            end_date=end_date,
            coupon_ids=coupon_ids,
            db_connector=DBConnector,
            host_info=host_info,
            coupon_kind=coupon_kind,
            coupon_group_ids=coupon_group_ids
        )

        used_amount_by_date["쿠폰종류"] = used_amount_by_date.apply(transform_cac_form, axis=1)

        pivoted_table = used_amount_by_date[used_amount_by_date["쿠폰종류"]!="이상치"]\
            .sort_values(by=["사용일자", "쿠폰종류"])\
                .pivot_table(
                    index="사용일자", 
                    columns="쿠폰종류", 
                    values="사용금액", 
                    aggfunc="sum"
                ).fillna(0)
        
        return pivoted_table


    @staticmethod
    def aggregate_using_rate(
            start_date:str,
            end_date:str,
            coupon_ids,
            host_info,
            transform_cac_form,
            coupon_kind=None,
            coupon_group_ids=None
    ):
        """
        사용내역을 추출하고 병합
        """
        using_rate_by_got_date = get_using_rate_by_got_date_common(
            start_date=start_date,
            end_date=end_date,
            coupon_ids=coupon_ids,
            db_connector=DBConnector,
            host_info=host_info,
            coupon_kind=coupon_kind,
            coupon_group_ids=coupon_group_ids
        )
        using_rate_by_got_date["쿠폰종류"] = using_rate_by_got_date.apply(transform_cac_form, axis=1)
        using_rate_by_got_date = using_rate_by_got_date[using_rate_by_got_date["쿠폰종류"]!="이상치"]\
                                    .groupby(["발급일자", "쿠폰종류"])[["발급쿠폰수", "발급유저수", "사용가능수", "사용쿠폰수", "사용유저수", "만료수"]].sum().reset_index()


        using_rate_by_got_date["쿠폰사용율"] = round(using_rate_by_got_date["사용쿠폰수"] / using_rate_by_got_date["발급쿠폰수"], 3)
        using_rate_by_got_date["쿠폰사용유저비율"] = round(using_rate_by_got_date["사용유저수"] / using_rate_by_got_date["발급유저수"], 3)

        using_rate_by_got_date_melted = using_rate_by_got_date.melt(id_vars=["발급일자", "쿠폰종류"], value_vars=["발급쿠폰수", "사용쿠폰수", "사용가능수", "쿠폰사용율", "쿠폰사용유저비율"])
        using_rate_by_got_date_pivot = using_rate_by_got_date_melted.pivot_table(
                                                                        index="발급일자", 
                                                                        columns=["쿠폰종류", "variable"], 
                                                                        values="value",
                                                                        aggfunc="sum"
                                                                    ).sort_index(axis=0, level=[0,1])                  
        return using_rate_by_got_date_pivot


    @staticmethod
    def get_first_funnel_info(start_date, end_date):
        start_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")
        query = f"""

        WITH target_users AS (
        
                SELECT identifier, created_date
                FROM "user" u
                WHERE 1=1
                    AND level =1
                    AND u.created_date BETWEEN '{start_date} 15:00:00' AND '{end_date} 14:59:59' 

        )

            SELECT 
                u.identifier 유저식별자, DATE(u.created_date + '9 hours') 가입일자, od.first_od AS 첫주문일자
            FROM target_users u
            LEFT JOIN (
                        SELECT od.user_identifier, DATE(MIN(od.created_date + '9 hours')) first_od
                        FROM "order" od
                        WHERE 1=1
                            AND od.user_identifier in (
                                select user_identifier
                                from target_users
                            )
                            AND od.kind = 0
                            AND od.status > 1
                        GROUP BY 1
            ) od on u.identifier = od.user_identifier
                
                
        """

        order_1 = rdb_pandas_extractor(
            db_connector=DBConnector(**DB_info["MONOLITHIC"]),
            query=query
        )

        order_1["첫주문경과일"] = (pd.to_datetime(order_1["첫주문일자"]) - pd.to_datetime(order_1["가입일자"])).dt.days
        first_order_info = order_1.groupby("가입일자")["유저식별자"].nunique().reset_index(name="가입자수")\
            .merge(order_1[order_1["첫주문경과일"] <=4].groupby("가입일자")["유저식별자"].nunique().reset_index(name="4일이내 첫주문자"))\
                .merge(order_1[order_1["첫주문경과일"] <=7].groupby("가입일자")["유저식별자"].nunique().reset_index(name="7일이내 첫주문자"))\
                    .merge(order_1[order_1["첫주문경과일"] <=10].groupby("가입일자")["유저식별자"].nunique().reset_index(name="10일이내 첫주문자"))

        return first_order_info.set_index("가입일자")



    @staticmethod
    def get_order_funnel_info(start_date, end_date, n_funnel): 
        
        start_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")

        query = f"""

            select user_identifier as 유저식별자,
                DATE(created_date + '9 hours') 주문일자,
                ROW_NUMBER() OVER (PARTITION BY user_identifier ORDER BY created_date ASC) as rank
            from "order"
            where 1=1
                and user_identifier in (
                        select user_identifier
                        from (
                                select user_identifier,
                                        created_date,
                                        ROW_NUMBER() OVER (PARTITION BY user_identifier ORDER BY created_date ASC) as rank
                                from "order"
                                where user_identifier in (
                                    select distinct user_identifier
                                    from "order"
                                    where created_date between '{start_date} 15:00:00' and '{end_date} 14:59:59'
                                        and kind = 0
                                        and status > 1
                                    )
                                    and kind = 0
                                    and status > 1
                            ) as temp
                        where 1=1
                            and temp.rank = {n_funnel}
                            and created_date between '{start_date} 15:00:00' and '{end_date} 14:59:59'
                        )
                and status > 1
                and kind = 0


        """
        print("start querying")
        orders = rdb_pandas_extractor(
            db_connector=DBConnector(**DB_info["MONOLITHIC"]),
            query=query
        )
        print("querying finished")
        orders["이전주문일"] = orders.groupby("유저식별자")["주문일자"].shift(1)
        orders["주문일차이"] = (pd.to_datetime(orders["주문일자"]) - pd.to_datetime(orders["이전주문일"])).dt.days    

        orders_merged = orders.merge(
            orders[orders["rank"]==n_funnel][["유저식별자", "주문일자"]].rename({"주문일자" :f"{n_funnel}번째주문일자"}, axis=1),
            on="유저식별자"
        )


        order_info = orders_merged.groupby(f"{n_funnel}번째주문일자")["유저식별자"].nunique().reset_index(name="주문자수")\
            .merge(orders_merged[(orders_merged["rank"] == n_funnel + 1)&(orders_merged["주문일차이"] <= 4)].groupby(f"{n_funnel}번째주문일자")["유저식별자"].nunique().reset_index(name=f"4일이내 {n_funnel + 1}회차 주문자수"))\
                .merge(orders_merged[(orders_merged["rank"] == n_funnel + 1)&(orders_merged["주문일차이"] <= 7)].groupby(f"{n_funnel}번째주문일자")["유저식별자"].nunique().reset_index(name=f"7일이내 {n_funnel + 1}회차 주문자수"))\
                    .merge(orders_merged[(orders_merged["rank"] == n_funnel + 1)&(orders_merged["주문일차이"] <= 10)].groupby(f"{n_funnel}번째주문일자")["유저식별자"].nunique().reset_index(name=f"10일이내 {n_funnel + 1}회차 주문자수"))\
                        .merge(orders_merged[(orders_merged["rank"] == n_funnel + 1)&(orders_merged["주문일차이"] <= 14)].groupby(f"{n_funnel}번째주문일자")["유저식별자"].nunique().reset_index(name=f"14일이내 {n_funnel + 1}회차 주문자수"))

        return order_info.set_index(f"{n_funnel}번째주문일자")


    @staticmethod
    def aggregate_used_amount_naver_place(
            start_date:str,
            end_date:str,
            host_info
    ):
        """
        매채홍_전화주문 사용금액 추적
        """
        start_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date_a_week_ago =  (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=8)).strftime("%Y-%m-%d")
        query = f"""

            SELECT
                DATE(puh.created_date + '9 hours') as 사용일자,
                sum(puh.used_point) AS 사용금액
            FROM point_used_history as puh
            WHERE 1=1
                and puh.accumulated_point_id IN (
                    select identifier from point where status = 11 and date between '{start_date_a_week_ago} 15:00:00' and '{end_date} 14:59:59'
                )
                and puh.created_date between '{start_date} 15:00:00' and '{end_date} 14:59:59'
            GROUP BY 1

        """
        used_amount_by_date = rdb_pandas_extractor(
            db_connector=DBConnector(**host_info),
            query=query
        )

        return used_amount_by_date.set_index("사용일자")







