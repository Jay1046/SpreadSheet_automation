
from configs import SPREADSHEET_INFORMATION, TRACKING_INDICES_INFORMATION, SLACK_TOKEN, SLACK_USER_IDS
from lib.aggregate_performance_indices import Aggregator
from lib.controll_spreadsheet import SpreadSheetController
import time
import datetime
from db.growth_slack_bot import GrowthSlackBot


class MainContoller:
    """
    스프래드시트 지표추적 자동화를 전체 컨트롤하는 역할을 수행합니다
    해당 클래스는 아래 2가지의 서브클래스를 포함합니다

    Aggregator : 데이터를 추출하고, 추출한 데이터를 병합하는 클래스입니다
    SpreadSheetController : 데이터를 스프래드시트에 기입하는 클래스입니다
    GrowthSlackBot : 슬랙봇입니다
    """
    def __init__(self, private_key_path, spread_sheet_url, slack_token):
        self.aggregator = Aggregator()
        self.sheet_controller = SpreadSheetController(
            private_key_path=private_key_path,
            spread_sheet_url=spread_sheet_url
        )
        self.slack_bot = GrowthSlackBot(token=slack_token)
        
        self.slack_bot.send_message(
            user_id=SLACK_USER_IDS["이재영"],
            text=f'{datetime.datetime.today().strftime("%Y-%m-%d")} 일자 지표 추적 시작'
        )
    
    def aggregate_and_insert(self, **kwargs):
        """
        각 역할을 수행하는 2개의 서브클래스의 동작에 필요한 여러 파라미터에 인자를 전달하여, 
        실제 동작을 수행하는 메소드 입니다
        모든 추적 지표들은 configs.py에 각 인자가 정리되어 있습니다.
        
        """

        if kwargs["type"] == "used_amount":
            used_amount = self.aggregator.aggregate_used_amount(
                start_date=kwargs["start_date"],
                end_date=kwargs["end_date"],
                coupon_ids=kwargs["coupon_ids"],
                host_info=kwargs["host_info"],
                transform_cac_form=kwargs["transform_cac_form"],
                coupon_kind=kwargs["coupon_kind"],
                coupon_group_ids=kwargs["coupon_group_ids"]
            )
            self.sheet_controller.track_used_amount_by_updating_each_cells(
                sheet_name=kwargs["sheet_name"],
                used_amount_df=used_amount
            )
        elif kwargs["type"] == "using_rate":
            using_rate = self.aggregator.aggregate_using_rate(
                start_date=kwargs["start_date"],
                end_date=kwargs["end_date"],
                coupon_ids=kwargs["coupon_ids"],
                host_info=kwargs["host_info"],
                transform_cac_form=kwargs["transform_cac_form"],
                coupon_kind=kwargs["coupon_kind"],
                coupon_group_ids=kwargs["coupon_group_ids"]
            )
        
            self.sheet_controller.track_using_rate_by_updating_batch_cells(
                sheet_name=kwargs["sheet_name"],
                using_rate_df=using_rate
            )
        elif kwargs["type"] == "first_funnel":
            first_order_info = self.aggregator.get_first_funnel_info(
                start_date=kwargs["start_date"],
                end_date=kwargs["end_date"]
            )
            self.sheet_controller.track_user_conversion_rate_by_updaing_batch_cells(
                sheet_name=kwargs["sheet_name"],
                conversion_rate_df=first_order_info
            )

        elif kwargs["type"] == "order_funnel":
            order_info = self.aggregator.get_order_funnel_info(
                start_date=kwargs["start_date"],
                end_date=kwargs["end_date"],
                n_funnel=kwargs["n_funnel"]
            )
            self.sheet_controller.track_user_conversion_rate_by_updaing_batch_cells(
                sheet_name=kwargs["sheet_name"],
                conversion_rate_df=order_info
            )

        elif kwargs["type"] == "naver_place":
            naver_used_amount = self.aggregator.aggregate_used_amount_naver_place(
                start_date=kwargs["start_date1"],
                end_date=kwargs["end_date1"],
                host_info=kwargs["host_info"]
            )
            self.sheet_controller.track_used_amount_by_updating_each_cells(
                sheet_name=kwargs["sheet_name"],
                used_amount_df=naver_used_amount
            )
            naver_using_rate = self.aggregator.aggregate_using_rate_naver_place(
                start_date=kwargs["start_date2"],
                end_date=kwargs["end_date2"],
                host_info=kwargs["host_info"]
            )
            self.sheet_controller.track_used_amount_by_updating_each_cells(
                sheet_name=kwargs["sheet_name"],
                used_amount_df=naver_using_rate
            )
            

        else:
            raise Exception(f"{kwargs['type']} is invalid type. please check your type")




if __name__ == "__main__":

    # 필요한 정보들 정의
    private_key_path = SPREADSHEET_INFORMATION["private_key_path"]
    track_dict = TRACKING_INDICES_INFORMATION
    spreadsheet_url = SPREADSHEET_INFORMATION["spread_sheet_url"]

    # 컨트롤러 객체 생성
    controller = MainContoller(
            private_key_path=private_key_path,
            spread_sheet_url=spreadsheet_url,
            slack_token=SLACK_TOKEN
        )
    

    for track_index, index_params in track_dict.items():

        try:
            print(f'Track <{track_index}> index started.. ')
            start_time = time.time()

            controller.aggregate_and_insert(**index_params)

            end_time = time.time()
            elapsed_time = end_time - start_time

            print(f"Track completed.. 실행 시간: {round(elapsed_time, 2)} sec")
            
            if "퍼널" in track_index:
                time.sleep(10)
            elif "매채홍" in track_index:
                time.sleep(5)
            else:
                time.sleep(3) 

        except Exception as e:
            controller.slack_bot.send_message(
                user_id=SLACK_USER_IDS["이재영"],
                text=f"""
                    {track_index} 작업에서 에러 발생. \n
                    Error : {e}
                """
            )



    