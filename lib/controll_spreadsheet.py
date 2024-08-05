import gspread as gs
from gspread.utils import rowcol_to_a1

import warnings
warnings.filterwarnings(action="ignore")



class SpreadSheetController:
    """
    gspread 라이브러리를 통해, 스프래드시트에 접근 및 조작을 진행합니다
    각 데이터 형태에 따라, 다른 방식으로 기입을 진행하며 추가적인 조작방식이 필요할 시, 해당 클래스에 추가
    """
    def __init__(
        self,
        private_key_path,
        spread_sheet_url
    ):
        self.private_key = private_key_path
        self.google_service_account = gs.service_account(private_key_path)
        self.document = self.google_service_account.open_by_url(spread_sheet_url)



    def track_used_amount_by_updating_each_cells(
            self,
            sheet_name,
            used_amount_df
        ):
        """
        사용금액 데이터 형식에 대한 자동화
        """
        worksheet = self.document.worksheet(sheet_name)

        first_date_of_tracked_index = used_amount_df.sort_index(ascending=True).index[0].strftime("%Y-%m-%d")

        cell_of_date = worksheet.find(first_date_of_tracked_index)
        row_of_date_cell, _ = cell_of_date.row, cell_of_date.col


        for column in used_amount_df.columns:
            try:
                print(f"{column} in {sheet_name} started")
                cell_of_column = worksheet.find(column)
                _, col_of_column_cell = cell_of_column.row, cell_of_column.col
                target_cell_address = rowcol_to_a1(row_of_date_cell, col_of_column_cell)
                used_amount_array = used_amount_df[column].to_frame().fillna(0).values.tolist()

                worksheet.update(target_cell_address, used_amount_array)
                # print("update_completed")
            except Exception as e:
                print(f"{column} in {sheet_name} got error")
                print(f"error : {e}")
                continue

    def track_using_rate_by_updating_batch_cells(
            self, 
            sheet_name, 
            using_rate_df,
        ):
        worksheet = self.document.worksheet(sheet_name)
        first_date_of_tracked_index = using_rate_df.sort_index(ascending=True).index[0].strftime("%Y-%m-%d")

        cell_of_date = worksheet.find(first_date_of_tracked_index)
        row_of_date_cell, _ = cell_of_date.row, cell_of_date.col

        for column in using_rate_df.columns.get_level_values(0).unique().tolist():
            try:
                print(f"{column} in {sheet_name} started")
                cell_of_column = worksheet.find(column)
                _, col_of_column_cell = cell_of_column.row, cell_of_column.col
                target_cell_address = rowcol_to_a1(row_of_date_cell, col_of_column_cell)

                using_rate_array = using_rate_df[column].fillna(0).values.tolist()
            
                worksheet.update(target_cell_address, using_rate_array)
                # print(f"{column} in {sheet_name} update_completed")
            except Exception as e:
                print(f"{column} in {sheet_name} got error")
                print(f"error : {e}")
                continue



    def track_user_conversion_rate_by_updaing_batch_cells(
            self, 
            sheet_name, 
            conversion_rate_df
        ):
        worksheet = self.document.worksheet(sheet_name)

        try:
            print(f"{sheet_name} started")
            first_date_of_tracked_index = conversion_rate_df\
                                            .sort_index(ascending=True).index[0].strftime("%Y-%m-%d")
            conversion_rate_array = conversion_rate_df.fillna(0).values.tolist()

            cell_of_date = worksheet.find(first_date_of_tracked_index)
            row_of_cell, col_of_cell = cell_of_date.row, cell_of_date.col
            target_cell_address = rowcol_to_a1(row_of_cell, col_of_cell+1)


            worksheet.update(target_cell_address, conversion_rate_array)
            # print("update_completed")
        except Exception as e:
            print(f"{sheet_name} got error")
            print(f"error : {e}")
    

        