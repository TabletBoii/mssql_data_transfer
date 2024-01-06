import os
import logging
import pyodbc

from datetime import datetime
from config import get_data
from dotenv import load_dotenv
from utils import return_root_path

# 2000 - вис

"""
    January - 31
    February - 28/29
    March - 31
    April - 30
    May - 31
    June - 30
    July - 31
    August - 31
    September - 30
    October - 31
    November - 30
    December - 31
"""


class Initialize:
    def __init__(self):
        self.__kompas_data = []
        self.__current_date = int(datetime.now().strftime("%Y"))
        self.__date_interval_list = self.__initialize_date_interval_list()
        self.__odbc_driver = "ODBC Driver 17 for SQL Server"

    @property
    def kompas_data(self) -> list:
        return self.__kompas_data

    @property
    def current_date(self):
        return self.__current_date

    @property
    def get_date_interval_list(self):
        return self.__date_interval_list

    @staticmethod
    def __create_batches(lst, batch_size):
        for i in range(0, len(lst), batch_size):
            yield lst[i:i + batch_size]

    def __execute_query(self, query: list[str] | str, result_list: list, credentials: tuple, *params) -> list | None:
        query_index = 0
        try:
            with pyodbc.connect(
                    f"DRIVER={self.__odbc_driver};SERVER={credentials[0]};DATABASE={credentials[3]};UID={credentials[1]};PWD={credentials[2]}") as db_conn:
                logging.info(f"{credentials[0]} server connection established")
                with db_conn.cursor() as cursor:
                    logging.info(f"{credentials[0]} cursor opened")
                    if isinstance(query, str):
                        cursor.execute(query)
                        db_conn.commit()
                        logging.info("non-parameterized query completed")
                        return
                    for single_query in query:
                        query_index += 1
                        logging.info(f"Query {query_index} processing")
                        cursor.execute(single_query, params)
                        rows = cursor.fetchall()
                        for row in rows:
                            result_list.append(row)
                        logging.info(f"Query {query_index} completed")
                    db_conn.commit()
        except Exception as exp:
            logging.error(exp)
            return
        logging.info(f"{credentials[0]} connection closed")
        return result_list

    def __execute_insert_query(self, query: str, insert_list: list, credentials: tuple):
        try:
            with pyodbc.connect(
                    f"DRIVER={self.__odbc_driver};SERVER={credentials[0]};DATABASE={credentials[3]};UID={credentials[1]};PWD={credentials[2]}") as db_conn:
                logging.info(f"{credentials[0]} server connection established")
                with db_conn.cursor() as cursor:
                    logging.info(f"{credentials[0]} cursor opened")
                    cursor.fast_executemany = True
                    cursor.executemany(query, insert_list)
                    db_conn.commit()
                    logging.info("Batch committed successfully")
        except Exception as exp:
            logging.error(exp)
            return
        logging.info(f"{credentials[0]} connection closed")

    def __initialize_date_interval_list(self) -> list:
        date_interval_list = []
        datebeg_from = self.__current_date - 2
        logging.info(f"date_interval_list processing")
        for year in range(datebeg_from, self.__current_date):
            for month in range(1, 13):
                str_month = month if month // 10 >= 1 else "0" + str(month)
                if month in (1, 3, 5, 7, 8, 10, 12):
                    date_interval_list.append([f"{year}{str_month}01", f"{year}{str_month}31"])
                elif month in (4, 6, 9, 11):
                    date_interval_list.append([f"{year}{str_month}01", f"{year}{str_month}30"])
                elif (year - 2000) % 4 == 0 and month == 2:
                    date_interval_list.append([f"{year}{str_month}01", f"{year}{str_month}29"])
                else:
                    date_interval_list.append([f"{year}{str_month}01", f"{year}{str_month}28"])
        logging.info(f"date_interval_list processing completed")
        return date_interval_list

    def __is_db_connection_established(self) -> bool:
        for credentials in (ONE_C_DB_CREDENTIALS, KOMPAS_DB_CREDENTIALS):
            try:
                with pyodbc.connect(
                        f"DRIVER={self.__odbc_driver};SERVER={credentials[0]};DATABASE={credentials[3]};UID={credentials[1]};PWD={credentials[2]}", timeout=5) as db_conn:
                    with db_conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        logging.info(f"{credentials[0]} db connection established")
                        break
            except Exception as e:
                logging.error(f"\n{credentials[0]} db no connection" + "\n Tip: turn on vpn or check credentials")
                logging.error(e)
                return False
        return True

    def fetch_kompas_data(self, credentials: tuple):
        queries = []
        for date_interval in self.__date_interval_list:
            logging.info(f"Formation of a query {date_interval}")
            queries.append(f"""
                SET NOCOUNT ON;
                EXEC	[dbo].[up_claim_info_KOMPAS]
                @datebeg_from = N'{date_interval[0]}',
                @datebeg_till = N'{date_interval[1]}'
            """)
        self.__execute_query(
            query=queries,
            result_list=self.__kompas_data,
            credentials=credentials,
        )

    def __test_fetch_kompas_data(self, credentials: tuple):
        queries = []
        length = len(self.__date_interval_list)
        for index in range(length - 1, length):
            logging.info(f"Formation of a query {self.__date_interval_list[index]}")
            queries.append(f"""
                SET NOCOUNT ON;
                EXEC	[dbo].[up_claim_info_KOMPAS]
                @datebeg_from = N'{self.__date_interval_list[index][0]}',
                @datebeg_till = N'{self.__date_interval_list[index][1]}'
            """)
        self.__execute_query(
            query=queries,
            result_list=self.__kompas_data,
            credentials=credentials
        )

    def __clear_temp_table(self):
        query = """
            TRUNCATE TABLE dbo.two_years_old_samotour_data;
        """
        self.__execute_query(
            query=query,
            result_list=[],
            credentials=ONE_C_DB_CREDENTIALS
        )

    def __insert_data_to_1c_db(self):
        query = """INSERT INTO dbo.two_years_old_samotour_data(
            claim$inc,
            claim$id,
            claim$status,
            claim$paidstatus,
            partner$inc,
            partner$name,
            partner$lname,
            partner$officialname,
            partner$phprefix,
            partner$phones,
            partner$phones1,
            partner$phones2,
            partner$faxes,
            partner$faxes1,      
            partner$email,
            partner$email1,  
            partner$town$name,
            tour$name,
            claim$cdate,
            claim$cdatetime,
            claim$datebeg,
            claim$dateend,
            claim$nights,
            claim$confirmeddate,
            claim$cost,
            claim$amount,
            claim$paid, 
            claim$debt,
            claim$currency$alias,
            claim$note,
            claim$comment,
            claim$privatecomment,
            claim$partnercomment,
            partner$parttype$name,
            current$date,
            supervisor$inc,
            supervisor$name,
            town$inc,
            town$name,
            confimredstatus$inc,
            confimredstatus$name,
            pax,
            user$name,
            commission$percent,
            total_commiss,
            ctype$inc,
            ctype$name,
            state$inc,
            state$name,
            statefrom$inc,
            statefrom$name,
            adl_count,
            chd_count,
            pax_count,
            departure$town$inc,
            departure$town$name,
            departure$state$inc,
            departure$state$name,
            owner$inc,
            owner$name,
            owner$lname,
            owner$officialname
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        self.__execute_insert_query(query=query, insert_list=self.__kompas_data, credentials=ONE_C_DB_CREDENTIALS)

    def run(self):
        is_db_connectable = self.__is_db_connection_established()
        if not is_db_connectable:
            return
        logging.info("Script activated")
        self.fetch_kompas_data(credentials=KOMPAS_DB_CREDENTIALS)
        self.__clear_temp_table()
        self.__insert_data_to_1c_db()

    def test(self):
        # self.__test_fetch_kompas_data(credentials=KOMPAS_DB_CREDENTIALS)
        self.__clear_temp_table()
        # self.__insert_data_to_1c_db()


if __name__ == '__main__':
    root_path = return_root_path()
    log_file_name = datetime.now().strftime("logfile_%H_%M_%S_%d_%m_%Y.log")
    logs_path = os.path.join(root_path, "logs", log_file_name)

    logging.basicConfig(
        level=logging.INFO,
        filename=logs_path,
        filemode="w",
        format="%(asctime)s %(levelname)s %(message)s"
    )

    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")

    load_dotenv(
        dotenv_path=dotenv_path
    )

    KOMPAS_DB_SERVER = get_data("KOMPAS_DB_SERVER")
    KOMPAS_DB_USERNAME = get_data("KOMPAS_DB_USERNAME")
    KOMPAS_DB_PASSWORD = get_data("KOMPAS_DB_PASSWORD")
    KOMPAS_DB_NAME = get_data("KOMPAS_DB_NAME")

    ONE_C_DB_SERVER = get_data("1C_DB_SERVER")
    ONE_C_DB_USERNAME = get_data("1C_DB_USERNAME")
    ONE_C_DB_PASSWORD = get_data("1C_DB_PASSWORD")
    ONE_C_DB_NAME = get_data("1C_DB_NAME")

    ONE_C_DB_CREDENTIALS = (
        ONE_C_DB_SERVER,
        ONE_C_DB_USERNAME,
        ONE_C_DB_PASSWORD,
        ONE_C_DB_NAME,
    )

    KOMPAS_DB_CREDENTIALS = (
        KOMPAS_DB_SERVER,
        KOMPAS_DB_USERNAME,
        KOMPAS_DB_PASSWORD,
        KOMPAS_DB_NAME,
    )

    initialize_instance = Initialize()
    initialize_instance.run()
    # initialize_instance.test()
