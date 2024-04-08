import datetime
import io
import logging
import os
import pathlib
import shutil
import smtplib
import socket
import urllib.parse
import uuid
import warnings
from sys import platform
from time import sleep
from typing import Any, Dict, List, Tuple, Union
from xml.dom import minidom

import pandas as pd
import requests
import win32com.client
import yaml
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

# При выходе нового юр.лица на рынок добавить ГТП и компанию в таблицу
# ses_gtp (237), а также указать настройки для нового ключа в файле
# settings.yaml в корне (логин, пароль, id ключа),
# дальше нужные папки и файлы по юр.лицам создадутся сами.

# Примерная логика работы:
# 1) Проверяем доступность внешнего интернета (пока отключена)
# но поидее нужна, т.к. если нет внешки, то ничего не уйдет
# 2) Проверка доступности почтового smtp сервера, чтобы
# выбрать основную или резервную почту.
# 3) Проверка доступности БД с прогнозом, если недоступна, то
# грузим прошлый прогноз из файла.
# Если БД доступна, то передается словарь из названий моделей
# и их id и пробуется загрузить их в порядке уменьшения точности.
# Если не загрузился ни один прогноз (ни одна модель на завтра не подготовилась),
# то скрипт уведомляет и закрывается.
# 4) Далее цикл по "генерации" и "потреблению", но потребление пока отключено,
# т.к. нет прогнозной модели. и надо бы объединить наверное датафреймы,
# или в конце работы перед проверкой с атс объединять, т.к. в мониторинге
# все сразу и генерация и потребление, или мониторинг разделить
# на генераццию и потребление.
# Заявки создаются в тех же папках что и раньше (share/cz).
# Отправка также пока по всем компаниям параллельно. Есть идея
# переделать на все ГТП сразу параллельно.
# 5) После запуска bat файлов идет сканирование папки с ЦЗ.
# Скрипт продолжается только если папка пустая.
# Хорошо бы как-то придумать мониторинг ошибок при работе батника.
# 6) После отправки ценовых ждем 5 минут
# (время настраивается в переменной TIMEOUT_BEFORE_CHECK_CZ ниже)
# перед запуском скачивания отчетов мониторинга с АТС.
# 7) Сверка объемов мониторинга и наших.
# Все запросы с повторными попытками в случае ошибки.
# Пока отчеты сохраняются в файлы, далее настроить
# отправку отличий в телеграм наверное надо.

# Задаем переменные
TIMEOUT_BEFORE_CHECK_CZ = 300
FORECAST_SOURCE_DICT_GEN = {
    "skm_LGBM_2024": 29,
    "skm_ecmwf": 27,
    "Open_Meteo": 20,
    "VisualCrossing": 18,
    "tomorrow_io": 22,
    "rp5_1da": 16,
    "cbr_rp5": 11,
}
FORECAST_SOURCE_DICT_CONS = {
    "skm_LGBM_2024": 35,
}
CERTSTORE = win32com.client.Dispatch("CAdESCOM.Store")
CERTSTORE.Open(2, "My", 0)
CLASS_TYPE = "REQ"
VERSION = "86"
MODIFICATION_CONSENT = "False"
INTEGRAL_TYPE = "0"
TARGET_DATE = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime(
    "%Y%m%d"
)
TARGET_DATE_FOR_ATS = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime(
    "%d.%m.%Y"
)
PHONE = "1"
BILATERAL_VOLUME = "0"
RD_PRIORITY_VOLUME = "0"
INTERVAL_NUMBER = "0"
PRICE = "0"
GOOGLE_HOST = "8.8.8.8"
GOOGLE_OPENPORT = 53
GOOGLE_TIMEOUT = 3


# Настройки для логера
if platform == "linux" or platform == "linux2":
    logging.basicConfig(
        filename="/var/log/log-execute/create_send_xml_cz.log.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - " "%(funcName)s: %(lineno)d - %(message)s"
        ),
    )
elif platform == "win32":
    logging.basicConfig(
        filename=f"{pathlib.Path(__file__).parent.absolute()}/create_send_xml_cz.log.txt",
        level=logging.INFO,
        format=(
            "%(asctime)s - %(levelname)s - " "%(funcName)s: %(lineno)d - %(message)s"
        ),
    )


# Загружаем yaml файл с настройками
with open(
    f"{pathlib.Path(__file__).parent.absolute()}/settings.yaml",
    "r",
    encoding="utf8",
) as yaml_file:
    settings = yaml.safe_load(yaml_file)
avsoltek_settings = pd.DataFrame(settings["avsoltek"])
greenrus_settings = pd.DataFrame(settings["greenrus"])
sunveter_settings = pd.DataFrame(settings["sunveter"])
telegram_settings = pd.DataFrame(settings["telegram"])
cz_path_settings = pd.DataFrame(settings["cz_path"])
basic_email_settings = pd.DataFrame(settings["basic_email_settings"])
reserve_email_settings = pd.DataFrame(settings["reserve_email_settings"])
sql_settings = pd.DataFrame(settings["sql_db"])


def telegram(i: int, text: str) -> None:
    """
    Функция отправки уведомлений в telegram на любое количество каналов
    Указать данные в yaml файле настроек.
    """
    try:
        msg = urllib.parse.quote(str(text))
        bot_token = str(telegram_settings.bot_token[i])
        channel_id = str(telegram_settings.channel_id[i])

        retry_strategy = Retry(
            total=3,
            status_forcelist=[101, 429, 500, 502, 503, 504],
            method_whitelist=["GET", "POST"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        http.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}",
            verify=False,
            timeout=10,
        )
    except Exception as err:
        print(f"create_xml: Ошибка при отправке в telegram - {err}")
        logging.error(f"create_xml: Ошибка при отправке в telegram - {err}")


def connection(i: int) -> Any:
    """
    Функция коннекта к базе Mysql.
    Для выбора базы задать порядковый номер числом! Начинается с 0!
    """
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    db_data = (
        f"mysql://{user_yaml}:{password_yaml}@{host_yaml}:{port_yaml}/{database_yaml}"
    )
    try:
        return create_engine(db_data).connect()
    except Exception:
        return False


def check_internet(host: str, port: int, timeout: int) -> bool:
    """
    Функция проверки доступности внешней сети путем
    проверки, доступен ли один из общедоступных DNS-серверов Google.

    Реализация взята с (там описано почему это интересный подход):
    (https://stackoverflow.com/questions/3764291/
     how-can-i-see-if-theres-an-available-and-active-network-connection-in-python/)
    """
    logging.info("create_xml: Старт функции проверки внешней сети.")
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        logging.info("create_xml: Финиш функции проверки внешней сети.")
        return True
    except socket.error as err:
        logging.info(f"create_xml: Ошибка проверки доступности сети - {err}.")
        logging.info("create_xml: Финиш функции проверки внешней сети.")
        return False


def check_smtp(e_mail_config: Dict[str, str]) -> bool:
    """
    Функция проверки доступности smtp сервера.
    """
    logging.info("create_xml: Старт функции проверки доступности smtp сервера.")
    host_email = e_mail_config["SMTPHost"]
    port_email = e_mail_config["SMTPPort"]
    user_email = e_mail_config["SMTPUser"]
    password_email = e_mail_config["SMTPPassword"]
    try:
        with smtplib.SMTP(host_email, port_email) as smtp:
            smtp.login(user_email, password_email)
            smtp.quit()
        logging.info("create_xml: Финиш функции проверки доступности smtp сервера.")
        return True
    except Exception:
        logging.info("create_xml: Финиш функции проверки доступности smtp сервера.")
        return False


def load_data_to_db(db_name: str, connect_id: int, dataframe: pd.DataFrame) -> None:
    """
    Функция записи датафрейма в базу.
    """
    telegram(1, "create_xml: Старт записи в БД.")
    logging.info("create_xml: Старт записи в БД.")

    dataframe = pd.DataFrame(dataframe)
    connection_skm = connection(connect_id)
    dataframe.to_sql(
        name=db_name,
        con=connection_skm,
        if_exists="append",
        index=False,
        chunksize=5000,
    )
    rows = len(dataframe)
    telegram(1, f"create_xml: записано в БД {rows} строк.")
    if len(dataframe.columns) > 5:
        telegram(0, f"create_xml: записано в БД {rows} строк.")
    logging.info(f"записано в БД {rows} строк.")
    telegram(1, "create_xml: Финиш записи в БД.")
    logging.info("create_xml: Финиш записи в БД.")


def load_data_from_db(
    db_name: str,
    col_from_database: List,
    connect_id: int,
    id_foreca: Union[int, None],
    gtp_type: Union[str, None],
) -> pd.DataFrame:
    """
    Функция загрузки датафрейма из базы.
    """
    telegram(1, "create_xml: Старт загрузки из БД.")
    logging.info("create_xml: Старт загрузки из БД.")

    list_col_database = ",".join(col_from_database)
    connection_db = connection(connect_id)
    if id_foreca is None:
        query = f"select {list_col_database} from {db_name};"
    else:
        query = (
            f"SELECT {list_col_database} FROM {db_name} WHERE id_foreca = "
            f"{id_foreca} AND gtp LIKE '{gtp_type}%%' AND (HOUR(load_time) < 15 "
            "AND DATE(load_time) = DATE_ADD(DATE(dt), INTERVAL -1 DAY)) AND "
            "DATE(dt) = DATE_ADD(CURDATE(), INTERVAL 1 DAY) ORDER BY gtp, dt;"
        )
    dataframe_from_db = pd.read_sql(sql=query, con=connection_db)

    telegram(1, "create_xml: Финиш загрузки из БД.")
    logging.info("create_xml: Финиш загрузки из БД.")
    return dataframe_from_db


def load_forecast_from_db(
    db_name: str,
    col_from_database: List,
    connect_id: int,
    forecast_source_dict: Dict,
    gtp_type: str,
) -> pd.DataFrame:
    """
    Функция загрузки прогноза из базы с добавлением названия компании.
    Перебирает прогнозы из словаря по порядку, на случай если
    какой-то не подготовился, то берется следующий.
    Расставлены по точности в порядке убывания.
    """
    telegram(1, "create_xml: Старт функции load_forecast_from_db.")
    logging.info("create_xml: Старт функции load_forecast_from_db.")
    for forecast_source, id_foreca in forecast_source_dict.items():
        logging.info(
            (
                f"create_xml: Пробую загрузить прогноз {forecast_source}"
                f" с id={id_foreca}"
            )
        )
        forecast_dataframe = load_data_from_db(
            db_name,
            col_from_database,
            connect_id,
            id_foreca,
            gtp_type,
        )
        if not forecast_dataframe.empty:
            telegram(
                1,
                (f"create_xml: Загружен прогноз {forecast_source}"),
            )
            logging.info(
                (f"create_xml: Загружен прогноз {forecast_source}"),
            )
            break
    else:
        telegram(
            1,
            (f"create_xml: Не найден ни один прогноз на завтра."),
        )
        logging.info(
            (f"create_xml: Не найден ни один прогноз на завтра."),
        )
        # прекращаем выполнение скрипта, т.к. если прогноза нет,
        # то и подавать нечего.
        # или вызвать загрузку из файла
        # return load_forecast_from_file()
        os._exit(1)

    forecast_dataframe["hour"] = pd.to_datetime(forecast_dataframe.dt.values).hour
    gtp_company_dataframe = load_data_from_db(
        "visualcrossing.ses_gtp",
        [
            "gtp",
            "company",
        ],
        0,
        None,
        None,
    )
    if gtp_type == "PVIE":
        # замена GVIE на PVIE для последующего merge
        # если гтп потребления
        gtp_company_dataframe["gtp"] = gtp_company_dataframe["gtp"].str.replace(
            "G", "P"
        )
    forecast_dataframe = forecast_dataframe.merge(
        gtp_company_dataframe,
        left_on=[
            "gtp",
        ],
        right_on=[
            "gtp",
        ],
        how="left",
    )
    forecast_dataframe["value"] = round(forecast_dataframe["value"] / 1000, 2)
    forecast_dataframe.value[forecast_dataframe.value == 0] = 0.1
    forecast_dataframe.to_csv(f"forecast_dataframe.csv")
    telegram(1, "create_xml: Финиш функции load_forecast_from_db.")
    logging.info("create_xml: Финиш функции load_forecast_from_db.")
    return forecast_dataframe


def load_forecast_from_file() -> pd.DataFrame:
    """
    Функция загрузки прогноза из csv файла
    на случай если база недоступна.
    Каждый день когда база доступна сохранятеся файл с
    прогнозными значениями и в случае отсутствия сети или
    при отправке не из рабочей сети, будет возможность загрузить
    значения из файла.
    """
    telegram(1, "create_xml: Старт функции load_forecast_from_file.")
    logging.info("create_xml: Старт функции load_forecast_from_file.")
    forecast_dataframe = pd.read_csv("forecast_dataframe.csv")
    telegram(1, "create_xml: Финиш функции load_forecast_from_file.")
    logging.info("create_xml: Финиш функции load_forecast_from_file.")
    return forecast_dataframe


def create_xml(
    class_type: str,
    version: str,
    direction: str,
    modification_consent: str,
    integral_type: str,
    target_date: str,
    sender: str,
    representator: str,
    phone: str,
    e_mail: str,
    company: str,
    gtp_code: str,
    bilateral_volume: str,
    rd_priority_volume: str,
    interval_number: str,
    tg_values: Dict[int, float],
    price: str,
    path_to_xml: str,
) -> None:
    """
    Функция создания XML файла ценовой заявки
    """
    logging.info(f"create_xml: Старт создания xml цз для гтп {gtp_code}.")
    # формирование названия файла
    if direction == "ask":
        filename = f"ASP_{company}_{gtp_code}_{target_date}.xml"
    if direction == "bid":
        filename = f"BSP_{company}_{gtp_code}_{target_date}.xml"

    local_id = str(int(datetime.datetime.now().timestamp()))
    now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Создание объекта XML
    root = minidom.Document()

    # Добавляем корневой элемент "message"
    root_element_message = root.createElement("message")
    root.appendChild(root_element_message)

    # Добавление атрибута "class"
    root_element_message.setAttribute("class", class_type)

    # Добавление атрибута "id"
    root_element_message.setAttribute("id", f"{{{str(uuid.uuid4()).upper()}}}")

    # Добавление атрибута "local id"
    root_element_message.setAttribute("local-id", local_id)

    # Добавление атрибута "version"
    root_element_message.setAttribute("version", version)

    root.appendChild(root_element_message)

    # Добавление элемента "request"
    child_element_request = root.createElement("request")

    # Добавление атрибута "direction" ("ask" - генерация, "bid" - потребление)
    child_element_request.setAttribute("direction", direction)

    # Добавление атрибута "created"
    child_element_request.setAttribute("created", now_time)

    # Добавление атрибута "last modified"
    child_element_request.setAttribute("last-modified", now_time)

    # Добавление атрибута "modification consent"
    # Добавляется только если заявки на генерацию
    if direction == "ask":
        child_element_request.setAttribute("modification-consent", modification_consent)

    # Добавление атрибута "integral"
    child_element_request.setAttribute("integral-type", integral_type)
    root_element_message.appendChild(child_element_request)

    # Добавление элемента "target-date" к "request"
    child_element_target_date = root.createElement("target-date")
    child_element_target_date.setAttribute("value", target_date)
    child_element_request.appendChild(child_element_target_date)

    # Добавление элемента "organization" к "request"
    child_element_organization = root.createElement("organization")
    child_element_request.appendChild(child_element_organization)

    # Добавление элемента "contacts" к "organization"
    child_element_contact = root.createElement("contacts")
    child_element_organization.appendChild(child_element_contact)

    # Добавление элемента "sender" к "contact"
    child_element_sender = root.createElement("sender")
    child_element_contact.appendChild(child_element_sender)
    child_element_sender_text = root.createTextNode(sender)
    child_element_sender.appendChild(child_element_sender_text)

    # Добавление элемента "rep" к "contact"
    child_element_representator = root.createElement("rep")
    child_element_contact.appendChild(child_element_representator)
    child_element_representator_text = root.createTextNode(representator)
    child_element_representator.appendChild(child_element_representator_text)

    # Добавление элемента "phone" к "contact"
    child_element_phone = root.createElement("phone")
    child_element_contact.appendChild(child_element_phone)
    child_element_phone_text = root.createTextNode(phone)
    child_element_phone.appendChild(child_element_phone_text)

    # Добавление элемента "e-mail" к "contact"
    child_element_email = root.createElement("e-mail")
    child_element_contact.appendChild(child_element_email)
    child_element_email_text = root.createTextNode(e_mail)
    child_element_email.appendChild(child_element_email_text)

    # code1 - юр.лицо
    # Добавление элемента "code1" к "organization"
    child_element_code1 = root.createElement("code1")
    child_element_organization.appendChild(child_element_code1)
    child_element_code1_text = root.createTextNode(company)
    child_element_code1.appendChild(child_element_code1_text)

    # code3 - код гтп
    # Добавление элемента "code3" к "organization"
    child_element_code3 = root.createElement("code3")
    child_element_organization.appendChild(child_element_code3)
    child_element_code3_text = root.createTextNode(gtp_code)
    child_element_code3.appendChild(child_element_code3_text)

    # Создание и добавление элемента "hourly-data" к "request"
    child_element_hourly_data = root.createElement("hourly-data")
    child_element_request.appendChild(child_element_hourly_data)

    # Добвление элементов с интервалами и ценами (24 часовых значения)
    for i in range(24):
        # Создание и добавление элемента "hour" к "hourly-data"
        child_element_hour = root.createElement("hour")
        child_element_hourly_data.appendChild(child_element_hour)
        child_element_hour.setAttribute("number", str(i))
        child_element_hour.setAttribute("bilateral-volume", bilateral_volume)
        child_element_hour.setAttribute("RD-priority-volume", rd_priority_volume)

        # Создание и добавление элемента "prices" к "hour"
        child_element_prices = root.createElement("prices")
        child_element_hour.appendChild(child_element_prices)

        # Создание и добавление элемента "intervals" к "prices"
        child_element_intervals = root.createElement("intervals")
        child_element_prices.appendChild(child_element_intervals)

        # Создание и добавление элемента "interval" к "intervals"
        child_element_interval = root.createElement("interval")
        child_element_interval.setAttribute("number", interval_number)
        child_element_intervals.appendChild(child_element_interval)

        # Создание и добавление элемента "high-value" к "interval"
        child_element_tg_volume = root.createElement("high-value")
        child_element_interval.appendChild(child_element_tg_volume)
        child_element_tg_volume_text = root.createTextNode(
            str(tg_values[i]).replace(".", ",")
        )
        child_element_tg_volume.appendChild(child_element_tg_volume_text)

        # Создание и добавление элемента "price" к "interval"
        child_element_price = root.createElement("price")
        child_element_interval.appendChild(child_element_price)
        child_element_price_text = root.createTextNode(price)
        child_element_price.appendChild(child_element_price_text)

    # toprettyxml Возвращает распечатанную версию документа.
    # indent задает строку отступа и по умолчанию используется табулятор.
    # При явном аргументе encoding результатом является
    # строка байтов в указанной кодировке.
    # Явное указание аргумента standalone приводит к добавлению
    # объявлений standalone document в пролог XML-документа.
    # Если значение установлено равным True, standalone="yes" добавляется,
    # в противном случае оно устанавливается равным "no".
    # xml_str = root.toprettyxml(indent="\t", encoding="windows-1251", standalone=False)
    xml_str = root.toprettyxml(encoding="windows-1251", standalone=False)

    # Запись xml в файл из бинарной строки
    # с проверкой есть ли папка с названием года и месяца в сетевой папке CZ
    if not os.path.exists(path_to_xml):
        os.makedirs(path_to_xml)
    with open(f"{path_to_xml}\{filename}", "wb") as f:
        f.write(xml_str)
    logging.info(f"create_xml: Финиш создания xml цз для гтп {gtp_code}.")


def create_config_and_bat(
    company: str,
    work_path: str,
    bat_file_name: str,
    e_mail_config: Dict[str, str],
    target_date: str,
    prefix_cz_file: str,
    move_cz_path: str,
    gtp_list: Tuple[str],
    mode: str,
    path_to_xml: str,
) -> None:
    """
    Функция создания CryptoSendMail.ini для cryptosendmail
    и самого bat файла.
    """
    telegram(1, f"create_xml: Старт функции создания ini и bat для {company}.")
    logging.info(f"create_xml: Старт функции создания ini и bat для {company}.")
    # проверяем ли папка work_path, если нет, то создаем
    if not os.path.exists(work_path):
        os.makedirs(work_path)
    # проверяем есть ли CryptoSendMail.exe в папке
    # если нет, то копируем из корня
    if not os.path.exists(f"{work_path}CryptoSendMail.exe"):
        shutil.copyfile(
            f"{pathlib.Path(__file__).parent.absolute()}/CryptoSendMail.exe",
            f"{work_path}CryptoSendMail.exe",
        )

    # создаем ini файл с конфигом для CryptoSendMail
    with open(f"{work_path}CryptoSendMail.ini", "w") as ini_file:
        ini_file.write("[Config]\n")
        for key, value in e_mail_config.items():
            ini_file.write(f"{key}={value}\n")

    # создаем bat файл для отправки ценовых заявок
    header_beg_tuple = (
        f"set sentdate={target_date}\n",
        f"set pMail={e_mail_config['SMTPHost']}\n",
        f"set pPort={e_mail_config['SMTPPort']}\n",
        f"set pTimeout={e_mail_config['SMTPTimeOut']}\n",
        f"set pUser={e_mail_config['SMTPUser']}\n",
        f"set pPassword={e_mail_config['SMTPPassword']}\n",
    )
    reserve_tuple = (
        "set ssl_mode=2\n",
        "set ssl_ver=auto\n",
        "set ssl_check_cert=N\n",
        "set ssl_check_cert_online=N\n",
    )
    header_end_tuple = (
        "set pCl=CryptoEnergyPro.log\n",
        f"set pSubj=ATS-Request:{prefix_cz_file}_{company}\n",
        f"set pPath={path_to_xml}\{prefix_cz_file}_{company}\n",
        f"set pPathMove={move_cz_path}\n",
        "set pSmtp_auth=Y\n",
        "set pS=Y\n",
        "set pE=Y\n\n",
    )

    with open(f"{work_path}{bat_file_name}", "w") as bat_file:
        for row in range(len(header_beg_tuple)):
            bat_file.write(header_beg_tuple[row])

        # раздел добавляется только при использовании резервной почты
        if mode == "reserve":
            for row in range(len(reserve_tuple)):
                bat_file.write(reserve_tuple[row])

        for row in range(len(header_end_tuple)):
            bat_file.write(header_end_tuple[row])

        # формируем список гтп со счетчиком
        p_index_list = []
        i = 0
        for gtp in range(len(gtp_list)):
            i += 1
            p_index_list.append(f"%p{str(i)}%")
            bat_file.write(f"set p{str(i)}={gtp_list[gtp]}\n")
        bat_file.write("\n\n\n\n\n\n")
        p_indexes = ", ".join(p_index_list)

        # конец батника
        footer_tuple = (
            f"FOR %%G IN ({p_indexes}) DO (Call :PCall %%G%)\n\n",
            "goto finish\n\n",
            ":pCall\n",
            "TIMEOUT /T 1 /NOBREAK\n",
            "set pGTP=%1\n\n",
            (
                "CryptoSendMail /i= /s=%pS% /e=%pE% /cs= /ce= /from= /to= "
                "/smtp_host=%pMail% /smtt_port=%pPort% /smtp_timeout=%pTimeout% "
                "/smtp_auth=%pSmtp_auth%  /smtp_user=%pUser% /smtp_password=%pPassword%"
                "  /cl=%pCl% /subj=%pSubj%_%pGTP%_%sentdate%  %pPath%_%pGTP%_%sentdate%.xml\n"
            ),
            "set Err1=%errorlevel%\n",
            "set ERROk=0\n",
            "IF %ERR1%==%ErrOk% move %pPath%_%pGTP%_%sentdate%.xml %pPathMove%\n\n",
            "exit /b\n\n",
            ":finish\n",
        )
        for row in range(len(footer_tuple)):
            bat_file.write(footer_tuple[row])
    telegram(1, f"create_xml: Финиш функции создания ini и bat для {company}.")
    logging.info(f"create_xml: Финиш функции создания ini и bat для {company}.")


def send_xml_cz_bat(work_path: str, bat_file_name: str, company: str) -> None:
    """
    Функция отправки ценовых заявок при помощи .bat файла.
    """
    telegram(1, f"create_xml: Старт функции отправки ценовых заявок {company}")
    logging.info(f"create_xml: Старт функции отправки ценовых заявок {company}")
    os.chdir(work_path)
    os.startfile(bat_file_name)


def dir_not_empty(dir_path: str) -> bool:
    """
    Функция проверки папки на наличие файлов.
    Возвращает True, если папка не пустая.
    """
    with os.scandir(dir_path) as iterator:
        if any(iterator):
            return True
    return False


def select_certificate(x509id: str) -> Union[Any, None]:
    """
    Функция выбора сертификата из хранилища по серийному номеру.
    """
    for i in range(1, CERTSTORE.Certificates.count + 1):
        if CERTSTORE.Certificates.Item(i).SerialNumber == x509id:
            return CERTSTORE.Certificates.Item(i)
    else:
        telegram(
            1,
            (f"create_xml: Не найден сертификат {x509id} в хранилище."),
        )
        logging.info(
            (f"create_xml: Не найден сертификат {x509id} в хранилище."),
        )


def ats_send_request(
    xmlhttp: Any,
    method: str,
    url: str,
    headers: Dict,
    option: Union[int, None],
    certificate: Union[str, None],
) -> Any:
    """
    Функция отправки запросов на сайт атс.
    """
    while True:
        if option is not None:
            xmlhttp.Option(option)
        xmlhttp.Open(method, url, False)
        if certificate is not None:
            xmlhttp.SetClientCertificate(certificate)
        for header, value in headers.items():
            xmlhttp.SetRequestHeader(header, value)
        xmlhttp.send
        if xmlhttp.Status == 200:
            return xmlhttp
        print(xmlhttp.Status)
        telegram(
            1,
            (f"create_xml: Неуспешный запрос на {url}.\nСтатус: {xmlhttp.Status}"),
        )
        logging.info(
            (f"create_xml: Неуспешный запрос на {url}.\nСтатус: {xmlhttp.Status}"),
        )
        sleep(5)


def ats_get_cookie(xmlhttp: Any) -> Tuple[str, Any]:
    """
    Функция получения cookies с сайта атс.
    """
    telegram(1, f"create_xml: Старт функции получения cookie с атс.")
    logging.info(f"create_xml: Старт функции получения cookie с атс.")
    url = "https://www.atsenergo.ru/auth"
    headers_auth = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "windows-1251,utf-8;q=0.7,*;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Connection": "keep-alive",
        "Origin": "https://www.atsenergo.ru",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36"
        ),
    }
    xmlhttp = ats_send_request(xmlhttp, "GET", url, headers_auth, None, None)
    cookie = xmlhttp.GetResponseHeader("Set-Cookie")
    telegram(1, f"create_xml: Финиш функции получения cookie с атс.")
    logging.info(f"create_xml: Финиш функции получения cookie с атс.")
    return cookie, xmlhttp


def ats_authorization(certificate, cookie: str, xmlhttp: Any) -> Any:
    """
    Функция авторизации по сертификату на сайте атс.
    """
    telegram(1, f"create_xml: Старт функции авторизации на атс.")
    logging.info(f"create_xml: Старт функции авторизации на атс.")
    url = "https://protected.atsenergo.ru/f800xx_reports/"
    headers_reports = {
        "Cookie": cookie,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "windows-1251,utf-8;q=0.7,*;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Connection": "keep-alive",
        "Origin": "https://www.atsenergo.ru",
        "Referer": "https://www.atsenergo.ru/nauth?access=personal",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36"
        ),
    }
    response = ats_send_request(xmlhttp, "POST", url, headers_reports, 6, certificate)
    telegram(1, f"create_xml: Финиш функции авторизации на атс.")
    logging.info(f"create_xml: Финиш функции авторизации на атс.")
    return response


def get_monitoring_report(
    company: str,
    target_date_for_ats: str,
    xmlhttp,
    cookie: str,
) -> pd.DataFrame:
    """
    Функция получения отчета с раздела мониторинга ценовых заявок.
    """
    telegram(
        1, f"create_xml: Старт функции получения отчета мониторинга {company} с атс."
    )
    logging.info(
        f"create_xml: Старт функции получения отчета мониторинга {company} с атс."
    )
    url = (
        "https://protected.atsenergo.ru/bids-monitoring/"
        f"zxweb.report.gtp_status.form.do?str_trader_code={company}&"
        f"dt_begin_date={target_date_for_ats}&dt_end_date={target_date_for_ats}"
        "&gtp_group_id=-1"
    )
    headers_report = {
        "Cookie": cookie,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "windows-1251,utf-8;q=0.7,*;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "If-Modified-Since": "Sat, 1 Jan 2000 00:00:00 GMT",
        "Connection": "keep-alive",
        "Origin": "https://www.atsenergo.ru",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36"
        ),
    }
    response = ats_send_request(xmlhttp, "GET", url, headers_report, None, None)

    report_temp = pd.read_excel(io.BytesIO(response.ResponseBody))
    report_temp.columns = [
        "gtp",
        "name_gtp",
        "operational_date",
        "cz_status",
        "gtp_status",
        "cz_number",
        "total_volume",
    ]
    # report_temp.columns = [
    #     "Код ГТП",
    #     "Наименование ГТП",
    #     "Дата",
    #     "Статус заявки",
    #     "Статус ГТП",
    #     "Номер заявки",
    #     "Суммарный объем, МВт*ч",
    # ]
    drop_index = int(report_temp[report_temp["gtp"] == "Код ГТП"].index[0]) + 1
    report_temp = report_temp[drop_index:]
    report_temp.reset_index(drop=True, inplace=True)
    telegram(
        1, f"create_xml: Финиш функции получения отчета мониторинга {company} с атс."
    )
    logging.info(
        f"create_xml: Финиш функции получения отчета мониторинга {company} с атс."
    )
    return report_temp


def compare_day_volumes(
    report_temp: pd.DataFrame,
    company_dataframe: pd.DataFrame,
    company: str,
    operational_date: str,
) -> None:
    """
    Функция сравнения суточных объемов по ГТП (отправленных и принятых).
    """
    telegram(1, f"create_xml: Старт функции сравнения объемов {company}.")
    logging.info(f"create_xml: Старт функции сравнения объемов {company}.")
    # company_dataframe получаем суммарный объем за сутки
    # company_dataframe["value"] = round(company_dataframe["value"] / 1000, 2)
    company_dataframe = company_dataframe.groupby("gtp")["value"].sum().reset_index()

    report_temp = report_temp.merge(
        company_dataframe,
        left_on=[
            "gtp",
        ],
        right_on=[
            "gtp",
        ],
        how="left",
    )
    print(report_temp)
    # report_temp.to_csv(f"bids_monitoring_{company}_{operational_date}.csv")
    report_temp.to_excel(f"bids_monitoring_{company}_{operational_date}.xlsx")
    telegram(1, f"create_xml: Финиш функции сравнения объемов {company}.")
    logging.info(f"create_xml: Финиш функции сравнения объемов {company}.")


if __name__ == "__main__":
    # Замер времени выполнения начало
    start_time = datetime.datetime.now()
    print(start_time)

    # internet_is_on = check_internet(GOOGLE_HOST, GOOGLE_OPENPORT, GOOGLE_TIMEOUT)
    work_smtp_available = check_smtp(basic_email_settings.config[0])
    check_db_connection = connection(1)

    if work_smtp_available:
        MODE = "basic"
        E_MAIL = str(basic_email_settings.e_mail[0])
        E_MAIL_CONFIG = basic_email_settings.config[0]
    else:
        MODE = "reserve"
        E_MAIL = str(reserve_email_settings.e_mail[0])
        E_MAIL_CONFIG = reserve_email_settings.config[0]

    # for cz_type in ("consumption", "generation"):
    for cz_type in ("generation",):
        if cz_type == "generation":
            GTP_TYPE = "GVIE"
            DIRECTION = "ask"
            PREFIX_CZ_FILE = "ASP"
            FORECAST_SOURCE_DICT = FORECAST_SOURCE_DICT_GEN
        if cz_type == "consumption":
            GTP_TYPE = "PVIE"
            DIRECTION = "bid"
            PREFIX_CZ_FILE = "BSP"
            FORECAST_SOURCE_DICT = FORECAST_SOURCE_DICT_CONS

        CREATE_CZ_PATH = cz_path_settings.create_cz_path[0]
        MOVE_CZ_PATH = cz_path_settings.move_cz_path[0]
        PATH_TO_XML = f"{CREATE_CZ_PATH}{TARGET_DATE[0:4]}\{TARGET_DATE[4:6]}"

        if check_db_connection is not False:
            FORECAST_DATAFRAME = load_forecast_from_db(
                "treid_03.weather_foreca",
                ["gtp", "dt", "load_time", "value"],
                1,
                FORECAST_SOURCE_DICT,
                GTP_TYPE,
            )
        else:
            FORECAST_DATAFRAME = load_forecast_from_file()
        print(FORECAST_DATAFRAME)
        # создаем список уникальных компаний из датафрейма
        # чтобы каждый раз потом не перебирать датафрейм
        LIST_OF_COMPANIES = FORECAST_DATAFRAME.company.unique().tolist()
        # создание пустого словаря под сертификаты
        CERTIFICATES_DICT = {}
        for COMPANY in LIST_OF_COMPANIES:
            # берем серийный номер сертификата для компании
            X509ID = str(globals()[f"{COMPANY.lower()}_settings"].x509id[0])
            # находим сертификат в хранилище
            CERTIFICATE_ITEM = select_certificate(X509ID)
            # получаем инфо о владельце сертификата из хранилища
            CERTIFICATE = CERTIFICATE_ITEM.GetInfo(0)
            # получаем отпечаток сертификата
            THUMBPRINT_CERT = CERTIFICATE_ITEM.Thumbprint
            # создаем словарь с сертификатами, чтобы при проверке
            # отчетов с атс второй раз не искать сертификаты
            CERTIFICATES_DICT.setdefault(
                COMPANY,
                {
                    "CERTIFICATE": CERTIFICATE,
                    "THUMBPRINT_CERT": THUMBPRINT_CERT,
                },
            )
        print(CERTIFICATES_DICT)

        for COMPANY in LIST_OF_COMPANIES:
            company_dataframe = pd.DataFrame(
                FORECAST_DATAFRAME.loc[FORECAST_DATAFRAME.company == COMPANY]
            )
            GTP_LIST = company_dataframe.gtp.unique()

            WORK_PATH = f"{pathlib.Path(__file__).parent.absolute()}/CZ/{COMPANY}/"
            BAT_FILE_NAME = f"!Отправить_ценовые_заявки_{COMPANY}!.bat"

            LOGIN = str(globals()[f"{COMPANY.lower()}_settings"].login[0])
            PASSWORD = str(globals()[f"{COMPANY.lower()}_settings"].password[0])
            SENDER = str(globals()[f"{COMPANY.lower()}_settings"].sender[0])
            REPRESENTATOR = str(globals()[f"{COMPANY.lower()}_settings"].sender[0])

            for GTP_CODE in company_dataframe.gtp.value_counts().index:
                gtp_dataframe = pd.DataFrame(
                    company_dataframe.loc[company_dataframe.gtp == GTP_CODE]
                )
                TG_VALUES = dict(zip(gtp_dataframe.hour, gtp_dataframe.value))

                create_xml(
                    CLASS_TYPE,
                    VERSION,
                    DIRECTION,
                    MODIFICATION_CONSENT,
                    INTEGRAL_TYPE,
                    TARGET_DATE,
                    SENDER,
                    REPRESENTATOR,
                    PHONE,
                    E_MAIL,
                    COMPANY,
                    GTP_CODE,
                    BILATERAL_VOLUME,
                    RD_PRIORITY_VOLUME,
                    INTERVAL_NUMBER,
                    TG_VALUES,
                    PRICE,
                    PATH_TO_XML,
                )

            # добавляем отпечаток в конфиг для ini файла
            E_MAIL_CONFIG["CertSign"] = str(
                CERTIFICATES_DICT[COMPANY]["THUMBPRINT_CERT"]
            ).lower()
            create_config_and_bat(
                COMPANY,
                WORK_PATH,
                BAT_FILE_NAME,
                E_MAIL_CONFIG,
                TARGET_DATE,
                PREFIX_CZ_FILE,
                MOVE_CZ_PATH,
                GTP_LIST,
                MODE,
                PATH_TO_XML,
            )

            send_xml_cz_bat(WORK_PATH, BAT_FILE_NAME, COMPANY)

        # проверка на наличие файлов в папке с ценовыми заявками
        # необходима чтобы batники не закрывались пока не отправятся все ценовые
        # иначе python запускает функцию и идет дальше и скрипт полностью
        # завершается ещё до полной отправки.
        while dir_not_empty(PATH_TO_XML):
            sleep(5)
        telegram(1, f"create_xml: Ценовые заявки отправлены.")
        logging.info(f"create_xml: Ценовые заявки отправлены.")

    # ждем 5 минут после отправки ценовых
    # чтобы они успели приняться и появиться в мониторинге на сайте атс
    sleep(TIMEOUT_BEFORE_CHECK_CZ)
    for COMPANY in LIST_OF_COMPANIES:
        # создание нового экземпляра WinHTTP.WinHTTPRequest.5.1
        XMLHTTP = win32com.client.Dispatch("WinHTTP.WinHTTPRequest.5.1")
        COMPANY_DATAFRAME = pd.DataFrame(
            FORECAST_DATAFRAME.loc[FORECAST_DATAFRAME.company == COMPANY]
        )
        # получаем инфо о владельце сертификата из словаря
        CERTIFICATE = CERTIFICATES_DICT[COMPANY]["CERTIFICATE"]
        # получаем первичное cookie при заходе на сайт атс
        COOKIE, XMLHTTP = ats_get_cookie(XMLHTTP)
        # авторизуемся по сертификату на сайте атс
        XMLHTTP = ats_authorization(CERTIFICATE, COOKIE, XMLHTTP)
        # получаем отчет из мониторинга ценовых заявок с сайта атс
        REPORT_TEMP = get_monitoring_report(
            COMPANY, TARGET_DATE_FOR_ATS, XMLHTTP, COOKIE
        )
        # и сверяем его с нашими прогнозными объемами
        compare_day_volumes(
            REPORT_TEMP, COMPANY_DATAFRAME, COMPANY, TARGET_DATE_FOR_ATS
        )

    # Замер времени выполнения конец
    end_time = datetime.datetime.now()
    delta = end_time - start_time
    print(end_time)
    print(delta)

