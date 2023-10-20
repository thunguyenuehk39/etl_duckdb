from enum import Enum, unique
import pandas as pd
import datetime


@unique
class CafefPriceHistoryTab(Enum):
    HISTORY = ("Price History", "Lịch sử giá", 1)
    ORDER = ("Order Statistics", "Thống kê đặt lệnh", 2)
    FOREIGN_TRANSACTION = ("Foreign Transactiosn", "Giao dịch nước ngoài", 3)

    def get_name(self, language: str = "en"):
        posi = 0 if language == "en" or language is None else 1
        return self.value[posi]

    def get_tab_number(self):
        return self.value[2]


@unique
class CafeFMarketSelectors(Enum):
    HOSE = "HOSE"
    HASTC = "HASTC"
    UPCOM = "UPCOM"
    VN30 = "VN30"


def cafef_market_depth(
    market: str = None, date: datetime.date = None
) -> tuple[pd.DataFrame, dict]:
    """Get Market Information from CafeF

    Args:
        market (str): Market Selectors, choose one of ['HOSE', 'HASTC', 'UPCOM', 'VN30'].
        date (datetime.date): Target date to get

    Returns:
        tuple of
        [1] pd.DataFrame: Data got from the market
        [2] dict: Error dictionary got from runtime.

    Note:
    1) The CafeF is not the mainstream data and with a lot of falsy state data
    2) The data can be delayed with 1 or 2 trading date, which we can't control or handle.
    So, we used cafef likely like a source to reference check.

    Usages:
    # Get data from https://s.cafef.vn/TraCuuLichSu2/1/HOSE/13/04/2022.chn
    >>> market = 'HOSE'
    >>> date = datetime.date.fromisoformat('2022-04-13')
    >>> cafef_market_depth(market=market, date=date)
    """
    assert date is not None, ValueError("Date can't not None")
    SUPPORTED_MARKETS = [p for p, x in CafeFMarketSelectors.__members__.items()]
    assert market.upper() in SUPPORTED_MARKETS, ValueError(
        f"Market is not supported, found [{market}], not in [{', '.join(SUPPORTED_MARKETS)}]"
    )

    errors = {}
    tabu = pd.DataFrame([])
    tab = CafefPriceHistoryTab.HISTORY.get_tab_number()
    # url = __CAFEF_API__.make_url(
    #     path=f"/TraCuuLichSu2/{tab}/{market}/{date.strftime('%d/%m/%Y')}.chn"
    # )

    url = f"https://s.cafef.vn/TraCuuLichSu2/{tab}/{market}/{date.strftime('%d/%m/%Y')}.chn"

    try:
        tabu = pd.read_html(url, attrs={"id": "table2sort"})[0]
    except Exception:
        errors.update(
            {
                datetime.datetime.now().strftime("%Y%m%d%H%M%S"): {
                    "error": "requests",
                    "url": url,
                }
            }
        )
        raise

    columns_mapping = {
        0: "ticker",
        1: "close",
        2: "change",
        3: "status",
        4: "reference",
        5: "open",
        6: "high",
        7: "low",
        8: "volume",
        9: "amount",
        10: "put_through_volume",
        11: "put_through_amount",
    }
    if market.upper() in ['HASTC', 'UPCOM']:
        columns_mapping = {
            0: "ticker",
            1: "close",
            2: "average",
            3: "change",
            4: "status",
            5: "reference",
            6: "open",
            7: "high",
            8: "low",
            9: "volume",
            10: "amount",
            11: "put_through_volume",
            12: "put_through_amount",
        }

    if tabu.empty is False:
        if len(tabu.columns) != 12 and len(tabu.columns) != 13:
            mes = (
                f"Number of columns not in deserved expected. It different in [12, 13] columns, "
                f"received tota of {tabu.shape[1]} columns"
            )
            raise Exception(mes)
        tabu.rename(
            mapper=columns_mapping,
            axis="columns",
            inplace=True,
        )
        for _ in tabu.columns:
            tabu[_] = tabu[_].apply(lambda x: x if x not in ["NaN", "&nbsp"] else None)
        tabu = tabu.query("ticker.notnull()")
        tabu = tabu.drop(columns=["change", "status"])
        for _ in ["open", "high", "low", "close", "reference"]:
            tabu[_] = tabu[_].apply(lambda x: float(x) if x is not None else None)
        for _ in ["volume", "amount", "put_through_volume", "put_through_amount"]:
            tabu[_] = tabu[_].apply(lambda x: int(float(x)) if x is not None else None)
        tabu['market'] = market
        tabu['date'] = date
        tabu['reference_link'] = url
        tabu['language'] = 'EN'

    return tabu, errors
