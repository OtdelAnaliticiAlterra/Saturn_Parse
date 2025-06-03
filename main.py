import aiohttp
import asyncio
from selectolax.parser import HTMLParser
import time
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv
from telegram_bot_logger import TgLogger
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(find_dotenv())

CHATS_IDS = '\\\\TG-Storage01\\Аналитический отдел\\Проекты\\Python\\chats_ids.csv'

logger = TgLogger(
    name='Парсинг_Сатурн',
    token=os.environ.get('LOGGER_BOT_TOKEN'),
    chats_ids_filename=CHATS_IDS,
)


async def get_response(session, url, retries=3):
    """Получение ответа от сервера с обработкой ошибок"""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=50) as response:
                response.raise_for_status()
                return await response.text()
        except (aiohttp.ClientTimeout, aiohttp.ClientError) as e:
            print(f"Network error occurred: {e}. Attempt {attempt + 1} of {retries}. Retrying...")
            await asyncio.sleep(2)
        except asyncio.TimeoutError:
            print(f"Timeout error occurred for URL: {url}. Attempt {attempt + 1} of {retries}. Retrying...")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"An unexpected error occurred while requesting {url}: {e}")
            break
    return None


async def parse_categories(session):
    """Парсинг категорий товаров"""
    response_text = await get_response(session, 'https://bar.saturn.net/catalog/')
    if response_text is not None:
        parser = HTMLParser(response_text)
        cat_links = [f'https://bar.saturn.net{categories.attributes.get("href")}' for categories in
                     parser.css('a.catalog__level1__list__item__link')]

        print(cat_links)
        return cat_links
    return []


async def parse_products(session):
    """Парсинг информации о товарах"""
    supply_path = await parse_categories(session)
    good_links = []
    product_links = []
    article_list = []
    name_list = []
    price_list = []
    dratel = []
    for elem in supply_path:
        print(f"elem = {elem}")
        response_text = await get_response(session, elem)
        if response_text is not None:
            parser = HTMLParser(response_text)
            page = [int(item.attributes.get("data-page")) for item in parser.css("li.pagination__item a")]

            max_page = int(max(page))

            for num in range(1, max_page + 1):
                response_text = await get_response(session, f"{elem}?&page={num}&per_page=20")
                parser = HTMLParser(response_text)
                for itm in parser.css("li.catalog_Level2__goods_list__item"):
                    dratel = []
                    if itm.css("div.goods_card_price_units_wrapper"):

                        for item in itm.css('div.goods_card_link link'):
                            good_links.append("https://bar.saturn.net/" + item.attributes.get("href"))
                            good_links.append("https://bar.saturn.net/" + item.attributes.get("href"))
                        for item in itm.css('div.goods_card_link meta'):
                            name_list.append(item.attributes.get("content"))
                            name_list.append(item.attributes.get("content"))
                        for drate in itm.css("div.goods_card_price_units_wrapper button"):
                            dratel.append(drate.attributes.get("data-rate"))
                        if itm.css('div.goods_card_footer div.goods_card_price_discount_value span'):
                            for item in itm.css('div.goods_card_footer div.goods_card_price_discount_value span'):
                                for rate in dratel:
                                    price_list.append(str(int(item.text().replace(' ', '')) * float(rate)))
                        else:
                            for item in itm.css('div.goods_card_footer div.goods_card_price_value span'):

                                for rate in dratel:
                                    if ',' in item.text():
                                        price_list.append(str(float(item.text().replace(',', '.')) * float(rate)))
                                    else:
                                        price_list.append(str(float(item.text().replace(' ', '')) * float(rate)))

                        for item in itm.css('div.goods_card_articul'):
                            article_list.append(item.text())
                            article_list.append(item.text())
                    else:
                        for item in itm.css('div.goods_card_link link'):
                            good_links.append("https://bar.saturn.net/" + item.attributes.get("href"))
                        for item in itm.css('div.goods_card_link meta'):
                            name_list.append(item.attributes.get("content"))
                        if itm.css('div.goods_card_footer div.goods_card_price_discount_value span'):
                            for item in itm.css('div.goods_card_footer div.goods_card_price_discount_value span'):
                                if ',' in item.text():
                                    price_list.append((item.text().replace(',', '.')))
                                else:
                                    price_list.append((item.text().replace(' ', '')))

                        else:
                            for item in itm.css('div.goods_card_footer div.goods_card_price_value span'):
                                if ',' in item.text():
                                    price_list.append((item.text().replace(',', '.')))
                                else:
                                    price_list.append((item.text().replace(' ', '')))

                        for item in itm.css('div.goods_card_articul'):
                            article_list.append(item.text())

    return good_links, article_list, name_list, price_list


async def main():
    start = time.time()
    async with aiohttp.ClientSession() as session:
        product_links, article_list, name_list, price_list = await parse_products(session)

        new_slovar = {
            "Код конкурента": "01-01028082",
            "Конкурент": "Сатурн",
            "Артикул": article_list,
            "Наименование": name_list,
            "Вид цены": "Цена СатурнБарнаул",
            "Цена": price_list,
            "Ссылка": product_links
        }
        df = pd.DataFrame(new_slovar)
        file_path = "\\\\tg-storage01\\Аналитический отдел\\Проекты\\Python\\Парсинг конкрунтов\\Выгрузки\\Сатурн\\Выгрузка цен.xlsx"

        if os.path.exists(file_path):
            os.remove(file_path)

        df.to_excel(file_path, sheet_name="Данные", index=False)
        print("Парсинг выполнен")
    end = time.time()
    print("Время", (end - start))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(e)
        raise e
