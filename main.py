import asyncio
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import aiohttp
import sys

lock = asyncio.Lock()

dir = rf"C:\Users\zakhr\OneDrive\Документы\addresses"

url = "https://ladbsdoc.lacity.org/IDISPublic_Records_test/idis/DocumentSearch.aspx?SearchType=DCMT_ADDR"

cookies = {'ASP.NET_SessionID': '3ftqdsbhpzjl12fczsu4ikfq',
           'bm_sv': '8BDD51118038A41D086EB4E7A7B8D4E0~YAAQpxc51SE1zmCLAQAA9YJvYxXLDTBp4SEO3iJm6r+CL6WsZ92abGcerqeavThA2MWfN/PHAf387DNjZ3J0CQ9HBco00DVQS96FmMjPx+TyMIzV7tTzJwYehdy/iepeJUleV1bae+1OGsmQmHIuCqWqe+By3M3Z+kwRloah00ChGSgI+a/Uf39iIRFDsdonkOxxVFe4ORY7ttV+niUpQ7E2LaOFXBvJi8hSa323FMuJeQU2hOaLnEOXChJC7Gmtyw==~1',
           '_ga': 'GA1.1.1158830629.1698133662',
           '_ga_ZKT0DX5HRB': 'GS1.1.1698180171.3.1.1698180337.0.0.0',
           'ak_bmsc':"D5AC4316CEE1F397535B378366629A75~000000000000000000000000000000~YAAQpxc51foqzmCLAQAACABtYxXPSRWaKQ85gLesdXdXYhY4nfLVh5XAFguOWxct0A06d2FId9G8ogEh62WEDAvZNS+VJkS7ynUiSF/+E2I74YTRQQpg6lu7Io6/fArH9o/IK67XIbb9DxHPYoN0Yy9dBIBuP9VPdO0kr4cx0rsU9nvxEztpoNcN5Xm/NQyWXlQ9lsoYq8yYw09Jb0a5Uj5/PJGyGEcxIeSwXDH7dZrdRYC0yVeLvf53x6jjC8mgZE2vHWjY+qjoRwR5l8ncwdXFvs1XfauQTb9aykogne34N1p2FMQZ3dWyxW/NctPlIk9KD0JZ+MmN2fGm3d851HBo7A2bMgos5v0G36Da0yePHX5f0b56w+8qoLxsdplFVRkf4SjzShIrIWt1SgORqxf88u++bfZs8emXpGdR5eNYHd6fkotYiKpvRYya2ZzA6Ft1YEHqawwtYcQ53ehCkdouDA3eSe31npwFddIHf9KnitVopDSLKv344KWdP+3hl8pCqkxa4a6OnYAWmuM7ppoqdoMzWYx0HsdpPpngYoYo+T3iUUitKTe1ItItY9fMB1rKJFD1bQotgkafgdRTVUvQWrJopzYWO2NBL/xl3RcRezhYiLSD63+ui/cLDuI/V3vh",
           '__AntiXsrfToken': '1c0d23627d874389a76d9bf8db216789',
           'RT': "z=1&dm=ladbsdoc.lacity.org&si=49827fc0-4dd7-467c-8776-8a2acb0c6925&ss=lo4smir9&sl=0&tt=0",
           'bm_mi':"706788AB91F538C7633BB570DF36F9DA~YAAQpxc51ewqzmCLAQAApvxsYxUSVopbodS9NQIQDpA90VAUd9+euV8bn6SQgDJ/2PbG3BExQTayJINIxBRjNDVaPyR2ngfxm/A9vdiyjOa3iCnW3BjRK3mpaHPNsddtID2Ev/bBayph/Lo8Y7/6rpCItO7luACpq4fXUapXGAF7TKQ3I6WRIJqZUUkMe6UcRaEc6f+/pYg0Q+ucyn8S8poNHQaACjy4g8P6dJSZCQHn/sG20NLwSqv56X918PPpMgKuTRjLeSTeYN/oXc/cxh9RS7aK65KarZQQU8NxDyi/AxyhtR47c4lM3/Gk5BnId7gpqSitBVpABeFF7UIKkwYex34AYE5qqjuTBWrIYKjYSqOJy9biGrhhRPWv6MluqcSaVT4=~1"

           }

headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
}


def get_excel():
    df = pd.read_excel(r"Complete Addresses 6_21_21 - October 2023.xlsx", skiprows=1)
    left_ad = []
    address_col = df['Address'].tolist()
    for ad in address_col:
        if '-' in ad or '/' in ad or '#' in ad or '-' in ad:
            left_ad.append(ad)
            address_col.remove(ad)
    return address_col, left_ad


def sort_by_date(payload):
    payload['ComboOne'] = 'Doc Date'
    payload['FirstAscDesc'] = 'Descending'
    payload['ComboTwo'] = 'Doc Date'
    payload['SecondAscDesc'] = 'Descending'
    payload['ComboThree'] = 'Doc Date'
    payload['ThirdAscDesc'] = 'Descending'
    payload['ComboFour'] = 'Doc Date'
    payload['FourthAscDesc'] = 'Descending'


def get_result_table(soup):
    tables = soup.find_all('table')
    dfs = pd.read_html(str(tables))
    res_table = dfs[-1]
    res_table.columns = res_table.iloc[0]
    res_table = res_table[1:].reset_index(drop=True)
    return res_table


def get_hidden_input(hidden_inputs):
    payload = {}
    for i in hidden_inputs:
        param = i.get('name')
        val = i.get('value')
        if i.get('value') is not None:
            if i.get('value') == 'false' or i.get('value') == 'False':
                payload[param] = False
            elif i.get('value') == 'true' or i.get('value') == 'True':
                payload[param] = False
            elif i.get('value').isnumeric():
                payload[param] = int(val)
            else:
                payload[param] = val
        else:
            payload[param] = ''
    return payload


def put_ticks(checkboxes, payload):
    for c in checkboxes:
        if c.get('name') == "chkAddress1":
            payload.add('chkAddress1', c.get('value'))


def get_table(soup, session):
    payload = get_hidden_input(soup.find_all('input', type='hidden'))
    sort_by_date(payload)
    payload['btnSort'] = 'Sort'
    dropdown = soup.find_all('select')
    for s in dropdown:
        if s.get('name') == 'lstAddress':
            payload['lstAddress'] = s.get('value')
    response = requests.post(url, cookies=cookies, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, 'lxml')
    df = get_result_table(soup)
    table = soup.find('table', id='grdIdisResult')
    rows = table.find_all('tr')
    for index, row in df.iterrows():
        df.iloc[index]['Digital Image'] = rows[index + 1].find_all('td')[-1].find('a').get("href")

    # Get All Pages
    pages = soup.find('div', id='pnlNavigate').find_all('a')
    for i in range(len(pages)):
        hidden_inputs = soup.find_all('input', type='hidden')
        payload = get_hidden_input(hidden_inputs)
        payload['PageNo'] = i + 2
        payload['PageNavigate'] = 'true'
        sort_by_date(payload)
        response = session.post(url, cookies=cookies, headers=headers, data=payload)
        soup = BeautifulSoup(response.text, 'lxml')
        df_extra = get_result_table(soup)
        table = soup.find('table', id='grdIdisResult')
        rows = table.find_all('tr')
        for index, row in df_extra.iterrows():
            df_extra.iloc[index]['Digital Image'] = rows[index + 1].find_all('td')[-1].find('a').get("href")
        df = pd.concat([df, df_extra])
        df = df.reset_index(drop=True)

    return df


async def get_table_async(soup, session):
    payload = get_hidden_input(soup.find_all('input', type='hidden'))
    sort_by_date(payload)
    payload['btnSort'] = 'Sort'
    dropdown = soup.find_all('select')
    for s in dropdown:
        if s.get('id') == 'lstAddress':
            o = s.find('option')
            payload['lstAddress'] = o.get('value')
    async with session.post(url, cookies=cookies, headers=headers, data=payload) as r:
        res = await r.text()
        soup = BeautifulSoup(res, 'lxml')
    df = get_result_table(soup)
    table = soup.find('table', id='grdIdisResult')
    rows = table.find_all('tr')
    for index, row in df.iterrows():
        df.iloc[index]['Digital Image'] = rows[index + 1].find_all('td')[-1].find('a').get("href")

    # Get All Pages
    pages = soup.find('div', id='pnlNavigate').find_all('a')
    for i in range(len(pages)):
        hidden_inputs = soup.find_all('input', type='hidden')
        payload = get_hidden_input(hidden_inputs)
        payload['PageNo'] = i + 2
        payload['PageNavigate'] = 'true'
        sort_by_date(payload)
        async with session.post(url, cookies=cookies, headers=headers, data=payload) as r:
            res = await r.text()
            soup = BeautifulSoup(res, 'lxml')
        df_extra = get_result_table(soup)
        table = soup.find('table', id='grdIdisResult')
        rows = table.find_all('tr')
        for index, row in df_extra.iterrows():
            df_extra.iloc[index]['Digital Image'] = rows[index + 1].find_all('td')[-1].find('a').get("href")
        df = pd.concat([df, df_extra])
        df = df.reset_index(drop=True)

    return df


def go_to_main_page(session):
    r = session.get(url, cookies=cookies, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    return soup


async def go_to_main_page_async(session):
    async with session.get(url, cookies=cookies, headers=headers) as r:
        res = await r.text()
        soup = BeautifulSoup(res, 'lxml')
    return soup


async def type_address_async(soup, address, session):
    hidden_inputs = soup.find_all('input', type='hidden')
    payload = get_hidden_input(hidden_inputs)
    payload['Address$txtAddress'] = address
    payload["btnSearchAddress"] = 'Search'
    async with session.post(url, cookies=cookies, headers=headers, data=payload) as r:
        res = await r.text()
        soup = BeautifulSoup(res, 'lxml')
    return soup


def type_address(soup, address, session):
    hidden_inputs = soup.find_all('input', type='hidden')
    payload = get_hidden_input(hidden_inputs)
    payload['Address$txtAddress'] = address
    payload["btnSearchAddress"] = 'Search'
    response = session.post(url, cookies=cookies, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, 'lxml')
    return soup


def perform_checkbox(session, soup, c, address, files, chk_num):
    hidden_input = soup.find_all('input', type='hidden')
    payload = get_hidden_input(hidden_input)
    payload['chkAddress1'] = c.get('value')
    payload['btnNext2'] = 'Continue'

    response = session.post(url, cookies=cookies, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, 'lxml')
    df = get_table(soup, session)

    subTypes = ['BLDG-NEW', 'BLDG-ADDITION']
    indexes = df.index[df['Sub Type'].isin(subTypes)].tolist()
    success = False
    for i in indexes:
        if df['Digital Image'].iloc[i] is not None:
            index = i
            success = True
            break
    img_links = []

    if success:
        img_links.append(df['Digital Image'].iloc[index])
        while index + 1 < len(df) and df['Doc Date'].iloc[index] == df['Doc Date'].iloc[index + 1] and \
                df['Sub Type'].iloc[index + 1] in subTypes:
            img_links.append(df['Digital Image'].iloc[index + 1])
            index = index + 1

        num_files = len(img_links)
        print(f"Number of files for {address}, checkbox num.{chk_num}: {num_files}")

        for link in img_links:
            ch = "'"
            suffix = re.findall(ch + "(.*)" + ch, link)[0]
            current_file = suffix[:-1]

            final_link = f"https://ladbsdoc.lacity.org/IDISPublic_Records_test/idis/StPdfViewer.aspx?Library=IDIS&Id={current_file}&ObjType=2&Op=View"
            if final_link not in files:
                print(f"Writing file for {address}...")

                files.append(final_link)
    return files


async def perform_checkbox_async(session, soup, c, address, files, chk_num):
    hidden_input = soup.find_all('input', type='hidden')
    payload = get_hidden_input(hidden_input)
    payload['chkAddress1'] = c.get('value')
    payload['btnNext2'] = 'Continue'
    async with session.post(url, cookies=cookies, headers=headers, data=payload) as r:
        res = await r.text()
        soup = BeautifulSoup(res, 'lxml')
    df = await get_table_async(soup, session)

    subTypes = ['BLDG-NEW', 'BLDG-ADDITION']
    indexes = df.index[df['Sub Type'].isin(subTypes)].tolist()
    success = False
    for i in indexes:
        if df['Digital Image'].iloc[i] is not None:
            index = i
            success = True
            break
    img_links = []

    if success:
        img_links.append(df['Digital Image'].iloc[index])
        while index + 1 < len(df) and df['Doc Date'].iloc[index] == df['Doc Date'].iloc[index + 1] and \
                df['Sub Type'].iloc[index + 1] in subTypes:
            img_links.append(df['Digital Image'].iloc[index + 1])
            index = index + 1

        num_files = len(img_links)
        print(f"Number of files for {address}, checkbox num.{chk_num}: {num_files}")

        for link in img_links:
            async with lock:
                ch = "'"
                suffix = re.findall(ch + "(.*)" + ch, link)[0]
                current_file = suffix[:-1]

                final_link = f"https://ladbsdoc.lacity.org/IDISPublic_Records_test/idis/StPdfViewer.aspx?Library=IDIS&Id={current_file}&ObjType=2&Op=View"
                if final_link not in files:
                    print(f"Writing file for {address}...")

                    files.append(final_link)
    return files


def exit_program():
    print('Exiting...')
    sys.exit(0)


def perform_frac(soup, session):
    hidden_input = soup.find_all('input', type='hidden')
    payload = get_hidden_input(hidden_input)
    payload['__EVENTTARGET'] = 'chkFrac'
    payload['chkFrac'] = 'on'
    r = session.post(url, headers=headers, cookies=cookies, data=payload)
    soup = BeautifulSoup(r.text, 'lxml')
    return soup


def perform_unit(soup, session, need_frac):
    hidden_input = soup.find_all('input', type='hidden')
    payload = get_hidden_input(hidden_input)
    if need_frac:
        payload['chkFrac'] = 'on'
    payload['__EVENTTARGET'] = 'chkUnit'
    payload['chkUnit'] = 'on'
    r = session.post(url, headers=headers, cookies=cookies, data=payload)
    soup = BeautifulSoup(r.text, 'lxml')
    return soup




def search(address, session, dict_ad):
    print(f'Searching for address {address}...')
    # Go To Main
    soup = go_to_main_page(session)
    # Type In Address
    soup = type_address(soup, address, session)
    # Put Tick
    need_frac = False
    if '/' in address:
        soup = perform_frac(soup, session)
        need_frac = True
    if '#' or 'unit' in address.lower():
        soup = perform_unit(soup, session, need_frac)


    checkboxes = soup.find_all('input', type='checkbox')
    files = []

    for i, c in enumerate(checkboxes[5:]):
        if i != 0:
            soup = go_to_main_page(session)
            soup = type_address(soup, address, session)

        print(f"Found checkbox: value:{c.get('value')}")
        perform_checkbox(session, soup, c, address, files, i+1)
        # dict_ad[address] = files
        return address, files


async def search_async(address, session, dict_ad):
    print(f'Searching for address {address}...')
    # Go To Main
    soup = await go_to_main_page_async(session)
    status = soup.find('head').find('title').string
    if status == 'Error':
        print(f'Stopped at: {address}')
        exit_program()
    # Type In Address
    soup = await type_address_async(soup, address, session)
    # Put Tick
    checkboxes = soup.find_all('input', type='checkbox')
    files = []
    if len(checkboxes) == 0:
        print(f'No Checkboxes for {address}')
    for i, c in enumerate(checkboxes[5:]):
        if i != 0:
            soup = await go_to_main_page_async(session)
            soup = await type_address_async(soup, address, session)

        print(f"Found checkbox: value:{c.get('value')}")
        await perform_checkbox_async(session, soup, c, address, files, i+1)

    dict_ad[address]=files


async def main():
    dictionary = {}
    async with aiohttp.ClientSession() as s:
        tasks = ['1272 E 87th Pl', '3426 Kelton Ave', '19606 Victory Blvd', '5190 Ellenwood Dr', '321 Alma Real Dr']
        results = await asyncio.gather(*[search_async(ad, s, dictionary) for ad in tasks])
    print(results)



if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())



