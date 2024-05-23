import httpx
from selectolax.parser import HTMLParser
import pandas as pd
import time


# Extract team links and data from Bundesliga season 2022/2023
def extract_data_for_21_22(url_22, h):
    teams_table = url_22.css('#results2021-2022201_overall a')
    u = [lnk.css_first('a').attributes['href'] for lnk in teams_table if '/squads' in lnk.attributes['href']]
    team_url = [f'https://fbref.com{lnk}' for lnk in u]

    data_22 = []
    for url in team_url:
        team_name = url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        prev_resp = httpx.get(url, headers=h)
        prev_h = HTMLParser(prev_resp.text)
        prev_table = pd.read_html(url, match='Scores & Fixtures')[0]
        all_comp_url = prev_h.css_first('#content > div:nth-child(6) > div:nth-child(3) a').attributes['href']
        prev_ac_link = f'https://fbref.com{all_comp_url}'
        print(f'Extracting {team_name} stats from 2021/2022 season...')
        prev_ac_table = pd.read_html(prev_ac_link, match='Shooting')[0]
        prev_ac_table.columns = prev_ac_table.columns.droplevel()

        try:
            prev_data = prev_table.merge(prev_ac_table[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
        except ValueError:
            continue
        prev_data = prev_data[prev_data["Comp"] == "Bundesliga"]
        prev_data['Season'] = 2022
        prev_data['Team'] = team_name
        data_22.append(prev_data)
        time.sleep(7)
    return data_22


# Extract data from Bundesliga season 2022/2023
def extract_data_for_prev_season(prev_urls, h):
    prev_d = []
    for url in prev_urls:
        team_name = url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        prev_resp = httpx.get(url, headers=h)
        prev_h = HTMLParser(prev_resp.text)
        prev_table = pd.read_html(url, match='Scores & Fixtures')[0]
        all_comp_url = prev_h.css_first('#content > div:nth-child(6) > div:nth-child(3) a').attributes['href']
        prev_ac_link = f'https://fbref.com{all_comp_url}'
        print(f'Extracting {team_name} stats from previous season...')
        prev_ac_table = pd.read_html(prev_ac_link, match='Shooting')[0]
        prev_ac_table.columns = prev_ac_table.columns.droplevel()

        try:
            prev_data = prev_table.merge(prev_ac_table[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
        except ValueError:
            continue
        prev_data = prev_data[prev_data["Comp"] == "Bundesliga"]
        prev_data['Season'] = 2023
        prev_data['Team'] = team_name
        prev_d.append(prev_data)
        time.sleep(7)
    return prev_d


# Extract data from Bundesliga 2023/2024 season
def extract_data_for_curr_season(team_urls, h):
    curr_data = []
    for url in team_urls:
        team_name = url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        team_resp = httpx.get(url, headers=h)
        team_html = HTMLParser(team_resp.text)
        sf_table = pd.read_html(url, match='Scores & Fixtures')[0]
        all_comp_url = team_html.css_first('#content > div:nth-child(6) > div:nth-child(3) a').attributes['href']
        all_comp_link = f'https://fbref.com{all_comp_url}'
        print(f'Extracting {team_name} stats...')
        all_comp_table = pd.read_html(all_comp_link, match='Shooting')[0]
        all_comp_table.columns = all_comp_table.columns.droplevel()
        try:
            team_data = sf_table.merge(all_comp_table[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
        except ValueError:
            continue
        team_data = team_data[team_data["Comp"] == "Bundesliga"]
        team_data['Season'] = 2024
        team_data['Team'] = team_name
        curr_data.append(team_data)
        time.sleep(7)
    return curr_data


# Extracting team links for Bundesliga season 2022/2023
def extract_team_links_prev(url):
    teams_table = url.css('#results2022-2023201_overall a')
    u = [lnk.css_first('a').attributes['href'] for lnk in teams_table if '/squads' in lnk.attributes['href']]
    team_url = [f'https://fbref.com{lnk}' for lnk in u]
    return team_url


# Extract team links for Bundesliga season 2023/2024
def extract_team_links(url):
    teams_table = url.css('#results2023-2024201_overall a')
    u = [lnk.css_first('a').attributes['href'] for lnk in teams_table if '/squads' in lnk.attributes['href']]
    team_url = [f'https://fbref.com{lnk}' for lnk in u]
    return team_url


league_url = 'https://fbref.com/en/comps/20/Bundesliga-Stats'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0'}

resp = httpx.get(league_url, headers=headers)
html = HTMLParser(resp.text)
team_links = extract_team_links(html)
curr_season = extract_data_for_curr_season(team_links, headers)

prev_url = html.css_first('.prevnext a').attributes['href']
prev_link = f'https://fbref.com{prev_url}'

prev_r = httpx.get(prev_link, headers=headers)
prev_html = HTMLParser(prev_r.text)

prev_links = extract_team_links_prev(prev_html)
prev_season = extract_data_for_prev_season(prev_links, headers)

season_2022 = prev_html.css_first('.prevnext a').attributes['href']
season_22_link = f'https://fbref.com{season_2022}'

resp_22 = httpx.get(season_22_link, headers=headers)
html_22 = HTMLParser(resp_22.text)
season_22 = extract_data_for_21_22(html_22, headers)

curr_df = pd.concat(curr_season)
prev_df = pd.concat(prev_season)
df_22 = pd.concat(season_22)

match_df = pd.concat([curr_df, prev_df, df_22])
match_df.columns = [c.lower() for c in match_df.columns]
match_df.to_csv('football_data.csv')
