# TODO:
# - patch 0 cases results
# - Docstrings

import re
import requests

from bs4 import BeautifulSoup
from dateparser.search import search_dates
import gspread
import pandas as pd


def scrape_articles_data(
    rs_url="https://dcpsreopenstrong.com/category/articles/",
) -> list:
    """
    Scrape school covid case data from the articles page. This page is
    easier to deal with because it has everything on one page, but it only
    has the 100 most recent letters.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    page = requests.get(rs_url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    incidents = soup.select("#wrap p")
    # this selector gets two copies of each set of text, because reasons
    return [r.text for r in incidents][1::2]


def scrape_notifications_data(
    rs_url="https://dcpsreopenstrong.com/health/response/notifications/",
) -> list:
    """
    Scrape school covid case data from the notifications page. This is just a
    page of links so the scraper has to follow each link to get the text.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    page = requests.get(rs_url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.select(".uagb-post__title a")
    incidents = []
    for link in links:
        # I hate this try/except but they broke the CSS on Dunbar Oct 13 and I
        # need to defend against future errors
        try:
            page = requests.get(link["href"], headers=headers)
            soup = BeautifulSoup(page.content, "html.parser")
            incidents.append(soup.select(".post__content__cat+ p")[0].text)
        except:
            print(link)
    return incidents


def parse_incidents(incidents: list) -> dict:
    """
    Parse data from text in the format found on the Reopen Strong website.
    This parser takes a list and returns a dict because
    """
    schools = []
    letter_dates = []
    cases_counts = []
    incident_dates = []
    no_case_counts = []
    incidents_out = []

    for incident in incidents:
        # bp = boilerplate
        bp_0 = "A letter to the "
        bp_1 = " community was sent on "
        school = [
            re.search(re.escape(bp_0) + r"(.*)" + re.escape(bp_1), incident).group(1)
        ]

        # this is a horrible hack around a bug in date_search()
        dates_and_counts = incident.split(bp_1)[1] + " to "

        # Hacks because dateparser doesn't have options to ignore "on", "to", etc...
        # and the letter text does ratchet stuff like
        # "September 13 (Oyster campus), 2021" that dateparser thinks is two dates
        dates = [
            i
            for i in search_dates(dates_and_counts)
            if i[0] not in ["on", "to", "2021", "2022", "2023"]
        ]

        letter_date = [dates[0][1]]

        if len(dates) == 2:
            incident_date = [dates[1][1]]
            incident_out = [incident]
        else:
            incident_date = [d[1] for d in dates[1:]]

        incident_count = len(incident_date)
        letter_date = letter_date * incident_count
        school = school * incident_count
        incident_out = [incident] * incident_count

        # this will work until it doesn't
        number_words = [
            "zero",
            "one",
            "two",
            "three",
            "four",
            "five",
            "six",
            "seven",
            "eight",
            "nine",
            "ten",
            "eleven",
            "twelve",
            "thirteen",
            "fourteen",
            "fifteen",
            "sixteen",
            "seventeen",
            "eighteen",
            "nineteen",
            "twenty",
        ]
        cases_count_decode = dict(zip(number_words, list(range(21))))

        # list of all the number word appearances
        cases_count = [
            cases_count_decode.get(w)
            for w in dates_and_counts.split()
            if cases_count_decode.get(w)
        ]

        if len(cases_count) == 0:
            cases_count = [1] * incident_count
            no_case_count = [True] * incident_count
        elif len(cases_count) < incident_count:
            # this handles instances that use "two cases ... respectively".
            # spread cases over dates using floor division
            cases_count = [cases_count[0] // incident_count] * incident_count
            no_case_count = [False] * incident_count
        else:
            no_case_count = [False] * incident_count

        # hack to make sure cases count is never zero, which can happen when
        # the text uses weird constructions that boil down to "a case"
        cases_count = [max(c, 1) for c in cases_count]

        outputs = [
            school,
            letter_date,
            cases_count,
            incident_date,
            no_case_count,
            incident_out,
        ]

        if len({len(i) for i in outputs}) == 1:
            schools.extend(school)
            letter_dates.extend(letter_date)
            cases_counts.extend(cases_count)
            incident_dates.extend(incident_date)
            no_case_counts.extend(no_case_count)
            incidents_out.extend(incident_out)
        else:
            print(dates_and_counts, outputs)

    return {
        "school": schools,
        "letter_date": letter_dates,
        "cases_count": cases_counts,
        "incident_date": incident_dates,
        "no_case_count": no_case_counts,
        "incident_text": incidents_out,
    }


def data_to_dataframe(all_data: dict) -> object:
    """
    Format and sort the output of parse_incidents()
    """
    all_data = pd.DataFrame(all_data)
    all_data = all_data[
        [
            "incident_date",
            "letter_date",
            "school",
            "cases_count",
            "no_case_count",
            "incident_text",
        ]
    ]
    all_data.sort_values(
        ["incident_date", "school", "letter_date", "cases_count"], inplace=True
    )
    return all_data


def fix_school_names(all_data: object) -> object:
    name_corrections = {
        "Barnard ES": "Barnard",
        "Beers ES": "Beers",
        "Boone Elementary": "Boone",
        "Bruce-Monrow": "Bruce-Monroe",
        "CW Harris": "Harris",
        "C.W. Harris": "Harris",
        "Dorothy I. Height": "Height",
        "Duke Ellington": "Ellington",
        "Dunbar High School": "Dunbar",
        "Excel Academy": "Excel",
        "Excel-Academy": "Excel",
        "GarrisonElementary": "Garrison",
        "H.D.Cooke ": "Cooke",
        "H.D.Cooke": "Cooke",
        "H.D. Cooke": "Cooke",
        "HD Cooke": "Cooke",
        "H.D. Woodson": "Woodson",
        "HD Woodson": "Woodson",
        "Houston ES": "Houston",
        "Ida.B.Wells": "Wells",
        "Ida B. Wells": "Wells",
        "J.O. Wilson Elementary": "Wilson",
        "J.O. Wilson": "Wilson",
        "Jefferson MS": "Jefferson",
        "John Hayden Johnson Middle School": "Johnson",
        "Kelly Miller": "Miller",
        "Kimball Elementary": "Kimball",
        "King ES": "King",
        "King Elementary": "King",
        "Langdon ES": "Langdon",
        "Luke C. Moore": "Moore",
        "Mann Elementary": "Mann",
        "Maury Elementary": "Maury",
        "McKinley High School": "McKinley HS",
        "McKinley Tech High School": "McKinley HS",
        "McKinley Middle": "McKinley MS",
        "McKinley Middle School": "McKinley MS",
        "Miner Elementary": "Miner",
        "Moten Elementary": "Moten",
        "Nalle Elementary School": "Nalle",
        "Oyster-Adams (Adams)": "Oyster-Adams",
        "Peabody and Watkins": "Peabody Watkins",
        "Ron Brown": "Brown",
        "Roosevelt STAY High School": "Roosevelt STAY",
        "School Without Walls at Francis-Stevens": "School Without Walls",
        "SWW @ Francis Stevens": "School Without Walls",
        "School Within Walls": "School Without Walls",
        "School-Within-School": "SWS Goding",
        "Seaton ES": "Seaton",
        "Stuart Hobson": "Stuart-Hobson",
        "Stuart-Hobson Middle": "Stuart-Hobson",
        "Thomas Elementary": "Thomas",
        "Thomson Elementary": "Thomson",
        "Tubman Elementary School": "Tubman",
        "Van Ness Elementary School": "Van Ness",
        "Walls": "School Without Walls",
        "Watkins Elementary School": "Watkins",
    }
    all_data["school"] = all_data["school"].replace(name_corrections)
    return all_data


def append_school_levels(all_data: object) -> object:
    school_level_decode = {
        "Aiton": "Elementary",
        "Amidon-Bowen": "Elementary",
        "Anacostia": "High",
        "Ballou": "High",
        "Ballou STAY": "High",
        "Bancroft": "Elementary",
        "Banneker": "High",
        "Bard": "High",
        "Barnard": "Elementary",
        "Beers": "Elementary",
        "Boone": "Elementary",
        "Brent": "Elementary",
        "Brightwood": "Elementary",
        "Brookland": "Middle",
        "Browne": "PK-8",
        "Bruce-Monroe": "Elementary",
        "Bunker Hill": "Elementary",
        "Burroughs": "Elementary",
        "Burrville": "Elementary",
        "C.W. Harris": "Elementary",
        "Capitol Hill Montessori": "Elementary",
        "Cardozo": "6-12",
        "CHEC": "6-12",
        "Cleveland": "Elementary",
        "Coolidge": "High",
        "Deal": "Middle",
        "Dorothy I. Height": "Elementary",
        "Drew": "Elementary",
        "Dunbar": "High",
        "Eastern": "High",
        "Eaton": "Elementary",
        "Eliot-Hine": "Middle",
        "Ellington": "High",
        "Excel": "PK-8",
        "Garfield": "Elementary",
        "Garrison": "Elementary",
        "H.D. Cooke": "Elementary",
        "Hardy": "Middle",
        "Hart": "Middle",
        "Hendley": "Elementary",
        "Houston": "Elementary",
        "Ida B. Wells": "Middle",
        "J.O. Wilson": "Elementary",
        "Janney": "Elementary",
        "Jefferson": "Middle",
        "Johnson": "Middle",
        "Kelly Miller": "Middle",
        "Ketcham": "Elementary",
        "Key": "Elementary",
        "Kimball": "Elementary",
        "King": "Elementary",
        "Kramer": "Middle",
        "Lafayette": "Elementary",
        "Langdon": "Elementary",
        "Langley": "Elementary",
        "Leckie": "PK-8",
        "Ludlow-Taylor": "Elementary",
        "Luke C. Moore": "High",
        "MacFarland": "Middle",
        "Malcolm X": "Elementary",
        "Marie Reed": "Elementary",
        "Maury": "Elementary",
        "McKinkey Middle": "Middle",
        "McKinley": "Unknown",
        "McKinley High": "High",
        "Military Road": "PK",
        "Miner": "Elementary",
        "Moten": "Elementary",
        "Murch": "Elementary",
        "Nalle": "Elementary",
        "Noyes": "Elementary",
        "Oyster-Adams": "PK-8",
        "Patterson": "Elementary",
        "Payne": "Elementary",
        "Peabody": "Elementary",
        "Peabody Watkins": "Elementary",
        "Phelps": "High",
        "Plummer": "Elementary",
        "Powell": "Elementary",
        "Randle Highlands": "Elementary",
        "Raymond": "Elementary",
        "River Terrace": "3-12",
        "Roosevelt": "High",
        "Savoy": "Elementary",
        "School Without Walls": "PK-8",
        "Seaton": "Elementary",
        "Shepherd": "Elementary",
        "Simon": "Elementary",
        "Smothers": "Elementary",
        "Sousa": "Middle",
        "Stanton": "Elementary",
        "Stevens": "PK",
        "Stoddert": "Elementary",
        "Stuart-Hobson": "Middle",
        "SWS Goding": "Elementary",
        "SWW @ Francis Stevens": "PK-8",
        "Takoma": "Elementary",
        "Thomas": "Elementary",
        "Thomson": "Elementary",
        "Truesdell": "Elementary",
        "Tubman": "Elementary",
        "Turner": "Elementary",
        "Tyler": "Elementary",
        "Van Ness": "Elementary",
        "Walker-Jones": "PK-8",
        "Watkins": "Elementary",
        "Wells": "Middle",
        "West": "Elementary",
        "Whittier": "Elementary",
        "Wilson": "High",
        "Woodson": "High",
    }
    all_data["school_level"] = (
        all_data["school"].map(school_level_decode).fillna("Unknown")
    )
    return all_data


def run_one_shot_fixes_html(incidents: list) -> list:
    """
    Sometimes the notification text is just too ratchet to parse, so these are
    manual fixes to known past issues. This version runs on the text in the
    html.
    """
    incidents[
        incidents.index(
            "A letter to Emery was sent on September 29, 2021, notifying them of a positive COVID-19 case in the building on September 27, 2021."
        )
    ] = "A letter to the Emery community was sent on September 29, 2021, notifying them of a positive COVID-19 case in the building on September 27, 2021."
    incidents[
        incidents.index(
            "A letter to the Dunbar community was sent on October 1, 2021, notifying them of two positive COVID-19 cases in the building on September 24 and September 30, 2021, respectively. "
        )
    ] = "A letter to the Dunbar community was sent on October 1, 2021, notifying them of two positive COVID-19 cases in the building on September 24, and September 30, respectively."
    incidents.append(
        "A letter to the Dunbar High School community was sent on October 13, 2021, notifying them of a positive COVID-19 case in the building on October 12, 2021."
    )
    incidents[
        incidents.index(
            "A letter to the Jefferson community was sent on October 15, 2021, notifying them of two positive COVID-19 cases in the building on October 12 and October 13, respectively."
        )
    ] = "A letter to the Jefferson community was sent on October 15, 2021, notifying them of two positive COVID-19 cases in the building on October 12, and October 13, respectively."

    return incidents


def update_gsheet(all_data: object):
    # Just hardcoding this for now
    gc = gspread.service_account(
        filename="/Users/tterry/.config/gspread/covid-dc-87e2feea6431.json"
    )
    covid = gc.open("DCPS covid data 2021-2022")
    data = covid.worksheet("data")
    all_data = all_data[all_data.incident_date >= "2021-08-20"][
        [
            "incident_date",
            "letter_date",
            "school",
            "cases_count",
            "school_level",
            "ward",
            "incident_text",
        ]
    ]
    # Google Data Studio is being a chaos monkey about recognizing dates.
    # I don't know why formatting them two different ways makes it work,
    # but it's the only thing that works so we're rolling with it.
    # I suspect that the underlying JSON or whatever that defines the
    # gsheet has tied itself in a knot about the types because of all the
    # changes made over time.
    all_data["incident_date"] = all_data["incident_date"].dt.strftime("%Y%m%d")
    all_data["letter_date"] = all_data["letter_date"].dt.strftime("%Y-%m-%d")
    data.update([all_data.columns.values.tolist()] + all_data.values.tolist())
    gc.session.close()
