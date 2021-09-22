# TODO:
# - patch 0 cases results
# - Docstrings

import re
import requests

from bs4 import BeautifulSoup
from dateparser.search import search_dates


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
        page = requests.get(link["href"], headers=headers)
        soup = BeautifulSoup(page.content, "html.parser")
        incidents.append(soup.select(".post__content__cat+ p")[0].text)
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
