import logging
import math
import os
from typing import Any, Dict, List, Optional, Union

from typing_extensions import Literal

import requests
from requests.adapters import HTTPAdapter, Retry

# *********************************************************************
# LOGGING - set of log messages
# *********************************************************************

log = logging.getLogger(__name__)

# *********************************************************************
# SETUP - of API KEY,header
# retry session , if error
# *********************************************************************

PAGE_SIZE = 1000

DARTMOUTH_API_URL = os.environ["DARTMOUTH_API_URL"]
DARTMOUTH_API_KEY = os.environ["DARTMOUTH_API_KEY"]

log.debug(f"{DARTMOUTH_API_URL}")

headers = {"Authorization": DARTMOUTH_API_KEY}

session = requests.Session()
session.headers["Accept"] = "application/json"

MAX_RETRY = 5
BACK_OFF_FACTOR = 1
ERROR_CODES = (429, 500, 502, 503)

### Retry mechanism for server error ### https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library###
# {backoff factor} * (2 ** ({number of total retries} - 1))
retry_strategy = Retry(total=MAX_RETRY, backoff_factor=BACK_OFF_FACTOR, status_forcelist=ERROR_CODES)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

# *********************************************************************
# FUNCTIONS -
# get login_jwt - get auth key & assign the requests to reponse using post method
# get_coa: get chart of accounts based on segment type

# assign the requests to reponse using get method
# append the list to coa
# In case, if error occurs retry
# *********************************************************************


def get_jwt(session: requests.Session = session, base_url: str = DARTMOUTH_API_URL, key: str = DARTMOUTH_API_KEY, scopes: str = "") -> str:
    """Returns a jwt for authentication to the iPaaS APIs

    Args:
        url (str): LOGIN_URL= https://api.dartmouth.edu/api/jwt
        key (str): API_KEY

    Returns:
        _type_: str
    """

    headers = {"Authorization": key}

    url = f"{base_url}/api/jwt"

    response = session.post(url=url, headers=headers, params={"scopes": scopes})

    if response.ok:
        response_json = response.json()

        log.debug(f"payload={response_json['payload']}")
        log.debug(f"accepted_scopes={response_json['accepted_scopes']}")

        if scopes and scopes not in response_json["accepted_scopes"]:
            raise Exception(f"Scope {scopes} not in the list of accepted scopes")

        jwt = response_json["jwt"]

    else:
        raise Exception("Failed to obtain a jwt")

    return jwt


def get_coa_segment(
    segment: Literal["entities", "orgs", "fundings", "activities", "subactivities", "natural_classes"],
    base_url: str = DARTMOUTH_API_URL,
    session: requests.Session = session,
    page_size=PAGE_SIZE,
    jwt: str = get_jwt(),
) -> List[Dict[str, Any]]:
    """returns iPaaS resources
    Args:
        jwt (str): Dartmouth JSON web token
        url (str): https://api.dartmouth.edu/general_ledger/***segment***
    Returns:
        _type_: list[dict], list[]
    """

    url = f"{base_url}/api/general_ledger/{segment}"

    # *********************************************************************
    # We only want to pull segments that have a parent_child_flag == C
    # the subactivites segment does not have this flag
    # *********************************************************************

    if segment in ["entities", "orgs", "fundings", "activities", "natural_classes"]:
        url = f"{url}?parent_child_flag=C"

    headers: dict = {"Authorization": "Bearer " + jwt, "Content-Type": "application/json"}

    coa_segment = []

    response = session.get(url=url, headers=headers, params={"pagesize": page_size})
    response_json = response.json()

    coa_segment += response_json

    continuation_key = response.headers.get("x-request-id")

    page = 2

    # use for loop until last page:
    while response_json:
        log.debug(f"Starting with page number {page}")

        response = session.get(url=url, headers=headers, params={"pagesize": page_size, "page": page, "continuation_key": continuation_key})

        response_json = response.json()

        log.debug(f"Response contained {len(response_json)} records")

        coa_segment += response_json

        log.debug(f"Records returned, so far: {len(coa_segment)}")
        page += 1

    else:
        log.debug(f"Ending on page {page}")

    return coa_segment
