import os
from datetime import datetime

import numpy as np
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

CRED_DIR = "./credentials/google"


class GoogleCalendarOperator:
    def __init__(self, name: str = ""):
        # Please follow this guide to create a credential file first:
        # https://developers.google.com/calendar/api/quickstart/python

        SCOPES = ["https://www.googleapis.com/auth/calendar"]
        creds = None
        if os.path.exists(os.path.join(CRED_DIR, f"{name}_token.json")):
            creds = Credentials.from_authorized_user_file(
                os.path.join(CRED_DIR, f"{name}_token.json"), SCOPES
            )
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    os.path.join(CRED_DIR, f"{name}.json"), SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(os.path.join(CRED_DIR, f"{name}_token.json"), "w") as token:
                token.write(creds.to_json())
        self.service = build("calendar", "v3", credentials=creds)

    def list_calendars(self):
        page_token = None
        while True:
            calendar_list = (
                self.service.calendarList().list(pageToken=page_token).execute()
            )
            for calendar_list_entry in calendar_list["items"]:
                print(calendar_list_entry["summary"])
                items = ["description", "id", "timeZone"]
                [
                    print(f"\t{item}: {calendar_list_entry[item]}")
                    for item in items
                    if item in calendar_list_entry
                ]
                print("\n")
            page_token = calendar_list.get("nextPageToken")
            if not page_token:
                break

    def get_calendar_id(self, calendar_name: str) -> str:
        page_token = None
        while True:
            calendar_list = (
                self.service.calendarList().list(pageToken=page_token).execute()
            )
            for calendar_list_entry in calendar_list["items"]:
                if calendar_name in calendar_list_entry["summary"]:
                    return str(calendar_list_entry["id"])
            page_token = calendar_list.get("nextPageToken")
            if not page_token:
                break
        raise ValueError(
            "Cannot find calendar id. Please provide correct calendar name."
        )

    def create_event_execute(self, calendar_id: str, event: dict):
        try:
            self.service.events().insert(calendarId=calendar_id, body=event).execute()
            return ""
        except Exception as e:
            return e

    def create_event(
        self,
        calendar: str,
        title: str,
        start: datetime,
        end: datetime,
        detail: str = "",
        timezone: str = "UTC",
    ):
        calendar_id = self.get_calendar_id(calendar)
        event = {
            "summary": title,
            "description": detail,
            "start": {
                "dateTime": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "timeZone": timezone,
            },
            "end": {
                "dateTime": end.strftime("%Y-%m-%dT%H:%M:%S"),
                "timeZone": timezone,
            },
        }
        return self.create_event_execute(calendar_id, event)


class GoogleSheetOperator:
    def __init__(self, name: str = ""):
        # Please follow this guide to create a credential file first:
        # https://developers.google.com/sheets/api/quickstart/python

        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = None
        if os.path.exists(os.path.join(CRED_DIR, f"{name}_token.json")):
            creds = Credentials.from_authorized_user_file(
                os.path.join(CRED_DIR, f"{name}_token.json"), SCOPES
            )
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    os.path.join(CRED_DIR, f"{name}.json"), SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(os.path.join(CRED_DIR, f"{name}_token.json"), "w") as token:
                token.write(creds.to_json())
        self.service = build("sheets", "v4", credentials=creds)

    def get_sheet(self, sheet_id, range_name, **kwargs):
        res = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=range_name, **kwargs)
            .execute()
        )
        return res.get("values", [])

    def get_sheet_df(self, sheet_id, range_name, header: int = 0, **kwargs):
        df = pd.DataFrame(self.get_sheet(sheet_id, range_name, **kwargs))
        if header >= 0:
            df.columns = df.iloc[header, :].to_list()
            df = df.drop(header)
        return df

    def update_sheet(
        self, sheet_id, range_name, values, input_option: str = "USER_ENTERED", **kwargs
    ):
        body = {"values": values}
        result = (
            self.service.spreadsheets()
            .values()
            .update(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption=input_option,
                body=body,
                **kwargs,
            )
            .execute()
        )
        return result

    def update_sheet_df(
        self,
        sheet_id,
        range_name,
        df,
        header=True,
        input_option: str = "USER_ENTERED",
        **kwargs,
    ):
        df = df.replace(np.nan, "")
        values = df.values.tolist()
        if header:
            values = [df.columns.to_list()] + values
        return self.update_sheet(sheet_id, range_name, values, input_option, **kwargs)

    def clear_sheet(self, sheet_id, range_name, **kwargs) -> None:
        self.service.spreadsheets().values().clear(
            spreadsheetId=sheet_id, range=range_name, body={}, **kwargs
        ).execute()
