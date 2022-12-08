#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""JIRA to PyPubSub bridge - polls JIRA for updates and pushes them to pypubsub."""
import requests
import time
import pytz
import datetime
import yaml
import sys

DEFAULT_POLL_INTERVAL = 5  # Default is poll once every five seconds.
DEFAULT_SETTINGS_FILE = "jira-to-pubsub.yaml"

# Launch time is recorded, so that we never fetch JIRA changes that are older than this.
LAUNCH_TIME = time.time()

# Global to keep track of the last time we saw an update for a specific ticket.
# This is far cheaper than keeping all the JiraTicket objects in memory.
UPDATES = {}


class Config:
    def __init__(self, config_filepath = DEFAULT_SETTINGS_FILE):
        self.yaml = yaml.safe_load(open(config_filepath).read())
        self.jira_url = self.yaml["jira_url"]
        self.jira_user = self.yaml["jira_user"]
        self.jira_pass = self.yaml["jira_pass"]
        self.jira_base = self.yaml["jira_base"]
        self.pubsub_url = self.yaml["pubsub_url"]
        self.debug = self.yaml.get("debug", False)
        self.poll_interval = int(self.yaml.get("polling_internal", DEFAULT_POLL_INTERVAL))


class JiraTicket:
    """Jira Ticket parser class"""
    def __init__(self, config: Config, json_data: dict):
        """Init a Jira ticket with the base data."""
        self.key = json_data["key"]
        self.link = f"{config.jira_base}{self.key}"
        self.summary = json_data["fields"].get("summary", "(No summary available)")
        self.created = self.jira_to_datetime(json_data["fields"]["created"])
        self.last_update = UPDATES.get(self.key, LAUNCH_TIME)
        self.events = []
        self.json_data = json_data
        self.config = config

    @staticmethod
    def jira_to_datetime(t: str):
        """Parses Jira timestamps into datetime objects"""
        return datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%f%z")

    def make_event_dict(self, entry: dict) -> [dict | None]:
        """Constructs a basic change event from a Jira change-set entry of any type"""

        if "created" not in entry:  # All entries must have this field, or we'll break...
            return
        change_epoch = self.jira_to_datetime(entry.get("updated", entry["created"])).timestamp()

        # If this change is older than our last seen update, ignore and return None
        if change_epoch <= self.last_update:
            return

        # Fetch author name and user ID if present
        if "author" not in entry and "creator" in entry:  # Rewrite author->creator if no author of this entry.
            entry["author"] = entry["creator"]
        if "author" in entry:
            author_name = entry["author"].get("displayName", "??")
            author_uid = entry["author"].get("name", "??")
        else:
            author_name = "??"
            author_uid = "??"

        # Make the basic event dict
        base_event = {
            "key": self.key,
            "link": self.link,
            "summary": self.summary,
            "project": self.key.split("-", 1)[0],
            "action": "unknown",
            "author": author_name,
            "author_uid": author_uid,
            "timestamp": change_epoch,
            "base_entry_created": self.jira_to_datetime(entry["created"]).timestamp()
        }
        return base_event

    def parse_changelog(self):
        """Parses a changelog and formats it for humans"""
        elements = self.json_data.get("changelog", {}).get("histories", [])
        for change in elements:
            base_event = self.make_event_dict(change)
            if not base_event:
                continue

            # Check all ticket status changes/edits
            for item in change["items"]:
                new_event = base_event.copy()  # Make a copy of the base event dict for each item

                # Changing resolution
                if item.get("field") == "resolution":
                    if not item.get("fromString"):  # If changing from no res to a res, assume closing as $X
                        res = item.get("toString", "Unresolved")
                        if self.last_update > 0:
                            new_event["action"] = "close"
                            new_event["resolution"] = res
                            new_event["action_human_text"] = f"closed <{self.link}|{self.key}> as _{res}_."
                            self.events.append(new_event)
                    elif self.last_update > 0:
                        old_resolution = item.get("fromString")
                        new_resolution = item.get("toString", "Unresolved")
                        new_event["action"] = "resolution"
                        new_event["from"] = old_resolution
                        new_event["to"] = new_resolution
                        new_event["action_human_text"] = f"changed the resolution of <{self.link}|{self.key}> from "
                        f"_{old_resolution}_ to _{new_resolution}_."
                        self.events.append(new_event)

                # Status change (like WFU/WFI)
                elif item.get("field") == "status":
                    old_status = item.get("fromString", "")
                    new_status = item.get("toString")
                    if self.last_update > 0:
                        new_event["action"] = "status"
                        new_event["from"] = old_status
                        new_event["to"] = new_status
                        new_event["action_human_text"] = f"changed the status of <{self.link}|{self.key}> to *{new_status}*."
                        self.events.append(new_event)

                # Summary change (editing the title of the ticket)
                elif item.get("field") == "summary":
                    old_summary = item.get("fromString", "")
                    new_summary = item.get("toString")
                    if self.last_update > 0:
                        new_event["action"] = "summary"
                        new_event["from"] = old_summary
                        new_event["to"] = new_summary
                        new_event["action_human_text"] = f"changed the summary of <{self.link}|{self.key}>"
                        self.events.append(new_event)

                # Description change (editing the main text body of the ticket)
                elif item.get("field") == "description":
                    old_description = item.get("fromString", "")
                    new_description = item.get("toString")
                    if self.last_update > 0:
                        new_event["action"] = "description"
                        new_event["from"] = old_description
                        new_event["to"] = new_description
                        new_event["action_human_text"] = f"changed the description of <{self.link}|{self.key}>"
                        self.events.append(new_event)

                # Assignee change
                elif item.get("field") == "assignee":
                    old_assignee = item.get("fromString", "")
                    new_assignee = item.get("toString", "")
                    if self.last_update > 0:
                        new_event["action"] = "assign"
                        new_event["from"] = old_assignee
                        new_event["to"] = new_assignee
                        if new_assignee:
                            new_event["action_human_text"] = f"assigned *{new_assignee}* to <{self.link}|{self.key}>."
                        else:
                            new_event["action_human_text"] = f"unassigned *{old_assignee}* from <{self.link}|{self.key}>."
                        self.events.append(new_event)

    def parse_comments(self):
        """Parse comment changes and format them for humans"""
        elements = self.json_data["fields"].get("comment", {}).get("comments", [])
        for comment in elements:
            base_event = self.make_event_dict(comment)
            if not base_event:
                continue

            # Comment altered?
            if base_event["base_entry_created"] != base_event["timestamp"]:
                if self.last_update > 0:
                    base_event["action"] = "comment_edit"
                    base_event["body"] = comment["body"]
                    base_event["action_human_text"] = "edited a comment"
                    self.events.append(base_event)

            # Or comment created?
            elif self.last_update > 0:
                base_event["action"] = "comment"
                base_event["body"] = comment["body"]
                base_event["action_human_text"] = "added a comment"
                self.events.append(base_event)

    def parse_worklog(self):
        """Parses worklog changes and formats it for humans"""
        elements = self.json_data["fields"].get("worklog", {}).get("worklogs", [])
        for worklog in elements:
            base_event = self.make_event_dict(worklog)
            if not base_event:
                continue

            # Comment altered?
            if base_event["base_entry_created"] != base_event["timestamp"]:
                if self.last_update > 0:
                    base_event["action"] = "comment_edit"
                    base_event["body"] = worklog["comment"]
                    base_event["timespent"] = worklog["timeSpentSeconds"]
                    base_event["action_human_text"] = "edited a worklog entry"
                    self.events.append(base_event)

            # Or comment created?
            elif self.last_update > 0:
                base_event["action"] = "comment"
                base_event["body"] = worklog["comment"]
                base_event["timespent"] = worklog["timeSpentSeconds"]
                base_event["action_human_text"] = "added a worklog entry"
                self.events.append(base_event)


def fetch_changes(config: Config):
    """Fetch the latest changes from JIRA and process them"""
    try:
        js = requests.get(config.jira_url, auth=(config.jira_user, config.jira_pass), timeout=15).json()
        assert isinstance(js, dict), "Jira search did not return a dictionary response"
        return js.get("issues", [])
    except requests.RequestException as e:  # This should work - if for whatever reason it doesn't, try again later.
        print(f"Could not contact Jira, waiting for next try: {e}")
        return []
    except AssertionError as e:
        print(f"General assertion error with Jira response: {e}")


def process_changes(config: Config, changes: list):
    now = datetime.datetime.now(pytz.utc)

    for issue in changes:
        ticket = JiraTicket(config, issue)

        # Is this a brand-new issue?
        if ticket.last_update == 0 and (now - ticket.created).seconds <= 30:
            UPDATES[ticket.key] = ticket.created.timestamp()
            base_event = ticket.make_event_dict(issue["fields"])
            base_event["action"] = "create"
            base_event["action_human_text"] = f"created {ticket.key}: {ticket.summary}"
            base_event["description"] = issue["fields"].get("description", "(No description available)")
            ticket.events.append(base_event)

        # Parse changelog, comments, worklog
        ticket.parse_changelog()
        ticket.parse_comments()
        ticket.parse_worklog()

        if ticket.events:  # If any changes were found, update our "last updated" marker for this ticket.
            UPDATES[ticket.key] = max([event["timestamp"] for event in ticket.events])

        # Post all new events found to pubsub
        for event in ticket.events:
            target_url = config.pubsub_url.format(**event)
            if config.debug:
                print(event["timestamp"], event["key"], event["author"], event["action_human_text"])
                print(event)
                print("---------------")
            else:
                try:
                    requests.post(target_url, json=event, timeout=5)
                except requests.RequestException as e:
                    print(f"Could not post update for {ticket.key}: {e}")


def main():
    config = Config(sys.argv[1] if (len(sys.argv) > 1) else DEFAULT_SETTINGS_FILE)
    while True:  # Loop the Loop, forever and ever.
        process_start = time.time()  # Log when we started processing this batch
        change_set = fetch_changes(config)  # Fetch latest change-set
        process_changes(config, change_set)  # Process change-set (if any)
        process_duration = time.time() - process_start  # Figure out how long that process took
        if process_duration < config.poll_interval:  # If we need to sleep, sleep, otherwise poll again.
            time.sleep(config.poll_interval - process_duration)


if __name__ == "__main__":
    main()
