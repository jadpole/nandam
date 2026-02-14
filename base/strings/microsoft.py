from base.core.strings import ValidatedStr
from base.strings.data import REGEX_UUID
from base.strings.file import FileName

REGEX_MS_CHANNEL_ID = r"19:(?:[0-9a-f]{32}|[a-zA-Z0-9_]{44})@thread\.tacv2"
REGEX_MS_GROUP_ID = REGEX_UUID
REGEX_MS_SITE_ID = REGEX_UUID
REGEX_MS_SITE_NAME = r"[a-zA-Z][a-zA-Z0-9]+(?:\-[a-zA-Z0-9]+)*"
REGEX_MS_TEAMS_BOT_ID = rf"28:{REGEX_UUID}"
REGEX_MS_TEAMS_USER_ID = r"29:[A-Za-z0-9]{58}-[A-Za-z0-9]{28}"


##
## Microsoft Teams
##


class MsChannelId(ValidatedStr):
    """
    The unique ID of a Microsoft Teams channel.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "19:0123456789abcdef0123456789abcdef@thread.tacv2",
            "19:1Mk4iREg3uiZ0Ae4qsFRB_Tche75Gn5hvtjOrim6Zbs1@thread.tacv2",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_MS_CHANNEL_ID

    @classmethod
    def from_filename(cls, filename: FileName) -> MsChannelId:
        return cls.decode(f"19:{filename}@thread.tacv2")

    def as_filename(self) -> FileName:
        """
        Returns the unique subset of the channel ID (32 hexadecimal characters)
        as a `FileName` that can be used in `ResourceUri`.

        NOTE: The original value is recovered with `MsChannelId.from_filename`.
        """
        return FileName.decode(self.removeprefix("19:").removesuffix("@thread.tacv2"))


class MsChannelName(ValidatedStr):
    """
    The human-readable name of a channel within Microsoft Teams, which does not
    have any limitations.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["Human-Readable Channel Name"]


class MsGroupId(ValidatedStr):
    """
    The unique ID of a Microsoft Teams group.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["00000000-0000-0000-0000-000000000000"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_MS_GROUP_ID


class MsSiteId(ValidatedStr):
    """
    The unique ID of a SharePoint site.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["00000000-0000-0000-0000-000000000000"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_MS_SITE_ID


class MsSiteName(ValidatedStr):
    """
    The machine-readable name of a Microsoft SharePoint (or Teams) site.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["SiteName-4-Example"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_MS_SITE_NAME


class MsTeamsBotId(ValidatedStr):
    """
    The unique ID of a Microsoft Teams user.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["28:00000000-0000-0000-0000-000000000000"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_MS_TEAMS_BOT_ID


class MsTeamsUserId(ValidatedStr):
    """
    The unique ID of a Microsoft Teams user.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "29:1XZzurdzYGpQpkGsnTBvqtOJlbbxkINm3AIXRzDdt7JMxXXEubNEZcSka0-m83jRfdmFe2hypFZsY6mf3fsqzWg",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_MS_TEAMS_USER_ID


##
## Microsoft Graph API
##


class MsDriveItemId(ValidatedStr):
    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["01NGVHQVK5D264WPLF6ZELAZ4GURKPLE7Z"]

    @classmethod
    def _schema_regex(cls) -> str:
        return r"[a-zA-Z0-9]+"

    @classmethod
    def from_filename(cls, filename: FileName) -> MsDriveItemId:
        """
        Convert `MsDriveItemId.as_filename` back into the original value.
        """
        return MsDriveItemId.decode(str(filename))

    def as_filename(self) -> FileName:
        """
        Transform base64 characters ("+" and "/", alongside "=" padding) into
        a format compatible with `ResourceUri`.
        """
        return FileName.decode(str(self))
