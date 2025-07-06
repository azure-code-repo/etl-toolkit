import enum

__doc__ = """Dictionary Files - contain all validation str constrain for user input"""


class UEnumMeta(enum.EnumMeta):
    def __contains__(cls, item):
        return item in [v.value for v in cls.__members__.values()]


class Country(enum.Enum, metaclass=UEnumMeta):
    """Country Dictionary

    This use to validate user input.
    """

    SEAID = "SEAID"  # All
    VN = "VN"  # Vietnam
    ID = "ID"  # Indonesia
    TH = "TH"  # THailand
    PH = "PH"  # Philippines
    MY = "MY"  # Malaysia
    KH = "KH"  # Khmer - Cambodia
    LA = "LA"  # Laos
    SG = "SG"  # Singapore


class SaveMode(enum.Enum, metaclass=UEnumMeta):
    """SaveMode Dictionary

    This use to validate user input."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"


class Layer(enum.Enum, metaclass=UEnumMeta):
    """Layer Dictionary

    This use to validate user input."""

    BRONZE = "Bronze"
    SILVER = "Silver"
    GOLD = "Gold"
    PLATINUM = "Platinum"


class Stage(enum.Enum, metaclass=UEnumMeta):
    """Stage Dictionary

    This use to validate user input."""

    LANDED = "Landed"
    PROCESSED = "Processed"


class UdfLocation(enum.Enum, metaclass=UEnumMeta):
    # TODO: Come up with something for pre-extract. for now disable it for easier use
    # PRE_EXTRACT = "PRE_EXTRACT"
    POST_EXTRACT = "POST_EXTRACT"
    PRE_TRANSFORM = "PRE_TRANSFORM"
    POST_TRANSFORM = "POST_TRANSFORM"
    PRE_LOAD = "PRE_LOAD"
    POST_LOAD = "POST_LOAD"
