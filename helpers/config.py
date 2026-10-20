"""Module for general configurations of the process"""

from fejlfinding import kv1, kv2, kv3, kv3_dev, kv4, kv5, kv6, kv7
from helpers.tillaeg_pairs import tillaeg_pairs

MAX_RETRY = 10

# ----------------------
# Queue population settings
# ----------------------
MAX_CONCURRENCY = 100  # tune based on backend capacity
MAX_RETRIES = 3  # transient failure retries per item
RETRY_BASE_DELAY = 0.5  # seconds (exponential backoff)

# The number of times the robot retries on an error before terminating.
MAX_RETRY_COUNT = 3

# Whether the robot should be marked as failed if MAX_RETRY_COUNT is reached.
FAIL_ROBOT_ON_TOO_MANY_ERRORS = True

# Constant/Credential names
ERROR_EMAIL = "Error Email"

# The limit on how many queue elements to process
MAX_TASK_COUNT = 100

# ----------------------
TEMP_PATH = R"C:\SDLøn"

# Dictionary with process specific functions and parameters
PROCESS_PROCEDURE_DICT = {
    "KV1": {
        "procedure": kv1.kv1,
        "parameters": {
            "overenskomst": 47302
        },  # Overenskomst in which all employments should have INSTKODE = XC
    },
    "KV2": {
        "procedure": kv2.kv2,
        "parameters": {"tillaegsnr_par": tillaeg_pairs},
    },
    "KV3": {
        "procedure": kv3.kv3,
        "parameters": {
            "accept_ovk_dag": (),  # Overenskomster starting with "7" but accepted in dagtilbud/UIAA
            "accept_ovk_skole": (
                43011,
                43017,
                43031,
                44001,
                44101,
                45001,
                45002,
                45081,
                45082,
                46901,
                47591,
                48888,
            ),  # Overenskomster starting with "4" but accepted in schools
        },
    },
    "KV3-DEV": {
        "procedure": kv3_dev.kv3_dev,
        "parameters": {
            "accept_ovk_dag": (),
            "accept_ovk_skole": (
                43011,
                43017,
                43031,
                44001,
                44101,
                45001,
                45002,
                45081,
                45082,
                46901,
                47591,
                48888,
            ),
        },
    },
    "KV4": {
        "procedure": kv4.kv4,
        "parameters": {
            "leder_overenskomst": (45082, 45081, 46901, 45101, 47201),
        },
    },
    "KV5": {
        "procedure": kv5.kv5,
        "parameters": {},
    },
    "KV6": {
        "procedure": kv6.kv6,
        "parameters": {},
    },
    "KV7": {
        "procedure": kv7.kv7,
        "parameters": {
            "exclude_schoolname": ["Stensagerskolen", "Langagerskolen"],
            "exclude_dagtilbudname": [],
        },
    },
}
