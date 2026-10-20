"""Mailnotifikationer: én mailtekst pr. kvalitetskontrol"""

from notifikationer.mail import kv1, kv2, kv3, kv4, kv5, kv6, kv7

MAIL_BUILDERS = {
    "KV1": kv1.construct_email,
    "KV2": kv2.construct_email,
    "KV3": kv3.construct_email,
    "KV3-DEV": kv3.construct_email,
    "KV4": kv4.construct_email,
    "KV5": kv5.construct_email,
    "KV6": kv6.construct_email,
    "KV7": kv7.construct_email,
}
