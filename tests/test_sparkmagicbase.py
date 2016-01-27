from remotespark.utils.constants import Constants
from remotespark.magics.sparkmagicsbase import SparkMagicBase


def test_get_livy_kind_covers_all_langs():
    for lang in Constants.lang_supported:
        SparkMagicBase.get_livy_kind(lang)


def test_print_endpoint_info_doesnt_throw():
    SparkMagicBase.print_endpoint_info(range(5))
