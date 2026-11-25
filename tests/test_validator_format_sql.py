from unittest import TestCase

from focus_validator.validator import Validator


class TestFormatSqlForDisplay(TestCase):
    def setUp(self):
        self.validator = Validator.__new__(Validator)

    def test_keywords_and_spaces_inside_literals_are_kept(self):
        sql = (
            "SELECT  COUNT(*)\n  FROM   tbl WHERE typeof(x) NOT IN "
            "('TIMESTAMP WITH TIME ZONE', 'a  FROM  b', 'it''s WHERE')"
        )

        formatted = self.validator._format_sql_for_display(sql)

        self.assertEqual(
            formatted,
            "SELECT COUNT(*)\n"
            "FROM tbl\n"
            "WHERE typeof(x) NOT IN "
            "('TIMESTAMP WITH TIME ZONE', 'a  FROM  b', 'it''s WHERE')",
        )
