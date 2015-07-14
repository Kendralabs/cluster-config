import unittest
#import pytest
from mock import MagicMock
from atk_config import dict

class TestCli(unittest.TestCase):
    def test_nest_happy(self):
        temp = {}
        key = "one.two.three"
        value = 3
        test = dict.nest(temp, key.split("."), value)

        assert temp["one"]["two"]["three"] == value

    def test_nest_single_key(self):
        temp = {}
        key = "one"
        value = "two"
        dict.nest(temp, key.split("."), value)

        assert temp["one"] == value

    def test_nest_two_keys(self):
        temp = {}
        key = "one.two"
        value = "2"
        dict.nest(temp, key.split("."), value)

        assert temp["one"]["two"] == value

    def test_nest_no_keys(self):
        temp = {}
        keys = []
        value = "2"
        dict.nest(temp, keys, value)

        assert len(temp) == 0

    def test_nest_none_keys(self):

        dict.nest(None, None, None)
        #if we get any errors the test will fail
        assert True

    def test_merge_dicts_happy(self):
        first = { "one" : 1}
        second = { "two": 2}

        merged = dict.merge_dicts(first, second)

        assert merged["one"] == 1 and merged["two"] == 2

        merged = dict.merge_dicts({ "lvl1": { "lvl2": { "lvl3": 3}}}, { "lvl1": { "lvl2": { "another": 3}}})

        assert merged["lvl1"]["lvl2"]["lvl3"] == 3 and merged["lvl1"]["lvl2"]["another"] == 3

    def test_merge_dicts_conflict_first(self):
        first = { "one" : 1}
        second = { "one": 2}

        #default will resolve conflict to 1 instead of 2
        merged = dict.merge_dicts(first, second)

        assert merged["one"] == 1

    def test_merge_dicts_conflict_second(self):
        first = { "one" : 1}
        second = { "one": 2}

        #default will resolve conflict to 1 instead of 2
        merged = dict.merge_dicts(first, second, "second")

        assert merged["one"] == 2


    def test_merge_dicts_none(self):
        first = { "one" : 1}
        second = { "one": 2}

        #default will resolve conflict to 1 instead of 2
        merged = dict.merge_dicts(None, None)

        assert merged == None

    def test_recurse_type_check_none(self):

        assert dict._recurse_type_check(None, None) == False

    def test_recurse_type_check_happy(self):

        check = { "list": [], "dict": {}}

        assert dict._recurse_type_check(check, "list") == True
        assert dict._recurse_type_check(check, "dict") == True

    def test_recurse_type_check_negative(self):

        check = { "list": [], "dict": {}, "value": 1}

        assert dict._recurse_type_check(check, "value") == False

    def test_merge_dicts_no_lists(self):
        dict1 = { "one": { "two": { "three" : []}},
                  "four":{ "five": { "six" : [
                      { "seven": {}, "eight": {} } ] } }
        }
        dict2 = { "four": { "five": { "six" : [{ "seven": {}}]}}}
        #with self.assertRaises(TypeError):
        merged = dict._merge_dicts(dict1, dict2)

        assert merged["four"]["five"]["six"] is None

    def test_merge_dicts_no_conflicts(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}}
        dict2 = { "four": { "five": { "six" : 8}}, "seven": 7}

        merged = dict._merge_dicts(dict1, dict2)

        #conclict resolution doesn't matter
        assert merged["four"]["five"]["six"] == 6

        assert merged["one"]["two"]["three"] == 3

        assert merged["seven"] == 7

        merged = dict._merge_dicts(dict2, dict1)

        #conclict resolution doesn't matter
        assert merged["four"]["five"]["six"] == 8

    def test_find_dict_conflicts_happy(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}}
        dict2 = { "four": { "five": { "six" : 8}}}

        conflicts = dict.find_dict_conflicts(dict1, dict2)

        assert len(conflicts) == 1

        assert conflicts[0][0] == "four" and conflicts[0][1] == "five" and conflicts[0][2] == "six"

        conflicts = dict.find_dict_conflicts(dict2, dict1)

        assert len(conflicts) == 1

        assert conflicts[0][0] == "four" and conflicts[0][1] == "five" and conflicts[0][2] == "six"


    def test_find_dict_conflicts_equal_conflicts(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}, "seven": 7}
        dict2 = { "four": { "five": { "six" : 8}}, "seven": 7}

        conflicts = dict.find_dict_conflicts(dict1, dict2)

        assert len(conflicts) == 1

        assert conflicts[0][0] == "four" and conflicts[0][1] == "five" and conflicts[0][2] == "six"

        conflicts = dict.find_dict_conflicts(dict2, dict1)

        assert len(conflicts) == 1

        assert conflicts[0][0] == "four" and conflicts[0][1] == "five" and conflicts[0][2] == "six"

    def test_find_dict_conflicts_equal_conflicts(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}, "seven": 11}
        dict2 = { "four": { "five": { "six" : 8}}, "seven": 7}

        conflicts = dict.find_dict_conflicts(dict1, dict2)

        assert len(conflicts) == 2

        assert conflicts[0][0] == "four" and conflicts[0][1] == "five" and conflicts[0][2] == "six"
        assert conflicts[1][0] == "seven"

        conflicts = dict.find_dict_conflicts(dict2, dict1)

        assert len(conflicts) == 2

        assert conflicts[0][0] == "four" and conflicts[0][1] == "five" and conflicts[0][2] == "six"
        assert conflicts[1][0] == "seven"



