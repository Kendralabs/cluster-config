import unittest

from mock import MagicMock
from cluster_config import dict
import cluster_config as cc

class TestCli(unittest.TestCase):
    dict1 = { "one": { "two": { "three" : 3}},
              "four":{ "five": { "six" : 6 }}, "seven": 11}
    dict2 = { "four": { "five": { "six" : 8}}, "seven": 7}

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

        assert dict._recurse_type_check(check, "list") == False
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

        assert merged["four"]["five"]["six"] == dict1["four"]["five"]["six"]

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

    def test_resolve_conflicts_first(self):
        dict.user_input = MagicMock()

        resolved = dict.resolve_conflicts([["seven"],["four","five","six"]], self.dict1, self.dict2, "first")

        assert resolved[0][1] == 11
        assert resolved[1][1] == 6
        assert dict.user_input.call_count == 0

    def test_resolve_conflicts_second(self):
        dict.user_input = MagicMock()

        resolved = dict.resolve_conflicts([["seven"],["four","five","six"]], self.dict1, self.dict2, "second")

        assert resolved[0][1] == 7
        assert resolved[1][1] == 8
        assert dict.user_input.call_count == 0

    def test_resolve_conflicts_interactive_user_single(self):
        dict.user_input = MagicMock(return_value=cc.SINGLE[0])

        resolved = dict.resolve_conflicts([["seven"],["four","five","six"]], self.dict1, self.dict2, "interactive")

        assert resolved[0][1] == 11
        assert resolved[1][1] == 6
        assert dict.user_input.call_count == 2

    def test_resolve_conflicts_interactive_generated_single(self):
        dict.user_input = MagicMock(return_value=cc.SINGLE[1])

        resolved = dict.resolve_conflicts([["seven"],["four","five","six"]], self.dict1, self.dict2, "interactive")

        assert resolved[0][1] == 7
        assert resolved[1][1] == 8
        assert dict.user_input.call_count == 2

    def test_resolve_conflicts_interactive_user_persistent(self):
        dict.user_input = MagicMock(return_value=cc.PERSISTANT[0])

        resolved = dict.resolve_conflicts([["seven"],["four","five","six"]], self.dict1, self.dict2, "interactive")

        assert resolved[0][1] == 11
        assert resolved[1][1] == 6
        assert dict.user_input.call_count == 1

    def test_resolve_conflicts_interactive_generated_persistent(self):
        dict.user_input = MagicMock(return_value=cc.PERSISTANT[1])

        resolved = dict.resolve_conflicts([["seven"],["four","five","six"]], self.dict1, self.dict2, "interactive")

        assert resolved[0][1] == 7
        assert resolved[1][1] == 8
        assert dict.user_input.call_count == 1

    def test_resolve_conflicts_None(self):

        resolved = dict.resolve_conflicts(None, None, None, 4)

        assert len(resolved) == 0


    def test_set_value_nested(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}, "seven": 11}

        dict.set_value(100, ["four","five", "six"], dict1)

        assert dict1["four"]["five"]["six"] == 100

    def test_set_value_top_level(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}, "seven": 11}

        dict.set_value("seven", ["seven"], dict1)

        assert dict1["seven"] == "seven"


    def test_set_value_none(self):

        dict.set_value(None, None, None)

        assert  True

    def test_get_value(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}, "seven": 11}

        assert dict.get_value(["seven"], dict1) == 11

        assert dict.get_value(["one","two","three"], dict1) == 3

        assert dict.get_value(["four","five","six"], dict1) == 6

    def test_set_resolved(self):
        dict1 = { "one": { "two": { "three" : 3}},
                  "four":{ "five": { "six" : 6 }}, "seven": 11}

        dict.set_resolved([(["one","two","three"], "three"),
                           (["seven"], "seven")], dict1)

        assert dict1["one"]["two"]["three"] == "three"
        assert dict1["seven"] == "seven"

    def test_resolve_conflict_single_user(self):
        dict.user_input = MagicMock(return_value=cc.SINGLE[0])

        assert dict.resolve_conflict(["four","five","six"], self.dict1, self.dict2, None)[1] == 6
        assert dict.user_input.call_count == 1

    def test_resolve_conflict_single_auto_generated(self):
        dict.user_input = MagicMock(return_value=cc.SINGLE[1])

        assert dict.resolve_conflict(["four","five","six"], self.dict1, self.dict2, None)[1] == 8
        assert dict.user_input.call_count == 1

    def test_resolve_conflict_persistent_user(self):
        dict.user_input = MagicMock(return_value=cc.PERSISTANT[0])

        assert dict.resolve_conflict(["four","five","six"], self.dict1, self.dict2, None)[1] == 6
        assert dict.user_input.call_count == 1

    def test_resolve_conflict_persistent_auto_generated(self):
        dict.user_input = MagicMock(return_value=cc.PERSISTANT[1])

        assert dict.resolve_conflict(["four","five","six"], self.dict1, self.dict2, None)[1] == 8
        assert dict.user_input.call_count == 1
