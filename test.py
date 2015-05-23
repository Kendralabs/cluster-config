from pprint import pprint

user = {"one": "one", "two": 2, "three": "3", "five": 5}
auto = {"one": "1", "two": {}, "three": "3", "four":"4"}

c = auto.copy()
pprint(c)

c.update(user)

pprint(c)

print len(c)
print len(auto["two"])

print type(c)
print type(auto["two"])
print type(user["one"])

if type(auto["two"]) == dict:
    print "hello dict"


test = []

test.append(2)

print test

one = [1]
print test + one

