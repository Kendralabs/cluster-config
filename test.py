from pprint import pprint

role = { "type": "test"}
print role["type"]
pprint(role)
role["bleh"] = "sdsdf"
#setattr(role, "type1", "")
role["type"] = ""

print role.type

somefunction="(memory/cores)"

somefunction = somefunction.replace("memory", "6000000")
somefunction = somefunction.replace("cores", "4")
pprint(somefunction)

print eval(somefunction)


#copy user into auto config