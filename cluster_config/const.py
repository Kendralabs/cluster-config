
class Const(object):

    def add(self, name, *obj):
        setattr(self, name, obj)
        print("test")
