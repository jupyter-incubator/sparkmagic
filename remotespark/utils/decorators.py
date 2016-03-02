from remotespark.utils import utils


def add_guid(attr_name):
    def class_decorator(cls):
        def get_attr(self, attr=attr_name):
            return getattr(self, "_" + attr)

        def set_attr(self, value, attr=attr_name):
            setattr(self, "_" + attr, value)

        prop = property(get_attr, set_attr)
        setattr(cls, attr_name, prop)
        setattr(cls, "_" + attr_name, utils.generate_uuid())  # Default value for that attribute
        return cls

    return class_decorator
