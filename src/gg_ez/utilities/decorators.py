def rename(newname):
    """Renames a function"""

    def decorator(f):
        f.__name__ = newname
        return f

    return decorator
