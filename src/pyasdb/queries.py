"""
Module containing functions for common queries to reduce repetitive lambda writing
"""


def eq(value, compare):
    return value == compare


def neq(value, compare):
    return value != compare


def gt(value, compare):
    return value > compare


def lt(value, compare):
    return value < compare


def gte(value, compare):
    return value >= compare


def lte(value, compare):
    return value <= compare


def contains(value, compare):
    return compare in value


def not_contains(value, compare):
    return compare not in value


def begins_with(value, compare):
    return value.startswith(compare)


def not_begins_with(value, compare):
    return not value.startswith(compare)


def between(value, compare):
    return compare[0] <= value <= compare[1]


def not_between(value, compare):
    return not between(value, compare)


def in_(value, compare):
    return value in compare


def not_in(value, compare):
    return value not in compare
