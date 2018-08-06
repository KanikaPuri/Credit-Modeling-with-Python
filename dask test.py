# -*- coding: utf-8 -*-
"""
Created on Sun Aug  5 17:52:21 2018

@author: kanika
"""

from dask import delayed
from graphviz import Digraph

@delayed
def increment(x):
    return x+1
@delayed
def halve(x):
    return x/2
@delayed
def default(hist, income):
    return hist**2 + income
@delayed
def agg(x, y):
    return x + y


def merge(seq):
    if len(seq) < 2:
        return seq
    middle = len(seq)//2
    left = merge(seq[:middle])
    right = merge(seq[middle:])
    if not right:
        return left
    return [agg(left[0],right[0])]


hist_yrs = range(10)
incomes = range(10)
inc_hist = [increment(n) for n in hist_yrs]
halved_income = [halve(n) for n in incomes]
estimated_defaults = [default(hist, income) for hist, income in zip(inc_hist, halved_income)]
default_sum = merge(estimated_defaults)
avg_default = default_sum[0] / 10
avg_default.compute()
avg_default.visualize()  # requires graphviz and python-graphviz to be installed