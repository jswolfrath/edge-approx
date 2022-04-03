#!/usr/bin/env python

import numpy as np

import ast
import sys
import math
import time
import random
import warnings

from scipy import optimize

if len(sys.argv) < 2:
    print "Expected Input Data"
    sys.exit(1)

warnings.simplefilter("ignore")

random.seed(0x12345678)

MI = []
PRED = []
MEAN = []
VAR = []
COSTS = []
V_BOUND = []
OBS = []
EXP_COND_V = []
NUM_STREAMS = 0
DIM = 0

C_BOUND = sum(OBS) / 10

lineNumber = 0
for line in sys.argv[1].split("\n"):

    if lineNumber == 0:
        NUM_STREAMS = int(line)
        DIM = NUM_STREAMS * 2
    elif lineNumber == 1:
        C_BOUND = float(line)
    elif lineNumber == 2:
        OBS = ast.literal_eval(line)
    elif lineNumber == 3:
        PRED = ast.literal_eval(line)
    elif lineNumber == 4:
        MI = ast.literal_eval(line)
    elif lineNumber == 5:
        COSTS = ast.literal_eval(line)
    elif lineNumber == 6:
        MEAN = ast.literal_eval(line)
    elif lineNumber == 7:
        VAR = ast.literal_eval(line)
    elif lineNumber == 8:
        V_BOUND = np.array(ast.literal_eval(line))
    elif lineNumber == 9:
        EXP_COND_V = np.array(ast.literal_eval(line))

    lineNumber += 1

if np.sum(np.array(OBS)) <= C_BOUND:
    np.set_printoptions(suppress=True)
    print np.array2string(np.array(OBS + ([0] * len(OBS))), separator=', ')
    sys.exit(0)

if C_BOUND <= NUM_STREAMS:
    np.set_printoptions(suppress=True)
    print np.array2string(np.array(([1] * NUM_STREAMS) + ([0] * NUM_STREAMS)), separator=', ')
    sys.exit(0)

NP_VAR = np.array(VAR)
PENALTY = 0.0

MI = np.ones(NUM_STREAMS)
W = np.ones(NUM_STREAMS) / (np.ones(NUM_STREAMS) + np.abs(np.array(MEAN)))

ERR = np.maximum(np.array([0.001] * NUM_STREAMS), W * (NP_VAR + PENALTY))

def obj(x):
    val = 0.0
    for i in range(NUM_STREAMS): 
        val = val + (ERR[i] / (x[i] + (MI[i] * x[i+NUM_STREAMS])))
    return val

def jacobian(x):
    jac = np.zeros(DIM)
    for i in range(NUM_STREAMS):
        a = ERR[i]
        b = 1.0
        denom = (x[i] + b*x[i + NUM_STREAMS])**2
        jac[i] = -a / denom
        jac[i + NUM_STREAMS] = -(a*b) / denom
    return jac

H_EVAL_CNT = 0

def hessian(x):

    hess = np.zeros((DIM, DIM))

    for i in range(DIM):

        streamIdx = i % NUM_STREAMS

        a = ERR[i]
        b = 1

        for j in range(i, DIM):

            denom = (x[streamIdx] + b*x[streamIdx + NUM_STREAMS])**3

            if(i == j):

                if i < NUM_STREAMS:
                    hess[i][j] = (2 * a) / denom
                else:
                    hess[i][j] = (2 * a * b * b) / denom

            elif j == (i + NUM_STREAMS):

                value = (2 * a * b) / denom
                hess[i][j] = value
                hess[j][i] = value

    #hess = hess + (0.00001 * np.identity(DIM))
    return hess 

def prop(samplesAllowed):

    prop = np.zeros(NUM_STREAMS)
    adjAllowance = samplesAllowed - NUM_STREAMS
    total = 0.0

    for i in range(NUM_STREAMS):
        total += OBS[i]

    for i in range(NUM_STREAMS):
        prop[i] = OBS[i] / total

    return np.ones(NUM_STREAMS) + prop * adjAllowance

def ney2(samplesAllowed):

    prop = np.zeros(NUM_STREAMS)
    adjAllowance = samplesAllowed - NUM_STREAMS
    denom = 0.0

    for i in range(NUM_STREAMS):
        denom += OBS[i] * math.sqrt(VAR[i])

    for i in range(NUM_STREAMS):
        prop[i] = (OBS[i] * math.sqrt(VAR[i])) / denom

    return np.ones(NUM_STREAMS) + prop * adjAllowance


cons = []

#
# COST
#
A3 = np.hstack((np.ones(NUM_STREAMS),
                np.zeros(NUM_STREAMS)))

def c3(x):
    cost = 0.0
    for i in range(NUM_STREAMS):
        if x[i + NUM_STREAMS] > 0.5:
           cost += 1.0
    cost += np.matmul(x, np.hstack((COSTS, np.zeros(NUM_STREAMS))))
    return C_BOUND - cost
cons.append({"type": "eq", "fun": c3})

A4 = np.zeros((NUM_STREAMS, 2*NUM_STREAMS),dtype=float)
for i in range(A4.shape[0]):
    A4[i][PRED[i]] = 1.0
    A4[i][NUM_STREAMS + i] = -1.01

cons.append({"type": "ineq", "fun": lambda x: np.matmul(A4, x) })

#
# Lower bound on real + sim
#
A7 = np.hstack((np.identity(NUM_STREAMS, dtype = float),
                np.identity(NUM_STREAMS, dtype = float)))
b7 = np.ones(NUM_STREAMS) + 0.01
cons.append({"type": "ineq", "fun": lambda x: np.matmul(A7, x) - b7 })

#
# Variation Bound on simulated values
#

vs = VAR - EXP_COND_V
n_one = np.ones(NUM_STREAMS) * -1.0
offs = 0.5 * np.hstack((np.zeros(NUM_STREAMS),
                        np.ones(NUM_STREAMS)))
A5 = np.zeros((NUM_STREAMS, 2*NUM_STREAMS))
for i in range(NUM_STREAMS):
    A5[i][i] = -V_BOUND[i]
    A5[i][NUM_STREAMS + i] = (VAR[i] - vs[i] - V_BOUND[i])
cons.append({"type": "ineq", "fun": lambda x: n_one * np.matmul(A5, x + offs)})

#
# Starting location
#
start = np.hstack((prop(C_BOUND),
                   np.zeros(NUM_STREAMS)))

unique, counts = np.unique(PRED, return_counts=True)
list1, list2 = zip(*sorted(zip(counts, unique)))
top2 = list2[len(list2)-2:]

for i in range(NUM_STREAMS):
    sidx = i + NUM_STREAMS
    if PRED[i] in top2:
        start[sidx] = start[PRED[i]]
    else:
        start[sidx] = 0

#
# Bounds on the dimensions
#
bnds = []
for i in range(NUM_STREAMS):
    bnds.append((0, OBS[i]))
for i in range(NUM_STREAMS):
    bnds.append((0, OBS[PRED[i]]))

bnds = tuple(bnds)

st = time.time()
result = optimize.minimize(obj, start,
                           method="SLSQP",
                           jac = jacobian,
                           hess = hessian,
                           options={'xtol': 1e-1, 'gtol': 1e-02, 'maxiter':200 },
                           bounds=bnds,
                           constraints=cons)

et = time.time()

if not result.success and result.status is not 9 and result.status is not 0:
    pass
    #print "FAILED"
    #print result.status
    #print result.message

np.set_printoptions(suppress=True)
print np.array2string(result.x.clip(0.0), separator=', ')
