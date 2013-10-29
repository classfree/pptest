#!/usr/bin/env python
# File: sum_primes.py
# Author: Vitalii Vanovschi
# Desc: This program demonstrates parallel computations with pp module
# It calculates the sum of prime numbers below a given integer in parallel
# Parallel Python Software: http://www.parallelpython.com

import math
import sys
import pp


def isprime(n):
    """Returns True if n is prime and False otherwise"""
    if not isinstance(n, int):
        raise TypeError("argument passed to is_prime is not of 'int' type")
    if n < 2:
        return False
    if n == 2:
        return True
    max = int(math.ceil(math.sqrt(n)))
    i = 2
    while i <= max:
        if n % i == 0:
            return False
        i += 1
    return True


def sum_primes(n):
    """Calculates sum of all primes below given integer n"""
    return sum([x for x in xrange(2, n) if isprime(x)])


print """Usage: python sum_primes.py [ncpus]
    [ncpus] - the number of workers to run in parallel,
    if omitted it will be set to the number of processors in the system"""

# tuple of all parallel python servers to connect with
#ppservers = ("127.0.0.1:60000", )
ppservers1 = ("192.168.56.102:1234", )
ppservers2 = ("192.168.0.11:8809", )

job_server1 = pp.Server(ncpus=0, ppservers=ppservers1, secret="blog")
job_server2 = pp.Server(ncpus=0, ppservers=ppservers2)

print sys.argv[0]
print len(sys.argv)

"""
if len(sys.argv) > 1:
    ncpus = int(sys.argv[1])
    # Creates jobserver with ncpus workers
    job_server = pp.Server(ncpus, ppservers=ppservers)
else:
    # Creates jobserver with automatically detected number of workers
    job_server = pp.Server(ppservers=ppservers)

print "Starting pp with", job_server.get_ncpus(), "workers"
"""

# Submit a job of calulating sum_primes(100) for execution.
# sum_primes - the function
# (100,) - tuple with arguments for sum_primes
# (isprime,) - tuple with functions on which function sum_primes depends
# ("math",) - tuple with module names which must be imported before
#             sum_primes execution
# Execution starts as soon as one of the workers will become available
job1 = job_server1.submit(sum_primes, (100, ), (isprime, ), ("math", ))
job2 = job_server2.submit(sum_primes, (200, ), (isprime, ), ("math", ))

# Retrieves the result calculated by job1
# The value of job1() is the same as sum_primes(100)
# If the job has not been finished yet, execution will
# wait here until result is available
result1 = job1()
print "Sum of primes below 100 is", result1, '\n'
result2 = job2()
print "Sum of primes below 200 is", result2, '\n'


# The following submits 8 jobs and then retrieves the results
#inputs = (100000, 100100, 100200, 100300, 100400, 100500, 100600, 100700)
#jobs = [(input, job_server1.submit(sum_primes, (input, ), (isprime, ), ("math", ))) for input in inputs]
"""
jobs=[]
for input in inputs:
    if input<100300:
        jobs.append((input, job_server1.submit(sum_primes, (input, ), (isprime, ), ("math", ))))
    else:
        jobs.append((input, job_server2.submit(sum_primes, (input, ), (isprime, ), ("math", ))))

for input, job in jobs:
    print "Sum of primes below", input, "is", job()
"""
"""
for input in inputs:
    if input<100300:
        job = job_server1.submit(sum_primes, (input, ), (isprime, ), ("math", ))
        result = job()
        print "Sum of primes below ", input, " is", result, '\n'
    else:        
        job = job_server2.submit(sum_primes, (input, ), (isprime, ), ("math", ))
        result = job()
        print "Sum of primes below ", input, " is", result, '\n'
"""


while True:
    num = input("insert num ? ")
    if num == 0:
        break
    elif num < 100000:
        job = job_server1.submit(sum_primes, (num, ), (isprime, ), ("math", ))
        result = job()
        print "Sum of primes below ", num, " is", result, '\n'
        job_server1.print_stats()
    else:        
        job = job_server2.submit(sum_primes, (num, ), (isprime, ), ("math", ))
        result = job()
        print "Sum of primes below ", num, " is", result, '\n'
        job_server2.print_stats()

print "job_server1"
job_server1.print_stats()
print "job_server2"
job_server2.print_stats()

# Parallel Python Software: http://www.parallelpython.com
