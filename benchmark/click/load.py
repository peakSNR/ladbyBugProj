#!/usr/bin/env python3

import monad
import timeit
import psutil

db = monad.Database("mydb")
con = monad.Connection(db)

start = timeit.default_timer()
con.execute(open("create.cypher").read())
con.execute("COPY hits FROM 'hits.csv' (PARALLEL=false);")
end = timeit.default_timer()
print(end - start)
