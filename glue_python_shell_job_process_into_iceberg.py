import sys
import awswrangler as wr

MERGE_QUERY = """
MERGE INTO employee_s2 t 
using (
select a.id, a.name, a.designation, a.salary, a.location
from (
select rank() over (partition by id order by record_ts desc) as rnk, * from employee_s1
) a where a.rnk = 1
)
s
on (t.id = s.id)
when matched and s.salary > 10000 
then delete
when matched
then update set name = s.name, designation = s.designation, salary = s.salary, location = s.location, record_ts = current_timestamp
when not matched 
then insert values (s.id, s.name, s.designation, s.salary, current_timestamp, s.location);
"""

wr.athena.start_query_execution(
    sql=MERGE_QUERY,
    database='iceberg',
    wait=True
)

